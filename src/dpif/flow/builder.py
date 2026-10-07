"""Builder constructing the common PipelineFlowGraph from existing evidence.

One builder serves BOTH validation paths (no offline/online split):
- Offline: contract (Source/Target) + local code analysis.
- Online:  synthesized contract + Databricks-retrieved code analysis.

Evidence sources consumed (all existing, nothing re-parsed):
- ``CodeAnalysis.operations`` / ``.assignments`` (AST parser)
- ``OperationType`` vocabulary and ``SHUFFLE_OPS`` (code.pyspark)
- ``EvidenceProvenanceKind`` (models.implementation) for provenance
- contract ``Source`` / ``Target`` declarations
- pipeline volume baseline via ``extract_baseline_volume`` (scalability.engine;
  same precedence offline and online: runtime → profile → contract)
- raw code text ONLY for a minimal ``dlt.read`` / ``create_streaming_table``
  vocabulary the AST parser does not cover (DLT; see docs/PIPELINE_FLOW_GRAPH.md)

UNKNOWN discipline: dataset identity, formats, source locations and volume
observations stay UNKNOWN (``None``) when evidence does not determine them.
Nothing is fabricated. Per-operation byte/row deltas are never estimated:
only SOURCE/TARGET pipeline baselines are attached.
"""

from __future__ import annotations

import re
from collections import defaultdict

from dpif.code.flow import ACTION_TYPES
from dpif.code.models import CodeAnalysis, Operation, OperationType
from dpif.code.pyspark import SHUFFLE_OPS
from dpif.flow.correlation import correlate_sql_operations
from dpif.flow.models import (
    DatasetIdentity,
    EvidenceState,
    FlowEdge,
    FlowNode,
    FlowNodeKind,
    FlowProvenance,
    PipelineFlowGraph,
    SourceLocation,
    VolumeObservation,
)
from dpif.models import CollectionMethod, DataProfile, PipelineContract
from dpif.models.implementation import EvidenceProvenanceKind
from dpif.runtime.models import QueryHistoryEntry
from dpif.scalability.engine import extract_baseline_volume
from dpif.scalability.models import EvidenceProvenance as ScalabilityProvenance

_FORMAT_VIAS = frozenset({"parquet", "csv", "json", "orc", "avro", "delta"})

_SAVEAS_TABLE_RE = re.compile(r"""\.(?:saveAsTable|insertInto)\(\s*['"]([^'"]+)['"]""")
_SAVE_PATH_RE = re.compile(r"""\.(?:save|parquet|csv|json|orc|avro)\(\s*['"]([^'"]+)['"]""")
_TOTABLE_RE = re.compile(r"""\.toTable\(\s*['"]([^'"]+)['"]""")
_START_PATH_RE = re.compile(r"""\.start\(\s*['"]([^'"]+)['"]""")
_SQL_FROM_RE = re.compile(r"(?i)\bfrom\s+([a-zA-Z0-9_.]+)")
_BARE_VAR_RE = re.compile(r"^([A-Za-z_]\w*)$")
_BROADCAST_VAR_RE = re.compile(r"^(?:[A-Za-z_]\w*\.)?broadcast\(\s*([A-Za-z_]\w*)\s*\)$")
_ATTR_CHAIN_VAR_RE = re.compile(r"^([A-Za-z_]\w*)\.\w+")
_READ_TERMINAL_RE = re.compile(r"""\.(?:load|parquet|csv|json|orc|avro)\(\s*['"]([^'"]+)['"]""")
_READ_FORMAT_RE = re.compile(r"""\.format\(\s*['"]([^'"]+)['"]""")
_DLT_READ_RE = re.compile(r"""dlt\.read_stream?\(?\(\s*['"]([^'"]+)['"]""")
_DLT_READ_STRICT_RE = re.compile(r"""dlt\.read(?:_stream)?\(\s*['"]([^'"]+)['"]""")
_DLT_STREAMING_TABLE_RE = re.compile(
    r"""dlt\.create_streaming_table\(\s*['"]([^'"]+)['"]"""
)
_DLT_TABLE_DECORATOR_RE = re.compile(r"""@dlt\.table\(\s*name\s*=\s*['"]([^'"]+)['"]""")
_DLT_TABLE_DECORATOR_FN_RE = re.compile(r"@dlt\.table\(\)\s*\ndef\s+([A-Za-z_]\w*)")

# Parser snippets are capped (~200 chars); long chained statements can lose
# their terminal call. The builder falls back to a small raw-code window.
_SNIPPET_LOOKAHEAD_LINES = 15


def _code_window(raw_code: str, line: int, lookahead: int = _SNIPPET_LOOKAHEAD_LINES) -> str:
    """Source window starting at ``line`` (1-indexed) for truncated-snippet recovery."""
    if not raw_code or not line:
        return ""
    lines = raw_code.splitlines()
    lo = max(line - 1, 0)
    return "\n".join(lines[lo : min(lo + lookahead, len(lines))])


def _static(state: EvidenceState = EvidenceState.KNOWN) -> FlowProvenance:
    return FlowProvenance(kind=EvidenceProvenanceKind.STATIC_CODE, state=state)


def _contract_prov(state: EvidenceState = EvidenceState.DERIVED) -> FlowProvenance:
    return FlowProvenance(kind=EvidenceProvenanceKind.CONTRACT, state=state)


_GB_BYTES = 1024.0**3

# Baseline provenance (scalability.engine) -> flow provenance (Phase 2).
# RUNTIME/CONTRACT are directly evidenced (KNOWN); FIXTURE/METADATA are
# representative or synthesized (DERIVED). UNKNOWN never yields a volume.
_BASELINE_PROVENANCE: dict[ScalabilityProvenance, FlowProvenance] = {
    ScalabilityProvenance.RUNTIME: FlowProvenance(
        kind=EvidenceProvenanceKind.RUNTIME, state=EvidenceState.KNOWN
    ),
    ScalabilityProvenance.CONTRACT: FlowProvenance(
        kind=EvidenceProvenanceKind.CONTRACT, state=EvidenceState.KNOWN
    ),
    ScalabilityProvenance.FIXTURE: FlowProvenance(
        kind=EvidenceProvenanceKind.FIXTURE, state=EvidenceState.DERIVED
    ),
    ScalabilityProvenance.DATABRICKS_METADATA: FlowProvenance(
        kind=EvidenceProvenanceKind.METADATA, state=EvidenceState.DERIVED
    ),
}


def _baseline_source_volume(
    baseline_gb: float | None,
    baseline_prov: ScalabilityProvenance,
    data_profile: DataProfile | None,
) -> VolumeObservation | None:
    """SOURCE pipeline baseline from ``extract_baseline_volume`` output.

    Returns ``None`` (UNKNOWN) when the baseline is missing or non-positive.
    Row/file counts attach only when positively evidenced (``> 0``); a
    ``DataProfile`` zero is indistinguishable from missing and stays UNKNOWN.
    File counts attach only for FIXTURE profiles, which carry real size
    distributions (online synthesized placeholders are zero post-hardening).
    """
    if baseline_gb is None or baseline_gb <= 0:
        return None
    provenance = _BASELINE_PROVENANCE.get(baseline_prov)
    if provenance is None:
        return None
    rows: int | None = None
    files: int | None = None
    if data_profile is not None:
        record_count = data_profile.record_count or 0
        if record_count > 0:
            rows = int(record_count)
        if (
            data_profile.collection_method == CollectionMethod.FIXTURE
            and (data_profile.file_count or 0) > 0
        ):
            files = int(data_profile.file_count)
    return VolumeObservation(
        input_bytes=int(baseline_gb * _GB_BYTES),
        input_rows=rows,
        file_count=files,
        provenance=provenance,
    )


def _strip_quotes(text: str) -> str:
    return text.strip().strip("'\"").strip()


def _read_identity(op: Operation, raw_code: str = "") -> DatasetIdentity:
    """Dataset identity for a READ op from its recorded arguments (no fabrication)."""
    via = str(op.arguments.get("via", ""))
    query = _strip_quotes(str(op.arguments.get("query", "")))
    if via in ("table",) and query:
        return DatasetIdentity(name=query, state=EvidenceState.KNOWN, kind="table")
    if via == "sql" and query:
        m = _SQL_FROM_RE.search(query)
        if m:
            return DatasetIdentity(name=m.group(1), state=EvidenceState.KNOWN, kind="table")
        return DatasetIdentity(name="UNKNOWN", state=EvidenceState.UNKNOWN, kind="sql")
    if via in _FORMAT_VIAS:
        # e.g. spark.read.parquet(<path>) — parser records via but not the path.
        m = re.search(r"""\(\s*['"]([^'"]+)['"]""", op.code)
        if m:
            return DatasetIdentity(
                name=m.group(1), state=EvidenceState.KNOWN, kind="path", format=via
            )
        return DatasetIdentity(
            name="UNKNOWN", state=EvidenceState.UNKNOWN, kind="path", format=via
        )
    if via == "read":
        # Terminal read call (.load/.parquet/...) — NOT .format(...), whose
        # argument is a format name, not a location.
        m = _READ_TERMINAL_RE.search(op.code)
        if m:
            fmt_m = _READ_FORMAT_RE.search(op.code)
            return DatasetIdentity(
                name=m.group(1),
                state=EvidenceState.KNOWN,
                kind="path",
                format=fmt_m.group(1) if fmt_m else None,
            )
        # Snippet truncated before the terminal call: recover from raw code.
        window = _code_window(raw_code, op.line)
        if window:
            m = _READ_TERMINAL_RE.search(window)
            if m:
                fmt_m = _READ_FORMAT_RE.search(window)
                return DatasetIdentity(
                    name=m.group(1),
                    state=EvidenceState.KNOWN,
                    kind="path",
                    format=fmt_m.group(1) if fmt_m else None,
                )
    return DatasetIdentity(name="UNKNOWN", state=EvidenceState.UNKNOWN)


def _write_identity(op: Operation, raw_code: str = "") -> DatasetIdentity:
    """Dataset identity for a WRITE op from its recorded snippet (no fabrication).

    Falls back to the raw-code window around the statement when the parser
    snippet was truncated before the terminal ``.saveAsTable`` / ``.save`` /
    ``.toTable`` / ``.start`` call.
    """
    via = str(op.arguments.get("via", "")).lower()
    m = _SAVEAS_TABLE_RE.search(op.code) or _TOTABLE_RE.search(op.code)
    if m:
        return DatasetIdentity(name=m.group(1), state=EvidenceState.KNOWN, kind="table", format="delta")
    m = _SAVE_PATH_RE.search(op.code) or _START_PATH_RE.search(op.code)
    if m:
        fmt = via if via in _FORMAT_VIAS else None
        return DatasetIdentity(name=m.group(1), state=EvidenceState.KNOWN, kind="path", format=fmt)
    window = _code_window(raw_code, op.line)
    if window:
        m = _SAVEAS_TABLE_RE.search(window) or _TOTABLE_RE.search(window)
        if m:
            return DatasetIdentity(name=m.group(1), state=EvidenceState.KNOWN, kind="table", format="delta")
        m = _SAVE_PATH_RE.search(window) or _START_PATH_RE.search(window)
        if m:
            fmt = via if via in _FORMAT_VIAS else None
            return DatasetIdentity(name=m.group(1), state=EvidenceState.KNOWN, kind="path", format=fmt)
    return DatasetIdentity(name="UNKNOWN", state=EvidenceState.UNKNOWN, kind="sink")


def _join_other_variable(op: Operation) -> str | None:
    """Resolve a join's second input to a DataFrame variable when possible.

    Handles ``df2``, ``F.broadcast(df2)`` / ``broadcast(df2)`` and attribute
    chains rooted at a variable (``df2.alias('r')``). The caller only uses the
    result when the variable has known producers, so no fabrication occurs.
    """
    other = str(op.arguments.get("other", "")).strip()
    if not other:
        return None
    m = _BARE_VAR_RE.match(other)
    if m:
        return m.group(1)
    m = _BROADCAST_VAR_RE.match(other)
    if m:
        return m.group(1)
    m = _ATTR_CHAIN_VAR_RE.match(other)
    if m:
        return m.group(1)
    return None


class _GraphBuilder:
    """Single-pass constructor over CodeAnalysis (plus optional contract)."""

    def __init__(
        self,
        code_analysis: CodeAnalysis | None,
        contract: PipelineContract | None,
        pipeline_name: str,
        raw_code: str,
        data_profile: DataProfile | None = None,
        runtime_run: object | None = None,
        query_history: list[QueryHistoryEntry] | list[dict[str, object]] | None = None,
        history_provenance: FlowProvenance | None = None,
        volume_mode: str = "pipeline",
    ) -> None:
        self.analysis = code_analysis
        self.contract = contract
        self.pipeline_name = pipeline_name or (
            getattr(contract, "pipeline_name", "") or "unknown" if contract else "unknown"
        )
        self.raw_code = raw_code or ""
        self.data_profile = data_profile
        self.runtime_run = runtime_run
        self.query_history = query_history
        self.history_provenance = history_provenance
        # Phase 9 (P9-4): "pipeline" keeps legacy SOURCE fan-out;
        # "attributed" skips it (UNKNOWN unless exact attribution attaches).
        self.volume_mode = volume_mode
        self.nodes: list[FlowNode] = []
        self.edges: list[FlowEdge] = []
        self.source_ids: list[str] = []
        self.target_ids: list[str] = []
        # var -> current producing node id (dataset node, op node or shuffle node)
        self.last_producer: dict[str, str] = {}
        # var -> dataset node id (stable identity of the variable)
        self.ds_nodes: dict[str, str] = {}

    # -- node factories ----------------------------------------------------
    def add_node(self, node: FlowNode, *, is_source: bool = False, is_target: bool = False) -> None:
        self.nodes.append(node)
        if is_source:
            self.source_ids.append(node.node_id)
        if is_target:
            self.target_ids.append(node.node_id)

    def dataset_node(self, var: str, line: int, task_key: str | None = None) -> FlowNode:
        """Get-or-create the DATASET node for a DataFrame variable.

        ``task_key`` is copied only when the caller supplies the authoritative
        owning operation's key; otherwise the node stays unattributed
        (UNKNOWN). Node identity is never derived from it.
        """
        existing = self.ds_nodes.get(var)
        if existing is not None:
            node = self.node_by_id(existing)
            if node is not None:
                return node
        node = FlowNode(
            node_id=f"ds:{var}:{line}",
            kind=FlowNodeKind.DATASET,
            name=var,
            dataset=DatasetIdentity(name=var, state=EvidenceState.DERIVED, kind="variable"),
            location=SourceLocation(
                file=self.analysis.source_file if self.analysis else None, line=line
            ),
            task_key=task_key,
            provenance=_static(EvidenceState.DERIVED),
        )
        self.add_node(node)
        self.ds_nodes[var] = node.node_id
        return node

    def node_by_id(self, node_id: str) -> FlowNode | None:
        for n in self.nodes:
            if n.node_id == node_id:
                return n
        return None

    def location(self, op: Operation) -> SourceLocation:
        return SourceLocation(
            file=(self.analysis.source_file if self.analysis else None) or None,
            line=op.line,
            column=op.column,
        )

    # -- main construction ---------------------------------------------------
    def build(self) -> PipelineFlowGraph:
        if self.analysis is not None and self.analysis.language == "sql":
            self._build_sql()
        elif self.analysis is not None:
            self._build_python()
            if not self.source_ids and not self.target_ids:
                # No Python dataflow detected (e.g. pure SQL-in-string content):
                # fall back to the SQL evidence so the graph still reflects reads.
                self._build_sql()
        else:
            self._build_dlt_sources()
        self._apply_contract_evidence()
        self._apply_volume_baselines(attribute_mode=(self.volume_mode == "attributed"))
        correlations = correlate_sql_operations(
            self.analysis,
            self.query_history,  # type: ignore[arg-type]
            history_provenance=self.history_provenance,
        )
        return PipelineFlowGraph(
            pipeline_name=self.pipeline_name,
            nodes=self.nodes,
            edges=self.edges,
            source_ids=self.source_ids,
            target_ids=self.target_ids,
            correlations=correlations,
        )

    # -- Python/PySpark path --------------------------------------------------
    def _build_python(self) -> None:
        assert self.analysis is not None
        analysis = self.analysis
        ops = sorted(analysis.operations, key=lambda o: (o.line, o.column))
        ops_by_line: dict[int, list[Operation]] = defaultdict(list)
        for op in ops:
            ops_by_line[op.line].append(op)
        assignments_by_line: dict[int, list[object]] = defaultdict(list)
        for a in analysis.assignments:
            assignments_by_line[a.line].append(a)

        for line in sorted(ops_by_line):
            # ``df.filter(...)`` ops run with the producer chain as it stood
            # before this line's assignment re-bound the target variable; the
            # snapshot keeps multi-branch fan-out (a = df.f(); b = df.g())
            # rooted at the same ancestor instead of chaining branch-to-branch.
            snapshot = dict(self.last_producer)
            for op in ops_by_line[line]:
                self._build_op(op)
            for assignment in assignments_by_line.get(line, []):
                outcome = self._apply_assignment(assignment, ops_by_line[line])  # type: ignore[arg-type]
                source = getattr(assignment, "source", "")
                target = getattr(assignment, "target", "")
                if outcome in ("chain", "action") and target != source:
                    # The chain rooted at ``source`` was consumed by binding
                    # ``target``: restore ``source``'s pre-line producer so a
                    # later branch reading ``source`` fans out correctly.
                    if source in snapshot:
                        self.last_producer[source] = snapshot[source]
                    else:
                        self.last_producer.pop(source, None)
                # outcome == "read": read wiring already bound ``target`` and
                # must stand; outcome == "none": nothing was consumed.
                # target == source (self-rebind): the new binding stands.
        self._build_dlt_sources()

    def _build_op(self, op: Operation) -> None:
        op_type = op.operation_type
        var = op.dataframe
        loc = self.location(op)

        # Reads become SOURCE nodes (the read is the pipeline entry boundary).
        # Phase 9 (P9-2): task_key is copied from the owning operation only;
        # node identity, edges, and volumes are untouched.
        if op_type == OperationType.READ:
            node = FlowNode(
                node_id=f"src:{op.line}:{op.column}",
                kind=FlowNodeKind.SOURCE,
                operation_type=OperationType.READ,
                dataset=_read_identity(op, self.raw_code),
                location=loc,
                task_key=op.task_key,
                metadata={"via": str(op.arguments.get("via", ""))},
                provenance=_static(),
            )
            self.add_node(node, is_source=True)
            self._link_read_output(op, node.node_id)
            return

        input_node = self.last_producer.get(var) if var else None

        # Writes become OPERATION -> TARGET.
        if op_type == OperationType.WRITE:
            op_node = FlowNode(
                node_id=f"op:{op_type.value.lower()}:{op.line}:{op.column}",
                kind=FlowNodeKind.OPERATION,
                operation_type=op_type,
                name=var,
                dataset=DatasetIdentity(name=var or "UNKNOWN", state=EvidenceState.DERIVED, kind="variable")
                if var
                else None,
                location=loc,
                task_key=op.task_key,
                provenance=_static(),
            )
            self.add_node(op_node)
            if input_node:
                self.edges.append(
                    FlowEdge(from_node=input_node, to_node=op_node.node_id, dataset=var, provenance=_static(EvidenceState.DERIVED))
                )
            target = FlowNode(
                node_id=f"tgt:{op.line}:{op.column}",
                kind=FlowNodeKind.TARGET,
                dataset=_write_identity(op, self.raw_code),
                location=loc,
                task_key=op.task_key,
                provenance=_static(),
            )
            self.add_node(target, is_target=True)
            self.edges.append(
                FlowEdge(from_node=op_node.node_id, to_node=target.node_id, dataset=var, provenance=_static())
            )
            return

        # Regular operation node.
        op_node = FlowNode(
            node_id=f"op:{op_type.value.lower()}:{op.line}:{op.column}",
            kind=FlowNodeKind.OPERATION,
            operation_type=op_type,
            name=var,
            dataset=DatasetIdentity(name=var or "UNKNOWN", state=EvidenceState.DERIVED, kind="variable")
            if var
            else None,
            location=loc,
            task_key=op.task_key,
            metadata={"code": op.code} if op.code else {},
            provenance=_static(),
        )
        self.add_node(op_node)

        if input_node:
            self.edges.append(
                FlowEdge(from_node=input_node, to_node=op_node.node_id, dataset=var, provenance=_static(EvidenceState.DERIVED))
            )

        # Multi-input: joins consume the broadcast/other side too.
        if op_type == OperationType.JOIN and var:
            other = _join_other_variable(op)
            if other and other in self.last_producer:
                self.edges.append(
                    FlowEdge(
                        from_node=self.last_producer[other],
                        to_node=op_node.node_id,
                        dataset=other,
                        provenance=_static(EvidenceState.DERIVED),
                    )
                )

        # Update the producer chain: shuffle-causing ops route through a
        # SHUFFLE node so downstream operations chain OPERATION -> SHUFFLE -> OPERATION.
        if var:
            if op_type in SHUFFLE_OPS:
                shuffle = FlowNode(
                    node_id=f"shf:{op_type.value.lower()}:{op.line}:{op.column}",
                    kind=FlowNodeKind.SHUFFLE,
                    shuffle_cause=op_type,
                    location=loc,
                    task_key=op.task_key,
                    provenance=_static(EvidenceState.DERIVED),
                )
                self.add_node(shuffle)
                self.edges.append(
                    FlowEdge(from_node=op_node.node_id, to_node=shuffle.node_id, dataset=var, provenance=_static(EvidenceState.DERIVED))
                )
                self.last_producer[var] = shuffle.node_id
            else:
                self.last_producer[var] = op_node.node_id

    def _link_read_output(self, op: Operation, source_node_id: str) -> None:
        """Connect a SOURCE to its assigned DataFrame variable (x = spark.read...)."""
        assert self.analysis is not None
        for a in self.analysis.assignments:
            if a.line == op.line and a.source == "spark" and a.target:
                ds = self.dataset_node(a.target, op.line, task_key=op.task_key)
                self.edges.append(
                    FlowEdge(from_node=source_node_id, to_node=ds.node_id, dataset=a.target, provenance=_static())
                )
                self.last_producer[a.target] = ds.node_id

    def _apply_assignment(self, assignment: object, line_ops: list[Operation]) -> str:
        """Wire ``target = <expr rooted at source>`` after ops of that line ran.

        Returns what was wired so the caller can maintain the producer chain:
        - "chain":  target bound to a new dataset node consuming ``source``.
        - "action": terminal/action consumed ``source``; no dataset created.
        - "read":   READ output wiring (handled by ``_link_read_output``).
        - "none":   nothing to wire.
        """
        target = getattr(assignment, "target", "")
        source = getattr(assignment, "source", "")
        if not target:
            return "none"
        producing: Operation | None = None
        for op in line_ops:
            if op.dataframe == source:
                producing = op  # keep last match (terminal chain op)
        if producing is None and source == "spark":
            for op in line_ops:
                if op.operation_type == OperationType.READ:
                    producing = op
        if producing is None:
            return "none"
        # Terminal/action outputs do not create new DataFrame datasets.
        if producing.operation_type in ACTION_TYPES or producing.operation_type == OperationType.WRITE:
            return "action"

        producer_node_id = self.last_producer.get(producing.dataframe or "")
        if producing.operation_type == OperationType.READ:
            # Read outputs were wired by _link_read_output.
            return "read"
        if producer_node_id is None:
            producer_node_id = (
                f"op:{producing.operation_type.value.lower()}:{producing.line}:{producing.column}"
            )
        ds = self.dataset_node(
            target, getattr(assignment, "line", producing.line), task_key=producing.task_key
        )
        self.edges.append(
            FlowEdge(from_node=producer_node_id, to_node=ds.node_id, dataset=target, provenance=_static(EvidenceState.DERIVED))
        )
        self.last_producer[target] = ds.node_id
        return "chain"

    # -- SQL path --------------------------------------------------------------
    def _build_sql(self) -> None:
        assert self.analysis is not None
        sql = self.analysis.sql_analysis
        if sql is None:
            return
        table_nodes: dict[str, str] = {}
        join_seq = 0  # join node ids must stay unique even when line/column are 0

        def qualified(ref: object) -> str:
            """Fully-qualified name (catalog.schema.name) from available parts."""
            name = str(getattr(ref, "name", "") or "").strip()
            schema = getattr(ref, "schema_name", None)
            catalog = getattr(ref, "catalog", None)
            parts = [p for p in (catalog, schema, name) if p]
            return ".".join(parts)

        def table_source(ref: object) -> str | None:
            name = qualified(ref)
            if not name:
                # Parser could not resolve this reference (e.g. empty join side).
                return None
            if name in table_nodes:
                return table_nodes[name]
            line = getattr(ref, "line", 0) or None
            node = FlowNode(
                node_id=f"src:sql:{name}",
                kind=FlowNodeKind.SOURCE,
                operation_type=OperationType.READ,
                dataset=DatasetIdentity(name=name, state=EvidenceState.KNOWN, kind="table"),
                location=SourceLocation(
                    file=self.analysis.source_file if self.analysis else None, line=line
                ),
                provenance=_static(),
            )
            self.add_node(node, is_source=True)
            table_nodes[name] = node.node_id
            return node.node_id

        declared: list[str] = []
        for t in sql.tables:
            if getattr(t, "is_subquery", False):
                continue
            ref_name = qualified(t)
            if ref_name:
                table_source(t)
                declared.append(ref_name)

        # Sides already resolved directly by the parser (used during recovery).
        assigned: set[str] = set()
        for join in sql.joins:
            for side_ref in (join.left_table, join.right_table):
                nm = qualified(side_ref)
                if nm:
                    assigned.add(nm)

        for join in sql.joins:
            left = table_source(join.left_table)
            right = table_source(join.right_table)
            if left is None and right is None:
                continue
            join_seq += 1

            # Deterministic recovery: the parser may leave a join side empty
            # (e.g. aliased ON clauses). When the query evidence declares more
            # tables than the parser resolved, fill unresolved sides from the
            # declared-but-unassigned tables (DERIVED, never fabricated).
            sides: list[str | None] = [left, right]
            for idx, side in enumerate(sides):
                if side is not None:
                    continue
                for cand in declared:
                    if cand not in assigned:
                        sides[idx] = table_nodes[cand]
                        assigned.add(cand)
                        break
            left, right = sides
            op_node = FlowNode(
                node_id=f"op:join:{join_seq}",
                kind=FlowNodeKind.OPERATION,
                operation_type=OperationType.JOIN,
                location=SourceLocation(file=self.analysis.source_file, line=join.line, column=join.column),
                metadata={"join_type": join.join_type.value, "join_columns": list(join.join_columns)},
                provenance=_static(),
            )
            self.add_node(op_node)
            shuffle = FlowNode(
                node_id=f"shf:join:{join_seq}",
                kind=FlowNodeKind.SHUFFLE,
                shuffle_cause=OperationType.JOIN,
                location=SourceLocation(file=self.analysis.source_file, line=join.line),
                provenance=_static(EvidenceState.DERIVED),
            )
            self.add_node(shuffle)
            if left:
                self.edges.append(FlowEdge(from_node=left, to_node=op_node.node_id, provenance=_static()))
            if right:
                self.edges.append(FlowEdge(from_node=right, to_node=op_node.node_id, provenance=_static()))
            self.edges.append(FlowEdge(from_node=op_node.node_id, to_node=shuffle.node_id, provenance=_static(EvidenceState.DERIVED)))

    # -- DLT vocabulary (raw-code fallback) -------------------------------------
    def _build_dlt_sources(self) -> None:
        if not self.raw_code:
            return
        existing_names = {
            n.dataset.name
            for n in self.nodes
            if n.dataset and n.dataset.name != "UNKNOWN"
        }
        for pattern in (_DLT_READ_STRICT_RE,):
            for m in pattern.finditer(self.raw_code):
                name = m.group(1)
                if name in existing_names:
                    continue
                line = self.raw_code.count("\n", 0, m.start()) + 1
                node = FlowNode(
                    node_id=f"src:dlt:{name}:{line}",
                    kind=FlowNodeKind.SOURCE,
                    operation_type=OperationType.READ,
                    dataset=DatasetIdentity(name=name, state=EvidenceState.KNOWN, kind="table"),
                    location=SourceLocation(
                        file=(self.analysis.source_file if self.analysis else None), line=line
                    ),
                    metadata={"dlt": True},
                    provenance=_static(),
                )
                self.add_node(node, is_source=True)
                existing_names.add(name)
        for pattern in (_DLT_STREAMING_TABLE_RE, _DLT_TABLE_DECORATOR_RE, _DLT_TABLE_DECORATOR_FN_RE):
            for m in pattern.finditer(self.raw_code):
                name = m.group(1)
                line = self.raw_code.count("\n", 0, m.start()) + 1
                node = FlowNode(
                    node_id=f"tgt:dlt:{name}:{line}",
                    kind=FlowNodeKind.TARGET,
                    dataset=DatasetIdentity(name=name, state=EvidenceState.KNOWN, kind="table"),
                    location=SourceLocation(
                        file=(self.analysis.source_file if self.analysis else None), line=line
                    ),
                    metadata={"dlt": True},
                    provenance=_static(),
                )
                self.add_node(node, is_target=True)

    # -- Contract evidence -------------------------------------------------------
    def _apply_contract_evidence(self) -> None:
        contract = self.contract
        if contract is None:
            return
        src = getattr(contract, "source", None)
        tgt = getattr(contract, "target", None)

        sources = self.nodes_of_kind_local(FlowNodeKind.SOURCE)
        if src is not None:
            declared = getattr(src, "path", "") or getattr(src, "name", "")
            matched = False
            for s in sources:
                if s.dataset and declared and s.dataset.name == declared:
                    s.provenance = FlowProvenance(
                        kind=EvidenceProvenanceKind.CONTRACT,
                        state=EvidenceState.KNOWN,
                        reference="contract source",
                    )
                    matched = True
            if not matched and not sources and declared:
                node = FlowNode(
                    node_id="src:contract",
                    kind=FlowNodeKind.SOURCE,
                    operation_type=OperationType.READ,
                    dataset=DatasetIdentity(
                        name=declared,
                        state=EvidenceState.KNOWN,
                        kind="table",
                        format=getattr(getattr(src, "format", None), "value", None),
                    ),
                    provenance=_contract_prov(EvidenceState.KNOWN),
                )
                self.add_node(node, is_source=True)
            elif not matched and len(sources) == 1 and sources[0].dataset and sources[0].dataset.name == "UNKNOWN":
                sources[0].dataset.name = declared
                sources[0].dataset.state = EvidenceState.DERIVED
                sources[0].provenance = _contract_prov()

        targets = self.nodes_of_kind_local(FlowNodeKind.TARGET)
        if tgt is not None:
            declared_t = getattr(tgt, "path", "") or getattr(tgt, "target_id", "")
            matched = False
            for t in targets:
                if t.dataset and declared_t and t.dataset.name == declared_t:
                    t.provenance = FlowProvenance(
                        kind=EvidenceProvenanceKind.CONTRACT,
                        state=EvidenceState.KNOWN,
                        reference="contract target",
                    )
                    matched = True
            if not matched and not targets and declared_t:
                node = FlowNode(
                    node_id="tgt:contract",
                    kind=FlowNodeKind.TARGET,
                    dataset=DatasetIdentity(name=declared_t, state=EvidenceState.KNOWN, kind="table"),
                    provenance=_contract_prov(EvidenceState.KNOWN),
                )
                self.add_node(node, is_target=True)
            elif not matched and len(targets) == 1 and targets[0].dataset and targets[0].dataset.name == "UNKNOWN":
                targets[0].dataset.name = declared_t
                targets[0].dataset.state = EvidenceState.DERIVED
                targets[0].provenance = _contract_prov()

    # -- Volume baselines (Phase 2) ------------------------------------------------
    def _apply_volume_baselines(self, *, attribute_mode: bool = False) -> None:
        """Attach pipeline volume baselines to SOURCE/TARGET nodes (Phase 2).

        SOURCE nodes share the pipeline input baseline from
        ``extract_baseline_volume`` (runtime → profile → contract precedence).
        TARGET nodes carry measured runtime output bytes only when stages were
        actually observed (genuine zero preserved; missing stays ``None``).
        Intermediate OPERATION/DATASET/SHUFFLE nodes are never touched:
        per-operation volume is UNKNOWN without operation↔stage correlation.

        Phase 9 (P9-4): with ``attribute_mode=True`` the SOURCE fan-out is
        skipped entirely — SOURCE nodes keep ``volume=None`` (UNKNOWN) unless
        exact attribution (``flow.attribution.attribute_volumes``) attaches an
        observation afterward. TARGET measured totals are unaffected (observed
        pipeline evidence, not baseline copies).
        """
        if not attribute_mode:
            baseline_gb, baseline_prov = extract_baseline_volume(
                self.contract, self.data_profile, self.runtime_run
            )
            source_volume = _baseline_source_volume(baseline_gb, baseline_prov, self.data_profile)
            if source_volume is not None:
                for node in self.nodes_of_kind_local(FlowNodeKind.SOURCE):
                    if node.volume is None:
                        node.volume = source_volume.model_copy(deep=True)

        output_bytes: int | None = None
        stages = getattr(self.runtime_run, "stages", None) if self.runtime_run is not None else None
        if stages:
            total_out = getattr(self.runtime_run, "total_output_bytes", None)
            if total_out is not None:
                output_bytes = int(total_out)
        if output_bytes is not None:
            for node in self.nodes_of_kind_local(FlowNodeKind.TARGET):
                if node.volume is None:
                    node.volume = VolumeObservation(
                        output_bytes=output_bytes,
                        provenance=FlowProvenance(
                            kind=EvidenceProvenanceKind.RUNTIME,
                            state=EvidenceState.KNOWN,
                            reference="runtime total_output_bytes",
                        ),
                    )

    def nodes_of_kind_local(self, kind: FlowNodeKind) -> list[FlowNode]:
        return [n for n in self.nodes if n.kind == kind]


def build_pipeline_flow_graph(
    code_analysis: CodeAnalysis | None = None,
    contract: PipelineContract | None = None,
    pipeline_name: str = "",
    raw_code: str = "",
    data_profile: DataProfile | None = None,
    runtime_run: object | None = None,
    query_history: list[QueryHistoryEntry] | list[dict[str, object]] | None = None,
    history_provenance: FlowProvenance | None = None,
    volume_mode: str = "pipeline",
) -> PipelineFlowGraph:
    """Build the common pipeline flow graph from existing evidence.

    Used identically by offline validation (contract + local code) and online
    validation (synthesized contract + Databricks-retrieved code). Never raises
    for analysis shapes it does not understand — it simply produces a smaller
    (honest) graph.

    Phase 2: ``data_profile`` / ``runtime_run`` feed SOURCE/TARGET pipeline
    volume baselines via ``extract_baseline_volume`` (same precedence both
    paths). When absent, node volumes stay ``None`` (UNKNOWN).

    Phase 3: ``query_history`` feeds SQL operation↔runtime correlation
    (UNKNOWN by default; KNOWN only via fingerprint + authoritative query
    id). PySpark/DLT operations never receive records.

    Phase 9 (P9-4): ``volume_mode="attributed"`` skips the SOURCE pipeline
    fan-out (UNKNOWN unless ``flow.attribution.attribute_volumes`` attaches
    an exact observation afterward); ``"pipeline"`` (default) keeps legacy
    behavior byte-identical for existing consumers.
    """
    try:
        builder = _GraphBuilder(
            code_analysis=code_analysis,
            contract=contract,
            pipeline_name=pipeline_name,
            raw_code=raw_code,
            data_profile=data_profile,
            runtime_run=runtime_run,
            query_history=query_history,
            history_provenance=history_provenance,
            volume_mode=volume_mode,
        )
        return builder.build()
    except Exception:  # pragma: no cover - defensive: graph must never break validation
        return PipelineFlowGraph(
            pipeline_name=pipeline_name or (contract.pipeline_name if contract else "unknown")
        )
