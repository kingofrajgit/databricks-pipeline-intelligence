"""Common pipeline data-flow graph model (audit GAP-001).

One graph, one vocabulary, used by BOTH offline and online validation:

    SOURCE ──▶ OPERATION ──▶ DATASET ──▶ OPERATION ──▶ SHUFFLE ──▶ OPERATION ──▶ TARGET

Design rules (from docs/PIPELINE_DATA_FLOW_INTELLIGENCE_AUDIT.md):
- Evidence-driven: every node/edge carries provenance; nothing is fabricated.
- UNKNOWN is preserved: dataset identity, locations and formats stay UNKNOWN
  when the evidence does not determine them.
- Provenance reuses the existing M5E ``EvidenceProvenanceKind`` enum — no new
-  provenance vocabulary is introduced (GAP-007 consolidation is out of scope).
- Per-operation volumes are NOT modeled (no operation↔stage correlation
-  exists yet): intermediate OPERATION/DATASET/SHUFFLE nodes keep
-  ``volume=None`` (UNKNOWN). SOURCE/TARGET nodes may carry a pipeline
-  volume baseline (``VolumeObservation``) with explicit provenance.
-  Runtime observations, partition states and cache nodes remain future
-  consumers of the graph.
- Operation↔runtime correlation (Phase 3) lives in sidecar
-  ``OperationRuntimeCorrelation`` records: UNKNOWN by default, KNOWN only
-  via explicit evidence (SQL fingerprint + query-history identifiers).
-  No per-operation volume attribution is derived from correlation.
"""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, Field

from dpif.code.models import OperationType
from dpif.models.implementation import EvidenceProvenanceKind
from dpif.models.sufficiency import ConfidenceLevel


class FlowNodeKind(StrEnum):
    """Minimum node categories for current and future flow intelligence."""

    SOURCE = "SOURCE"
    OPERATION = "OPERATION"
    DATASET = "DATASET"
    SHUFFLE = "SHUFFLE"
    TARGET = "TARGET"


class EvidenceState(StrEnum):
    """Whether an attribute of the graph is known, derived, or unknown.

    KNOWN   — stated directly by the evidence (e.g. table name in code).
    DERIVED — inferred by deterministic analysis (e.g. DataFrame variable).
    UNKNOWN — evidence unavailable; never guessed.
    """

    KNOWN = "KNOWN"
    DERIVED = "DERIVED"
    UNKNOWN = "UNKNOWN"


class SourceLocation(BaseModel):
    """Source-code location of a graph element (file/line/column/function)."""

    file: str | None = None
    line: int | None = None
    column: int | None = None
    function: str | None = None

    def label(self) -> str:
        file_part = self.file or "<code>"
        if self.line is None:
            return file_part
        return f"{file_part}:{self.line}"


class FlowProvenance(BaseModel):
    """Evidence provenance for graph elements.

    ``kind`` reuses the existing M5E ``EvidenceProvenanceKind`` (STATIC_CODE,
    RUNTIME, CONTRACT, FIXTURE, METADATA, ...) rather than introducing a new
    enum. ``state`` distinguishes KNOWN / DERIVED / UNKNOWN as required by the
    phase-1 spec.
    """

    kind: EvidenceProvenanceKind = EvidenceProvenanceKind.STATIC_CODE
    state: EvidenceState = EvidenceState.UNKNOWN
    reference: str | None = None


class DatasetIdentity(BaseModel):
    """Identity of a dataset (table/path/variable) with explicit evidence state.

    ``name`` stays ``"UNKNOWN"`` when no evidence determines it; builders must
    never fabricate names.
    """

    name: str = "UNKNOWN"
    state: EvidenceState = EvidenceState.UNKNOWN
    kind: str | None = None  # e.g. "table", "path", "sql", "variable"
    format: str | None = None


class VolumeObservation(BaseModel):
    """Evidence-backed volume attached to a flow node (Phase 2, GAP-002).

    Every numeric field is ``Optional`` and ``None`` means UNKNOWN — no
    evidence was available. A genuinely measured zero stays ``0``; builders
    must never coerce with ``or 0`` when populating these fields.

    Provenance reuses ``FlowProvenance`` (``EvidenceProvenanceKind`` +
    ``EvidenceState``); no new provenance vocabulary is introduced.
    ``state`` distinguishes KNOWN (directly evidenced, e.g. runtime bytes
    or declared contract volume) from DERIVED (representative/synthesized,
    e.g. fixture metadata).

    Only SOURCE/TARGET pipeline baselines are populated today; intermediate
    OPERATION/DATASET/SHUFFLE nodes keep ``volume=None`` (UNKNOWN) because
    no operation↔stage correlation exists yet.
    """

    input_bytes: int | None = None
    output_bytes: int | None = None
    input_rows: int | None = None
    output_rows: int | None = None
    file_count: int | None = None
    provenance: FlowProvenance = Field(default_factory=FlowProvenance)

    @property
    def status(self) -> EvidenceState:
        """KNOWN / DERIVED / UNKNOWN mirroring ``provenance.state``."""
        return self.provenance.state

    @property
    def is_known(self) -> bool:
        """Whether any numeric volume quantity is actually evidenced."""
        return any(
            v is not None
            for v in (
                self.input_bytes,
                self.output_bytes,
                self.input_rows,
                self.output_rows,
                self.file_count,
            )
        )


class FlowNode(BaseModel):
    """One node of the pipeline flow graph.

    - SOURCE nodes carry ``dataset`` identity of the read entry.
    - OPERATION nodes carry ``operation_type`` + ``location`` (file/line).
    - DATASET nodes carry ``dataset`` identity (DataFrame variable or SQL alias;
      identity state is DERIVED for variables, UNKNOWN when unresolvable).
    - SHUFFLE nodes carry ``shuffle_cause`` (the operation type that induces it).
    - TARGET nodes carry ``dataset`` identity of the sink.
    - SOURCE/TARGET nodes may carry ``volume`` (pipeline baseline); ``None``
      means UNKNOWN — never ``0`` unless genuinely measured.
    - ``task_key`` is a Phase 9 attribution slot: the Databricks job task that
      owns the code unit the node was built from. ``None`` means unattributed
      (UNKNOWN); attribution requires an authoritative task boundary.
    """

    node_id: str
    kind: FlowNodeKind
    name: str | None = None
    operation_type: OperationType | None = None
    dataset: DatasetIdentity | None = None
    location: SourceLocation | None = None
    shuffle_cause: OperationType | None = None
    volume: VolumeObservation | None = None
    task_key: str | None = None
    metadata: dict[str, object] = Field(default_factory=dict)
    provenance: FlowProvenance = Field(default_factory=FlowProvenance)

    @property
    def label(self) -> str:
        """Human-readable label for summaries (e.g. ``Filter`` / ``Join``)."""
        if self.kind == FlowNodeKind.SOURCE:
            base = "Source"
            if self.dataset and self.dataset.name != "UNKNOWN":
                base = f"Source({self.dataset.name})"
            return base
        if self.kind == FlowNodeKind.TARGET:
            base = "Target"
            if self.dataset and self.dataset.name != "UNKNOWN":
                base = f"Target({self.dataset.name})"
            return base
        if self.kind == FlowNodeKind.SHUFFLE:
            cause = self.shuffle_cause.value if self.shuffle_cause else "unknown"
            return f"Shuffle({cause})"
        if self.kind == FlowNodeKind.DATASET:
            if self.dataset and self.dataset.name != "UNKNOWN":
                return f"Dataset({self.dataset.name})"
            return "Dataset"
        if self.operation_type is not None:
            return self.operation_type.value.capitalize()
        return self.name or self.node_id


class FlowEdge(BaseModel):
    """Directed data-flow edge between two graph nodes."""

    from_node: str
    to_node: str
    dataset: str | None = None  # carrying dataset name when applicable
    provenance: FlowProvenance = Field(default_factory=FlowProvenance)


class FlowGraphIssue(BaseModel):
    """One structural validation issue (see :meth:`PipelineFlowGraph.structural_issues`)."""

    code: str  # DUPLICATE_NODE | INVALID_EDGE | CYCLE | ORPHAN_NODE | UNREACHABLE_TARGET
    message: str
    node_id: str | None = None


class CorrelationMethod(StrEnum):
    """Evidence basis of an operation↔runtime correlation (Phase 3).

    Only methods with an authoritative identifier trail are admitted.
    Positional/index/order/duration/rule-ID alignment is never a method.
    """

    SQL_FINGERPRINT = "SQL_FINGERPRINT"


class OperationRuntimeCorrelation(BaseModel):
    """Sidecar record linking a static operation to runtime evidence (Phase 3).

    UNKNOWN BY DEFAULT: a record is created per SQL operation with
    ``state=UNKNOWN`` and is upgraded to KNOWN only when an explicit,
    traceable evidence path (canonical SQL fingerprint + exactly one
    matching query-history entry carrying an authoritative query id)
    establishes it. Multiple candidates, missing identifiers, missing
    fingerprints, or unavailable history all stay UNKNOWN.

    PySpark operations and DLT nodes have no runtime-joinable identifier
    today and therefore never receive records (absence == UNKNOWN).

    Confidence reuses ``ConfidenceLevel``; provenance reuses
    ``FlowProvenance``. No per-operation volume is derived from correlation.

    ``volume`` is a Phase 9 attribution slot: it may hold a volume only when
    an exact, authoritative join (table name or ``query_id`` + fingerprint
    with response-exposed byte/row measurements) establishes it. ``None``
    means UNKNOWN — never a measured or inferred zero.
    """

    operation_node_id: str
    runtime_query_id: str | None = None
    runtime_statement_id: str | None = None
    method: CorrelationMethod = CorrelationMethod.SQL_FINGERPRINT
    state: EvidenceState = EvidenceState.UNKNOWN
    confidence: ConfidenceLevel = ConfidenceLevel.INSUFFICIENT
    evidence: dict[str, str] = Field(default_factory=dict)
    provenance: FlowProvenance = Field(default_factory=FlowProvenance)
    volume: VolumeObservation | None = None


class PipelineFlowGraph(BaseModel):
    """Directed pipeline data-flow graph common to offline and online validation."""

    pipeline_name: str
    nodes: list[FlowNode] = Field(default_factory=list)
    edges: list[FlowEdge] = Field(default_factory=list)
    source_ids: list[str] = Field(default_factory=list)
    target_ids: list[str] = Field(default_factory=list)
    correlations: list[OperationRuntimeCorrelation] = Field(default_factory=list)

    # -- lookup helpers ----------------------------------------------------
    def node(self, node_id: str) -> FlowNode | None:
        for n in self.nodes:
            if n.node_id == node_id:
                return n
        return None

    def node_ids(self) -> set[str]:
        return {n.node_id for n in self.nodes}

    def edges_from(self, node_id: str) -> list[FlowEdge]:
        return [e for e in self.edges if e.from_node == node_id]

    def edges_to(self, node_id: str) -> list[FlowEdge]:
        return [e for e in self.edges if e.to_node == node_id]

    def nodes_of_kind(self, kind: FlowNodeKind) -> list[FlowNode]:
        return [n for n in self.nodes if n.kind == kind]

    def operations_of_type(self, operation_type: OperationType) -> list[FlowNode]:
        return [
            n
            for n in self.nodes
            if n.kind == FlowNodeKind.OPERATION and n.operation_type == operation_type
        ]

    # -- traversal ----------------------------------------------------------
    def _adjacency(self) -> dict[str, list[str]]:
        adj: dict[str, list[str]] = {n.node_id: [] for n in self.nodes}
        for e in self.edges:
            adj.setdefault(e.from_node, []).append(e.to_node)
        return adj

    def _reverse_adjacency(self) -> dict[str, list[str]]:
        radj: dict[str, list[str]] = {n.node_id: [] for n in self.nodes}
        for e in self.edges:
            radj.setdefault(e.to_node, []).append(e.from_node)
        return radj

    def _reachable(self, adjacency: dict[str, list[str]], start: str) -> set[str]:
        seen: set[str] = set()
        stack = [start]
        while stack:
            cur = stack.pop()
            if cur in seen:
                continue
            seen.add(cur)
            stack.extend(adjacency.get(cur, []))
        return seen

    def ancestors_of(self, node_id: str) -> set[str]:
        """All nodes from which ``node_id`` is reachable (cycle-safe)."""
        return self._reachable(self._reverse_adjacency(), node_id) - {node_id}

    def descendants_of(self, node_id: str) -> set[str]:
        """All nodes reachable from ``node_id`` (cycle-safe)."""
        return self._reachable(self._adjacency(), node_id) - {node_id}

    def reaches_target(self, node_id: str) -> bool | None:
        """Whether ``node_id`` reaches any target.

        Returns None (UNKNOWN) when the graph declares no targets, so callers
        never mistake "no target evidence" for "disconnected".
        """
        if not self.target_ids:
            return None
        return any(t in self.descendants_of(node_id) for t in self.target_ids)

    def flow_chain(self) -> list[str]:
        """Linear label chain of the shortest source→target path.

        Intended for compact summaries (``Source → Filter → Join → Target``).
        Branches are not represented; use the full node/edge model for those.
        """
        if not self.nodes:
            return []
        adj = self._adjacency()
        start = self.source_ids[0] if self.source_ids else self.nodes[0].node_id
        goals = set(self.target_ids)
        # BFS shortest path
        from collections import deque

        queue: deque[str] = deque([start])
        prev: dict[str, str | None] = {start: None}
        while queue:
            cur = queue.popleft()
            if cur in goals:
                path: list[str] = []
                walk: str | None = cur
                while walk is not None:
                    path.append(walk)
                    walk = prev[walk]
                path.reverse()
                labels = []
                for nid in path:
                    n = self.node(nid)
                    if n is not None and n.kind != FlowNodeKind.DATASET:
                        labels.append(n.label)
                return labels
            for nxt in adj.get(cur, []):
                if nxt not in prev:
                    prev[nxt] = cur
                    queue.append(nxt)
        # No path to target: return the reachable chain from the source.
        chain: list[str] = []
        seen: set[str] = set()
        stack = [start]
        while stack:
            cur = stack.pop()
            if cur in seen:
                continue
            seen.add(cur)
            n = self.node(cur)
            if n is not None and n.kind != FlowNodeKind.DATASET:
                chain.append(n.label)
            stack.extend(adj.get(cur, []))
        return chain

    def summary(self) -> str:
        """One-line flow summary (``Source → Filter → Join → Aggregate → Target``)."""
        chain = self.flow_chain()
        return " → ".join(chain) if chain else "<empty>"

    # -- serialization -------------------------------------------------------
    def to_dict(self) -> dict[str, object]:
        """JSON-safe serialization consumed by reports and future analyzers."""
        return self.model_dump(mode="json")

    # -- structural validation ------------------------------------------------
    def structural_issues(self) -> list[FlowGraphIssue]:
        """Structural validation only — no business/optimization rules.

        Detects: duplicate node IDs, edges referencing missing nodes, cycles,
        orphan nodes, and targets unreachable from every source.
        """
        issues: list[FlowGraphIssue] = []

        # Duplicate node IDs
        seen: set[str] = set()
        for n in self.nodes:
            if n.node_id in seen:
                issues.append(
                    FlowGraphIssue(
                        code="DUPLICATE_NODE",
                        message=f"Duplicate node id '{n.node_id}'",
                        node_id=n.node_id,
                    )
                )
            seen.add(n.node_id)

        known = self.node_ids()

        # Invalid edges / missing referenced nodes
        for e in self.edges:
            if e.from_node not in known:
                issues.append(
                    FlowGraphIssue(
                        code="INVALID_EDGE",
                        message=(
                            f"Edge references missing node '{e.from_node}' "
                            f"(edge {e.from_node} -> {e.to_node})"
                        ),
                        node_id=e.from_node,
                    )
                )
            if e.to_node not in known:
                issues.append(
                    FlowGraphIssue(
                        code="INVALID_EDGE",
                        message=(
                            f"Edge references missing node '{e.to_node}' "
                            f"(edge {e.from_node} -> {e.to_node})"
                        ),
                        node_id=e.to_node,
                    )
                )

        # Cycle detection (iterative DFS with colors). The graph contract is a DAG.
        adj = self._adjacency()
        WHITE, GRAY, BLACK = 0, 1, 2
        color = {nid: WHITE for nid in known}

        def dfs(root: str) -> list[str] | None:
            stack: list[tuple[str, bool]] = [(root, False)]
            path: list[str] = []
            on_path: set[str] = set()
            while stack:
                cur, processed = stack.pop()
                if processed:
                    path.pop()
                    on_path.discard(cur)
                    color[cur] = BLACK
                    continue
                if color[cur] == GRAY:
                    continue
                if color[cur] == BLACK:
                    continue
                color[cur] = GRAY
                path.append(cur)
                on_path.add(cur)
                stack.append((cur, True))
                for nxt in adj.get(cur, []):
                    if color.get(nxt, BLACK) == GRAY:
                        return path[path.index(nxt):] + [nxt]
                    if color.get(nxt, BLACK) == WHITE:
                        stack.append((nxt, False))
            return None

        for nid in sorted(known):
            if color.get(nid, BLACK) == WHITE:
                cycle = dfs(nid)
                if cycle:
                    issues.append(
                        FlowGraphIssue(
                            code="CYCLE",
                            message="Cycle detected: " + " -> ".join(cycle),
                            node_id=cycle[0],
                        )
                    )

        # Orphan nodes (no incident edges) when the graph has multiple nodes
        if len(self.nodes) > 1:
            connected: set[str] = set()
            for e in self.edges:
                connected.add(e.from_node)
                connected.add(e.to_node)
            for n in self.nodes:
                if n.node_id not in connected:
                    issues.append(
                        FlowGraphIssue(
                            code="ORPHAN_NODE",
                            message=f"Node '{n.node_id}' has no edges",
                            node_id=n.node_id,
                        )
                    )

        # Source/target consistency: declared ids must exist; targets must be
        # reachable from at least one source.
        for sid in self.source_ids:
            if sid not in known:
                issues.append(
                    FlowGraphIssue(
                        code="INVALID_EDGE",
                        message=f"source_ids references missing node '{sid}'",
                        node_id=sid,
                    )
                )
        for tid in self.target_ids:
            if tid not in known:
                issues.append(
                    FlowGraphIssue(
                        code="INVALID_EDGE",
                        message=f"target_ids references missing node '{tid}'",
                        node_id=tid,
                    )
                )
                continue
            if self.source_ids:
                reachable_from_any_source = any(
                    tid in self._reachable(adj, sid) for sid in self.source_ids if sid in known
                )
                if self.source_ids and not reachable_from_any_source:
                    issues.append(
                        FlowGraphIssue(
                            code="UNREACHABLE_TARGET",
                            message=f"Target '{tid}' is not reachable from any source",
                            node_id=tid,
                        )
                    )

        return issues
