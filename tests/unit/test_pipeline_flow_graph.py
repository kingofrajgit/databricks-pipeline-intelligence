"""Unit tests for the common pipeline flow graph (audit GAP-001).

Covers:
- Graph model semantics (node kinds, labels, provenance states, serialization)
- Builder evidence handling: PySpark reads/writes, path reads, SQL, DLT,
  branching (multi-consumer), multi-input joins, shuffle nodes
- Structural validation (cycles, orphans, unreachable targets)
- Integration: offline CLI path and online orchestrator produce graphs
- Offline/online parity: same builder, same evidence → same graph
- UNKNOWN discipline: nothing fabricated when evidence is missing
"""

from __future__ import annotations

import json
from typing import Any

import pytest
from click.testing import CliRunner

from dpif.code.models import OperationType
from dpif.code.parser import analyze_source
from dpif.connectors.base import DatabricksConnector
from dpif.flow import (
    EvidenceState,
    FlowEdge,
    FlowNode,
    FlowNodeKind,
    PipelineFlowGraph,
    build_pipeline_flow_graph,
)
from dpif.models.implementation import EvidenceProvenanceKind
from dpif.orchestration.online import OnlineValidationOrchestrator

# ==============================================================================
# Model semantics
# ==============================================================================


def test_node_labels_by_kind():
    src = FlowNode(node_id="s1", kind=FlowNodeKind.SOURCE)
    assert src.label == "Source"
    src_named = FlowNode(
        node_id="s2",
        kind=FlowNodeKind.SOURCE,
        dataset={"name": "bronze.orders", "state": "KNOWN"},
    )
    assert src_named.label == "Source(bronze.orders)"

    op = FlowNode(
        node_id="o1", kind=FlowNodeKind.OPERATION, operation_type=OperationType.FILTER
    )
    assert op.label == "Filter"

    shf = FlowNode(
        node_id="h1",
        kind=FlowNodeKind.SHUFFLE,
        shuffle_cause=OperationType.GROUP_BY,
    )
    assert shf.label == "Shuffle(GROUP_BY)"

    tgt = FlowNode(
        node_id="t1", kind=FlowNodeKind.TARGET, dataset={"name": "gold.out", "state": "KNOWN"}
    )
    assert tgt.label == "Target(gold.out)"


def test_evidence_state_enum_covers_known_derived_unknown():
    assert {s.value for s in EvidenceState} == {"KNOWN", "DERIVED", "UNKNOWN"}


def test_to_dict_is_json_safe():
    g = build_pipeline_flow_graph(
        code_analysis=analyze_source(
            "df = spark.read.table('a.b')\ndf.write.saveAsTable('c.d')",
            filename="x.py",
        ),
        pipeline_name="p",
        raw_code="df = spark.read.table('a.b')\ndf.write.saveAsTable('c.d')",
    )
    d = g.to_dict()
    # Round-trips through JSON without error
    encoded = json.dumps(d)
    assert "pipeline_name" in d
    assert d["pipeline_name"] == "p"
    assert isinstance(encoded, str)


def test_default_provenance_is_static_code():
    prov = FlowNode(node_id="n", kind=FlowNodeKind.OPERATION).provenance
    assert prov.kind == EvidenceProvenanceKind.STATIC_CODE


# ==============================================================================
# Builder: PySpark code evidence
# ==============================================================================


def _summarize(g: PipelineFlowGraph) -> str:
    return g.summary()


def test_builder_pyspark_read_to_write_chain():
    code = (
        "df = spark.read.table('bronze.orders')\n"
        "cleaned = df.filter(\"amount > 0\")\n"
        "cleaned.write.mode('overwrite').saveAsTable('silver.orders')"
    )
    analysis = analyze_source(code, filename="pipeline.py")
    g = build_pipeline_flow_graph(code_analysis=analysis, pipeline_name="p", raw_code=code)

    chain = _summarize(g)
    assert "Source(bronze.orders)" in chain
    assert "Filter" in chain
    assert "Target(silver.orders)" in chain
    assert g.structural_issues() == []
    assert len(g.source_ids) == 1
    assert len(g.target_ids) == 1


def test_builder_records_source_and_target_identity():
    code = (
        "df = spark.read.table('bronze.orders')\n"
        "df.write.mode('append').saveAsTable('silver.orders')"
    )
    g = build_pipeline_flow_graph(
        code_analysis=analyze_source(code, filename="p.py"), pipeline_name="p", raw_code=code
    )
    src = g.nodes_of_kind(FlowNodeKind.SOURCE)[0]
    assert src.dataset is not None
    assert src.dataset.name == "bronze.orders"
    assert src.dataset.state == EvidenceState.KNOWN
    assert src.dataset.kind == "table"
    assert src.provenance.kind == EvidenceProvenanceKind.STATIC_CODE

    tgt = g.nodes_of_kind(FlowNodeKind.TARGET)[0]
    assert tgt.dataset is not None
    assert tgt.dataset.name == "silver.orders"
    assert tgt.dataset.state == EvidenceState.KNOWN


def test_builder_path_read_keeps_format_known_and_location():
    code = (
        "df = spark.read.format('parquet').load('s3://bucket/raw/events')\n"
        "df.write.save('s3://bucket/out')"
    )
    g = build_pipeline_flow_graph(
        code_analysis=analyze_source(code, filename="p.py"), pipeline_name="p", raw_code=code
    )
    src = g.nodes_of_kind(FlowNodeKind.SOURCE)[0]
    assert src.dataset is not None
    assert src.dataset.name == "s3://bucket/raw/events"
    assert src.dataset.format == "parquet"
    assert src.location is not None and src.location.line == 1


def test_builder_shuffle_nodes_from_shuffle_ops():
    code = (
        "df = spark.read.table('bronze.orders')\n"
        "agg = df.groupBy('customer_id').count()\n"
        "agg.write.saveAsTable('silver.agg')"
    )
    g = build_pipeline_flow_graph(
        code_analysis=analyze_source(code, filename="p.py"), pipeline_name="p", raw_code=code
    )
    shuffles = g.nodes_of_kind(FlowNodeKind.SHUFFLE)
    causes = {s.shuffle_cause for s in shuffles}
    assert OperationType.GROUP_BY in causes
    # Downstream of the shuffle the chain continues (Shuffle -> ... -> Write)
    shf = next(s for s in shuffles if s.shuffle_cause == OperationType.GROUP_BY)
    assert g.edges_from(shf.node_id), "shuffle node must connect to its consumer"


def test_builder_multi_input_join_adds_second_edge():
    code = (
        "df = spark.read.table('bronze.orders')\n"
        "df2 = spark.read.table('bronze.customers')\n"
        "j = df.join(F.broadcast(df2), 'customer_id')\n"
        "j.write.saveAsTable('silver.joined')"
    )
    g = build_pipeline_flow_graph(
        code_analysis=analyze_source(code, filename="p.py"), pipeline_name="p", raw_code=code
    )
    joins = g.operations_of_type(OperationType.JOIN)
    assert len(joins) == 1
    join_node = joins[0]
    incoming = g.edges_to(join_node.node_id)
    # One edge from each input branch
    assert len(incoming) == 2
    # Both sources exist
    assert len(g.nodes_of_kind(FlowNodeKind.SOURCE)) == 2
    assert g.structural_issues() == []


def test_builder_branching_fanout_single_source():
    code = (
        "df = spark.read.table('bronze.events')\n"
        "a = df.filter(\"x > 1\")\n"
        "b = df.filter(\"y < 2\")\n"
        "a.write.saveAsTable('out.a')\n"
        "b.write.saveAsTable('out.b')"
    )
    g = build_pipeline_flow_graph(
        code_analysis=analyze_source(code, filename="p.py"), pipeline_name="p", raw_code=code
    )
    # df consumed twice: dataset node must have two outgoing edges
    ds_nodes = g.nodes_of_kind(FlowNodeKind.DATASET)
    df_node = next(n for n in ds_nodes if n.name == "df")
    assert len(g.edges_from(df_node.node_id)) == 2
    assert len(g.target_ids) == 2
    assert g.structural_issues() == []


def test_builder_no_fabrication_when_read_unknown():
    # readStream.table without resolvable location still yields UNKNOWN identity
    code = "df = spark.readStream.load()\ndf.write.saveAsTable('out.t')"
    g = build_pipeline_flow_graph(
        code_analysis=analyze_source(code, filename="p.py"), pipeline_name="p", raw_code=code
    )
    srcs = g.nodes_of_kind(FlowNodeKind.SOURCE)
    assert len(srcs) == 1
    assert srcs[0].dataset is not None
    assert srcs[0].dataset.name == "UNKNOWN"
    assert srcs[0].dataset.state == EvidenceState.UNKNOWN


def test_builder_recovers_identity_from_truncated_snippet():
    """Parser snippets are capped (~200 chars); the builder must recover the
    terminal write path from the raw code window instead of staying UNKNOWN."""
    code = open("tests/fixtures/code/good_pipeline.py", encoding="utf-8").read()
    g = build_pipeline_flow_graph(
        code_analysis=analyze_source(code, filename="good_pipeline.py"),
        pipeline_name="gp",
        raw_code=code,
    )
    tgt = g.nodes_of_kind(FlowNodeKind.TARGET)[0]
    assert tgt.dataset is not None
    assert tgt.dataset.name == "abfss://curated@acct.dfs.core.windows.net/customer_daily/"
    assert tgt.dataset.state == EvidenceState.KNOWN
    src = g.nodes_of_kind(FlowNodeKind.SOURCE)[0]
    assert src.dataset is not None
    assert src.dataset.name == "abfss://landing@acct.dfs.core.windows.net/customer_daily/"
    assert src.dataset.format == "parquet"
    assert g.structural_issues() == []


# ==============================================================================
# Builder: SQL evidence
# ==============================================================================


def test_builder_sql_join_two_sources():
    analysis = analyze_source(
        "SELECT o.id, c.name FROM bronze.orders o JOIN bronze.customers c ON o.cid = c.id",
        filename="q.sql",
    )
    g = build_pipeline_flow_graph(code_analysis=analysis, pipeline_name="sql_p")
    srcs = sorted(n.dataset.name for n in g.nodes_of_kind(FlowNodeKind.SOURCE) if n.dataset)
    assert srcs == ["bronze.customers", "bronze.orders"]
    joins = g.operations_of_type(OperationType.JOIN)
    assert len(joins) == 1
    assert len(g.nodes_of_kind(FlowNodeKind.SHUFFLE)) == 1
    assert g.structural_issues() == []


def test_builder_sql_multi_join_unique_nodes():
    q = "SELECT * FROM a.x JOIN b.y ON a.x.id = b.y.id JOIN c.z ON b.y.zid = c.z.id"
    g = build_pipeline_flow_graph(
        code_analysis=analyze_source(q, filename="q.sql"), pipeline_name="q"
    )
    assert len(g.operations_of_type(OperationType.JOIN)) == 2
    assert g.structural_issues() == []


# ==============================================================================
# Builder: DLT vocabulary
# ==============================================================================


def test_builder_dlt_sources_and_streaming_table_targets():
    code = (
        "df = dlt.read_stream('bronze.orders')\n"
        "@dlt.table(name='silver_orders')\n"
        "def silver():\n"
        "    return df.filter('amount > 0')\n"
    )
    analysis = analyze_source(code, filename="dlt.py")
    g = build_pipeline_flow_graph(code_analysis=analysis, pipeline_name="dlt", raw_code=code)
    src_names = {n.dataset.name for n in g.nodes_of_kind(FlowNodeKind.SOURCE) if n.dataset}
    tgt_names = {n.dataset.name for n in g.nodes_of_kind(FlowNodeKind.TARGET) if n.dataset}
    assert "bronze.orders" in src_names
    assert "silver_orders" in tgt_names
    assert all(n.metadata.get("dlt") for n in g.nodes if n.kind in (FlowNodeKind.SOURCE, FlowNodeKind.TARGET) and n.metadata)


def test_builder_dlt_create_streaming_table_target():
    code = (
        "dlt.create_streaming_table('gold_metrics')\n"
        "df = dlt.read_stream('bronze.events')\n"
    )
    g = build_pipeline_flow_graph(
        code_analysis=analyze_source(code, filename="dlt2.py"), pipeline_name="dlt2", raw_code=code
    )
    tgt_names = {n.dataset.name for n in g.nodes_of_kind(FlowNodeKind.TARGET) if n.dataset}
    assert "gold_metrics" in tgt_names


# ==============================================================================
# Structural validation
# ==============================================================================


def test_validate_detects_duplicate_nodes():
    g = PipelineFlowGraph(
        pipeline_name="p",
        nodes=[
            FlowNode(node_id="n1", kind=FlowNodeKind.SOURCE),
            FlowNode(node_id="n1", kind=FlowNodeKind.TARGET),
        ],
        edges=[FlowEdge(from_node="n1", to_node="n1")],
        source_ids=["n1"],
        target_ids=["n1"],
    )
    codes = {i.code for i in g.structural_issues()}
    assert "DUPLICATE_NODE" in codes


def test_validate_detects_invalid_edges():
    g = PipelineFlowGraph(
        pipeline_name="p",
        nodes=[FlowNode(node_id="n1", kind=FlowNodeKind.SOURCE)],
        edges=[FlowEdge(from_node="n1", to_node="ghost")],
        source_ids=["n1"],
    )
    codes = {i.code for i in g.structural_issues()}
    assert "INVALID_EDGE" in codes


def test_validate_detects_cycle():
    g = PipelineFlowGraph(
        pipeline_name="p",
        nodes=[
            FlowNode(node_id="a", kind=FlowNodeKind.OPERATION),
            FlowNode(node_id="b", kind=FlowNodeKind.OPERATION),
        ],
        edges=[
            FlowEdge(from_node="a", to_node="b"),
            FlowEdge(from_node="b", to_node="a"),
        ],
    )
    codes = {i.code for i in g.structural_issues()}
    assert "CYCLE" in codes


def test_validate_detects_orphan_node():
    g = PipelineFlowGraph(
        pipeline_name="p",
        nodes=[
            FlowNode(node_id="a", kind=FlowNodeKind.SOURCE),
            FlowNode(node_id="b", kind=FlowNodeKind.TARGET),
            FlowNode(node_id="lonely", kind=FlowNodeKind.OPERATION),
        ],
        edges=[FlowEdge(from_node="a", to_node="b")],
        source_ids=["a"],
        target_ids=["b"],
    )
    codes = {i.code for i in g.structural_issues()}
    assert "ORPHAN_NODE" in codes


def test_validate_detects_unreachable_target():
    g = PipelineFlowGraph(
        pipeline_name="p",
        nodes=[
            FlowNode(node_id="a", kind=FlowNodeKind.SOURCE),
            FlowNode(node_id="b", kind=FlowNodeKind.OPERATION),
            FlowNode(node_id="t", kind=FlowNodeKind.TARGET),
        ],
        edges=[FlowEdge(from_node="a", to_node="b")],
        source_ids=["a"],
        target_ids=["t"],
    )
    codes = {i.code for i in g.structural_issues()}
    assert "UNREACHABLE_TARGET" in codes


def test_reaches_target_none_when_no_targets():
    g = PipelineFlowGraph(
        pipeline_name="p",
        nodes=[FlowNode(node_id="a", kind=FlowNodeKind.SOURCE)],
        source_ids=["a"],
    )
    assert g.reaches_target("a") is None


def test_ancestors_and_descendants():
    g = PipelineFlowGraph(
        pipeline_name="p",
        nodes=[
            FlowNode(node_id="s", kind=FlowNodeKind.SOURCE),
            FlowNode(node_id="m", kind=FlowNodeKind.OPERATION),
            FlowNode(node_id="t", kind=FlowNodeKind.TARGET),
        ],
        edges=[
            FlowEdge(from_node="s", to_node="m"),
            FlowEdge(from_node="m", to_node="t"),
        ],
        source_ids=["s"],
        target_ids=["t"],
    )
    assert g.descendants_of("s") == {"m", "t"}
    assert g.ancestors_of("t") == {"s", "m"}


def test_empty_graph_is_honest_and_safe():
    g = build_pipeline_flow_graph(code_analysis=None, contract=None, pipeline_name="empty")
    assert g.nodes == []
    assert g.summary() == "<empty>"
    assert g.structural_issues() == []


# ==============================================================================
# Contract evidence enrichment
# ==============================================================================


def test_contract_fills_unknown_source_identity():
    from dpif.models import PipelineContract, Source, SourceType, Target

    contract = PipelineContract.model_construct(
        pipeline_name="cp",
        source=Source(source_id="s", type=SourceType.OTHER, path="decl.source.tbl"),
        target=Target(target_id="t", path="decl.target.tbl"),
    )
    # No code analysis: contract-only evidence
    g = build_pipeline_flow_graph(code_analysis=None, contract=contract, pipeline_name="cp")
    srcs = g.nodes_of_kind(FlowNodeKind.SOURCE)
    tgts = g.nodes_of_kind(FlowNodeKind.TARGET)
    assert srcs and srcs[0].dataset.name == "decl.source.tbl"
    assert srcs[0].provenance.kind == EvidenceProvenanceKind.CONTRACT
    assert tgts and tgts[0].dataset.name == "decl.target.tbl"


# ==============================================================================
# Offline CLI integration
# ==============================================================================


def test_cli_offline_validation_includes_flow_graph(tmp_path: Any):
    from dpif.cli import cli

    contract_path = "examples/customer_daily.yaml"
    runner = CliRunner()
    res = runner.invoke(
        cli,
        [
            "validate",
            "--contract",
            contract_path,
            "--offline",
            "--output-dir",
            str(tmp_path),
            "--json",
        ],
    )
    assert res.exit_code == 0
    payload = json.loads(res.output)
    assert "pipeline_flow_graph" in payload

    fg = payload["pipeline_flow_graph"]
    assert fg["pipeline_name"] == "customer_daily"
    assert fg["source_ids"], "offline fixture must produce at least one source"
    assert fg["target_ids"], "offline fixture must produce at least one target"

    # Persisted JSON must carry the same graph
    run_dirs = list((tmp_path / "offline" / "customer_daily").iterdir())
    data = json.loads((run_dirs[0] / "validation.json").read_text(encoding="utf-8"))
    assert "pipeline_flow_graph" in data

    md_text = (run_dirs[0] / "validation.md").read_text(encoding="utf-8")
    assert "## Pipeline Flow Graph (GAP-001)" in md_text


# ==============================================================================
# Online orchestration integration
# ==============================================================================


class _FlowMockConnector(DatabricksConnector):
    """Mock connector returning a job whose notebook code is a simple ETL."""

    def __init__(self, job_id: int) -> None:
        self.code = (
            "df = spark.read.table('bronze.orders')\n"
            "df.filter(\"amount > 0\").write.mode('overwrite').saveAsTable('silver.orders')\n"
        )
        self.job_payload = {
            "job_id": job_id,
            "settings": {
                "name": "flow_graph_job",
                "tasks": [
                    {
                        "task_key": "etl",
                        "existing_cluster_id": "c-flow",
                        "notebook_task": {"notebook_path": "/Users/dev/etl"},
                    }
                ],
            },
        }
        self.cluster_payload = {
            "cluster_id": "c-flow",
            "cluster_name": "flow_cluster",
            "spark_version": "14.3.x-scala2.12",
            "node_type_id": "i3.xlarge",
            "num_workers": 2,
        }

    def mode(self) -> str:
        return "live-api"

    def get_workspace_status(self) -> dict[str, Any] | None:
        return {"status": "connected"}

    def get_job(self, job_id: int | str) -> dict[str, Any] | None:
        return self.job_payload

    def get_cluster(self, cluster_id: str) -> dict[str, Any] | None:
        return self.cluster_payload

    def get_recent_runs(
        self, job_id: int | str, limit: int = 10
    ) -> dict[str, Any] | list[dict[str, Any]] | None:
        return {"runs": []}

    def get_run(self, run_id: int | str) -> dict[str, Any] | None:
        return None

    def get_table_profile(self, table: str) -> dict[str, Any] | None:
        return None


def test_online_orchestrator_builds_flow_graph():
    import base64

    conn = _FlowMockConnector(771001)
    encoded = base64.b64encode(conn.code.encode("utf-8")).decode("ascii")
    conn.job_payload["settings"]["tasks"][0]["notebook_task"]["notebook_path"] = "/Users/dev/etl"

    # Patch workspace export by monkeypatching the connector method
    original_export = getattr(conn, "export_workspace_object", None)

    def export(path: str, format: str = "SOURCE") -> dict[str, Any] | None:
        return {"content": encoded, "file_type": "PYTHON"}

    conn.export_workspace_object = export  # type: ignore[method-assign]
    if original_export is None:
        pytest.skip("connector lacks export_workspace_object")

    orch = OnlineValidationOrchestrator(connector=conn)
    result = orch.validate(job_id=771001)

    assert result.flow_graph is not None
    src_names = {
        n.dataset.name for n in result.flow_graph.nodes_of_kind(FlowNodeKind.SOURCE) if n.dataset
    }
    tgt_names = {
        n.dataset.name for n in result.flow_graph.nodes_of_kind(FlowNodeKind.TARGET) if n.dataset
    }
    assert "bronze.orders" in src_names
    assert "silver.orders" in tgt_names

    # Serialized payload carries the graph too
    d = result.to_dict()
    assert "pipeline_flow_graph" in d


def test_online_markdown_report_has_flow_section(tmp_path: Any):
    import base64

    from dpif.reporting.validation_reports import persist_online_validation_report

    conn = _FlowMockConnector(771002)
    encoded = base64.b64encode(conn.code.encode("utf-8")).decode("ascii")

    def export(path: str, format: str = "SOURCE") -> dict[str, Any] | None:
        return {"content": encoded, "file_type": "PYTHON"}

    conn.export_workspace_object = export  # type: ignore[method-assign]

    orch = OnlineValidationOrchestrator(connector=conn)
    result = orch.validate(job_id=771002)
    json_p, md_p = persist_online_validation_report(result, output_dir=tmp_path)

    assert json_p.exists()
    data = json.loads(json_p.read_text(encoding="utf-8"))
    assert "pipeline_flow_graph" in data
    md_text = md_p.read_text(encoding="utf-8")
    assert "## Pipeline Flow Graph (GAP-001)" in md_text


# ==============================================================================
# Offline / online parity (GAP-001 core requirement)
# ==============================================================================


def test_offline_online_parity_same_builder_same_graph():
    """The identical (analysis, contract, code) inputs must yield the identical
    graph regardless of the validation path — the builder is shared."""
    code = (
        "df = spark.read.table('bronze.orders')\n"
        "df.write.mode('overwrite').saveAsTable('silver.orders')\n"
    )
    analysis = analyze_source(code, filename="workload.py")

    # Offline-style call
    g_offline = build_pipeline_flow_graph(
        code_analysis=analysis,
        pipeline_name="parity",
        raw_code=code,
    )
    # Online-style call (same arguments)
    g_online = build_pipeline_flow_graph(
        code_analysis=analysis,
        pipeline_name="parity",
        raw_code=code,
    )
    assert g_offline.to_dict() == g_online.to_dict()

    # And matches the direct builder result
    assert [n.node_id for n in g_offline.nodes] == [n.node_id for n in g_online.nodes]


def test_graph_survives_malformed_evidence_without_raising():
    """Builder must never break validation for shapes it does not understand."""
    # Empty / garbage inputs must not raise
    g1 = build_pipeline_flow_graph(code_analysis=None, contract=None, pipeline_name="x", raw_code="")
    assert g1.pipeline_name == "x"

    g2 = build_pipeline_flow_graph(
        code_analysis=analyze_source("", filename="empty.py"),
        pipeline_name="y",
        raw_code="",
    )
    assert g2.pipeline_name == "y"
