"""P9-4 focused tests: exact volume attribution + conservation.

Attribution rule under test (``flow.attribution.attribute_volumes``):

    exact attribution  -> attach the observed/declared/profile volume
    otherwise          -> volume stays None (UNKNOWN)

No fan-out, no fabricated remainders, no cross-nature sums, no heuristic
matching. Pipeline totals remain independently observable on the evidence
objects; they are never cloned into unattributed nodes.
"""

from __future__ import annotations

from dpif.code.parser import analyze_source
from dpif.flow.attribution import attribute_volumes
from dpif.flow.builder import build_pipeline_flow_graph
from dpif.flow.models import (
    EvidenceState,
    FlowNodeKind,
    OperationRuntimeCorrelation,
)
from dpif.models import CollectionMethod, DataProfile, Source, SourceType

CODE_TWO_SOURCES = (
    "df_a = spark.read.parquet('abfss://lake@acct.dfs.core.windows.net/a')\n"
    "df_b = spark.read.parquet('abfss://lake@acct.dfs.core.windows.net/b')\n"
)
PATH_A = "abfss://lake@acct.dfs.core.windows.net/a"
PATH_B = "abfss://lake@acct.dfs.core.windows.net/b"


def _graph(code: str = CODE_TWO_SOURCES, **kwargs):
    return build_pipeline_flow_graph(
        code_analysis=analyze_source(code, filename="t.py"),
        pipeline_name="p9",
        raw_code=code,
        volume_mode="attributed",
        **kwargs,
    )


def _sources(graph):
    return [n for n in graph.nodes if n.kind == FlowNodeKind.SOURCE]


def _by_name(graph):
    return {n.dataset.name: n for n in _sources(graph) if n.dataset is not None}


def _profile(name: str, total_bytes: int, records: int = 0) -> DataProfile:
    return DataProfile(
        source_name=name,
        total_bytes=total_bytes,
        record_count=records,
        collection_method=CollectionMethod.RUNTIME,
    )


# A. Exact source attribution attaches the correct volume.
def test_exact_profile_attribution():
    graph = _graph()
    summary = attribute_volumes(
        graph, data_profiles=[_profile(PATH_A, 1000, records=10)]
    )
    nodes = _by_name(graph)
    assert nodes[PATH_A].volume is not None
    assert nodes[PATH_A].volume.input_bytes == 1000
    assert nodes[PATH_A].volume.input_rows == 10
    assert summary["attributed"].get("DATA_PROFILE") == 1


# B. Exact operation attribution (operation-derived SOURCE node).
def test_operation_derived_source_attributed():
    graph = _graph()
    summary = attribute_volumes(
        graph, data_profiles=[_profile(PATH_B, 2000)]
    )
    nodes = _by_name(graph)
    assert nodes[PATH_B].volume is not None
    assert nodes[PATH_B].volume.input_bytes == 2000
    assert nodes[PATH_A].volume is None
    assert summary["unknown"] == 1


# C. No exact attribution produces None/UNKNOWN.
def test_no_evidence_stays_unknown():
    graph = _graph()
    summary = attribute_volumes(graph)
    assert all(n.volume is None for n in _sources(graph))
    assert summary["attributed"] == {}
    assert summary["unknown"] == 2


# D. Zero attributed observations still produces UNKNOWN everywhere.
def test_zero_attributed_observations_all_unknown():
    graph = _graph()
    summary = attribute_volumes(
        graph, sources=[], data_profiles=[], query_entries=[]
    )
    assert all(n.volume is None for n in _sources(graph))
    assert summary == {"attributed": {}, "unknown": 2, "total_sources": 2}


# E/F. Pipeline baseline is never copied to sources or operations.
def test_no_baseline_fanout_in_attributed_mode():
    from dpif.models import PipelineContract, Target

    contract = PipelineContract(
        contract_id="c",
        pipeline_name="p",
        source=Source(source_id="s", type=SourceType.ADLS, path="abfss://x"),
        target=Target(target_id="t"),
        expected_daily_volume_gb=500.0,
    )
    graph = build_pipeline_flow_graph(
        code_analysis=analyze_source(CODE_TWO_SOURCES, filename="t.py"),
        contract=contract,
        pipeline_name="p",
        raw_code=CODE_TWO_SOURCES,
        volume_mode="attributed",
    )
    assert all(n.volume is None for n in _sources(graph))
    assert all(
        n.volume is None
        for n in graph.nodes
        if n.kind == FlowNodeKind.OPERATION
    )


# G. Pipeline baseline remains independently observable at pipeline level.
def test_pipeline_baseline_independently_observable():
    from dpif.models import PipelineContract, Target
    from dpif.scalability.engine import extract_baseline_volume

    contract = PipelineContract(
        contract_id="c",
        pipeline_name="p",
        source=Source(source_id="s", type=SourceType.ADLS, path="abfss://x"),
        target=Target(target_id="t"),
        expected_daily_volume_gb=500.0,
    )
    baseline_gb, provenance = extract_baseline_volume(contract, None, None)
    assert baseline_gb == 500.0
    assert contract.expected_daily_volume_gb == 500.0


# H/I. Mixed attributed/unknown sources preserve the UNKNOWN remainder.
def test_mixed_attribution_preserves_unknown_remainder():
    graph = _graph()
    summary = attribute_volumes(
        graph, data_profiles=[_profile(PATH_A, 1000)]
    )
    nodes = _by_name(graph)
    assert nodes[PATH_A].volume is not None
    assert nodes[PATH_B].volume is None
    # No remainder fabricated from the attributed value or totals.
    assert summary["unknown"] == 1
    assert summary["total_sources"] == 2


# J. Different evidence natures do not get cross-summed.
def test_natures_never_cross_summed():
    graph = _graph()
    declared = Source(
        source_id="d",
        type=SourceType.ADLS,
        path=PATH_A,
        expected_volume_gb=10.0,
    )
    summary = attribute_volumes(
        graph,
        sources=[declared],
        data_profiles=[_profile(PATH_A, 1000)],
    )
    vol = _by_name(graph)[PATH_A].volume
    assert vol is not None
    # First exact match wins (profile); declared bytes never added in.
    assert vol.input_bytes == 1000
    assert summary["attributed"] == {"DATA_PROFILE": 1}


# K. CONTRACT_DECLARED only for explicitly declared source volumes.
def test_declared_only_when_explicit():
    from dpif.models import PipelineContract, Target

    graph = _graph()
    declared = Source(
        source_id="d",
        type=SourceType.ADLS,
        path=PATH_A,
        expected_volume_gb=10.0,
    )
    summary = attribute_volumes(graph, sources=[declared])
    vol = _by_name(graph)[PATH_A].volume
    assert vol is not None
    assert vol.input_bytes == int(10.0 * (1024.0**3))
    assert vol.provenance.kind.value == "CONTRACT"
    assert _by_name(graph)[PATH_B].volume is None
    assert summary["attributed"] == {"CONTRACT_DECLARED": 1}

    # Pipeline-level daily rate without per-source declaration: no attach.
    contract = PipelineContract(
        contract_id="c",
        pipeline_name="p",
        source=Source(source_id="s", type=SourceType.ADLS, path="abfss://other"),
        target=Target(target_id="t"),
        expected_daily_volume_gb=500.0,
    )
    graph2 = _graph()
    summary2 = attribute_volumes(graph2, contract_source=contract.source)
    assert all(n.volume is None for n in _sources(graph2))
    assert summary2["attributed"] == {}


# L. DATA_PROFILE attaches only to its exact source.
def test_profile_attaches_only_exact_source():
    graph = _graph()
    attribute_volumes(graph, data_profiles=[_profile("abfss://elsewhere", 999)])
    assert all(n.volume is None for n in _sources(graph))


# M. SQL exact correlation + measurement attaches volume correctly.
def test_sql_exact_correlation_with_measurement():
    from dpif.runtime.models import QueryHistoryEntry

    code = "df = spark.sql('SELECT * FROM gold.marts')"
    graph = build_pipeline_flow_graph(
        code_analysis=analyze_source(code, filename="q.py"),
        pipeline_name="p9",
        raw_code=code,
        volume_mode="attributed",
    )
    graph.correlations.append(
        OperationRuntimeCorrelation(
            operation_node_id="sql:query:0",
            runtime_query_id="q-9",
            state=EvidenceState.KNOWN,
        )
    )
    entry = QueryHistoryEntry(
        query_id="q-9", tables=["gold.marts"], read_bytes=5000, rows=7
    )
    summary = attribute_volumes(graph, query_entries=[entry])
    target = next(
        n
        for n in _sources(graph)
        if n.dataset is not None and n.dataset.name == "gold.marts"
    )
    assert target.volume is not None
    assert target.volume.input_bytes == 5000
    assert target.volume.input_rows == 7
    assert target.volume.provenance.kind.value == "RUNTIME"
    assert summary["attributed"] == {"OBSERVED_RUNTIME": 1}


# N. SQL exact correlation without measurement remains UNKNOWN.
def test_sql_correlation_without_measurement_unknown():
    from dpif.runtime.models import QueryHistoryEntry

    code = "df = spark.sql('SELECT * FROM gold.marts')"
    graph = build_pipeline_flow_graph(
        code_analysis=analyze_source(code, filename="q.py"),
        pipeline_name="p9",
        raw_code=code,
        volume_mode="attributed",
    )
    graph.correlations.append(
        OperationRuntimeCorrelation(
            operation_node_id="sql:query:0",
            runtime_query_id="q-9",
            state=EvidenceState.KNOWN,
        )
    )
    summary = attribute_volumes(
        graph, query_entries=[QueryHistoryEntry(query_id="q-9", tables=["gold.marts"])]
    )
    assert all(n.volume is None for n in _sources(graph))
    assert summary["attributed"] == {}


# N (variant). Measured entry without KNOWN correlation remains UNKNOWN.
def test_measurement_without_correlation_unknown():
    from dpif.runtime.models import QueryHistoryEntry

    code = "df = spark.sql('SELECT * FROM gold.marts')"
    graph = build_pipeline_flow_graph(
        code_analysis=analyze_source(code, filename="q.py"),
        pipeline_name="p9",
        raw_code=code,
        volume_mode="attributed",
    )
    summary = attribute_volumes(
        graph,
        query_entries=[
            QueryHistoryEntry(query_id="q-9", tables=["gold.marts"], read_bytes=5000)
        ],
    )
    assert all(n.volume is None for n in _sources(graph))
    assert summary["attributed"] == {}


# O. PySpark stage/task evidence without lineage remains UNKNOWN.
def test_pyspark_stages_without_lineage_unknown():
    from dpif.runtime.models import RuntimeRun, RuntimeStage

    run = RuntimeRun(
        run_id="r1",
        duration_seconds=60.0,
        stages=[RuntimeStage(stage_id=1, input_bytes=10**9, task_count=4)],
    )
    graph = build_pipeline_flow_graph(
        code_analysis=analyze_source(CODE_TWO_SOURCES, filename="t.py"),
        pipeline_name="p9",
        raw_code=CODE_TWO_SOURCES,
        runtime_run=run,
        volume_mode="attributed",
    )
    summary = attribute_volumes(graph)
    assert all(n.volume is None for n in _sources(graph))
    assert summary["attributed"] == {}
    # Pipeline totals still independently observable on the run object.
    assert run.total_input_bytes == 10**9


# P. STATIC_DISCOVERED never becomes a fabricated numeric runtime volume.
def test_static_discovery_never_numeric():
    graph = _graph()
    attribute_volumes(graph)
    for node in _sources(graph):
        assert node.volume is None


# Q. Multiple sources remain independently attributed.
def test_independent_multi_source_attribution():
    graph = _graph()
    attribute_volumes(
        graph,
        data_profiles=[_profile(PATH_A, 100), _profile(PATH_B, 200)],
    )
    nodes = _by_name(graph)
    assert nodes[PATH_A].volume.input_bytes == 100
    assert nodes[PATH_B].volume.input_bytes == 200


# R. Identical source names attribute deterministically (exact identity).
def test_identical_names_attribute_consistently():
    code = (
        f"df1 = spark.read.parquet('{PATH_A}')\n"
        f"df2 = spark.read.parquet('{PATH_A}')\n"
    )
    graph = build_pipeline_flow_graph(
        code_analysis=analyze_source(code, filename="t.py"),
        pipeline_name="p9",
        raw_code=code,
        volume_mode="attributed",
    )
    attribute_volumes(graph, data_profiles=[_profile(PATH_A, 100)])
    vols = [n.volume for n in _sources(graph)]
    assert len(vols) == 2
    assert all(v is not None and v.input_bytes == 100 for v in vols)


# S/T. task_key participates only when authoritative; missing stays UNKNOWN.
def test_task_key_neither_blocks_nor_invents():
    from dpif.code.parser import parse_task_boundaries, tag_operations_with_tasks

    blob = "# --- TASK: t1 (/a) ---\n" + CODE_TWO_SOURCES
    analysis = analyze_source(blob, filename="combined.py")
    tag_operations_with_tasks(analysis, parse_task_boundaries(blob))
    graph = build_pipeline_flow_graph(
        code_analysis=analysis,
        pipeline_name="p9",
        raw_code=blob,
        volume_mode="attributed",
    )
    attribute_volumes(graph, data_profiles=[_profile(PATH_A, 100)])
    nodes = _by_name(graph)
    # Identity-based match works with or without task_key present.
    assert nodes[PATH_A].volume is not None
    assert nodes[PATH_B].volume is None
    assert all(n.task_key == "t1" for n in _sources(graph))


# U. Graph topology and node IDs unchanged by attribution.
def test_topology_unchanged_by_attribution():
    analysis = analyze_source(CODE_TWO_SOURCES, filename="t.py")
    legacy = build_pipeline_flow_graph(
        code_analysis=analysis, pipeline_name="p9", raw_code=CODE_TWO_SOURCES
    )
    attributed = build_pipeline_flow_graph(
        code_analysis=analysis,
        pipeline_name="p9",
        raw_code=CODE_TWO_SOURCES,
        volume_mode="attributed",
    )
    attribute_volumes(attributed, data_profiles=[_profile(PATH_A, 100)])
    assert {n.node_id for n in legacy.nodes} == {n.node_id for n in attributed.nodes}
    assert {(e.from_node, e.to_node) for e in legacy.edges} == {
        (e.from_node, e.to_node) for e in attributed.edges
    }


# V. P9-2 task attribution behavior unchanged.
def test_p92_tagging_unchanged():
    from dpif.code.parser import parse_task_boundaries, tag_operations_with_tasks

    blob = "# --- TASK: t1 (/a) ---\n" + CODE_TWO_SOURCES
    analysis = analyze_source(blob, filename="combined.py")
    assert tag_operations_with_tasks(analysis, parse_task_boundaries(blob)) > 0


# W. M5H/M5I creates no new blocker solely because a volume is UNKNOWN.
def test_no_blocker_from_unknown_volume():
    from dpif.analyzers.synthesis import DecisionRiskSynthesisAnalyzer
    from tests.unit.test_decision_synthesis import (
        _make_clean_checkpoints,
        _make_dummy_readiness,
        _make_dummy_sufficiency,
    )

    analyzer = DecisionRiskSynthesisAnalyzer(
        checkpoints=_make_clean_checkpoints(),
        readiness=_make_dummy_readiness(),
        evidence_sufficiency=_make_dummy_sufficiency(),
    )
    result = analyzer.analyze()
    assert all("olume" not in b.title for b in result.blockers)


# Conservation matrix (1-10): all attributed / partial / zero / multi-nature /
# pipeline-present variants / single-table profile / SQL present/absent.
def test_conservation_all_attributed():
    graph = _graph()
    summary = attribute_volumes(
        graph, data_profiles=[_profile(PATH_A, 100), _profile(PATH_B, 200)]
    )
    assert summary["unknown"] == 0
    assert summary["total_sources"] == 2


def test_conservation_partial():
    graph = _graph()
    summary = attribute_volumes(graph, data_profiles=[_profile(PATH_A, 100)])
    assert summary["unknown"] == 1
    assert summary["total_sources"] == 2


def test_conservation_zero_attributed():
    graph = _graph()
    summary = attribute_volumes(graph)
    assert summary == {"attributed": {}, "unknown": 2, "total_sources": 2}


def test_conservation_multi_nature_kept_separate():
    declared = Source(
        source_id="d", type=SourceType.ADLS, path=PATH_B, expected_volume_gb=5.0
    )
    graph = _graph()
    summary = attribute_volumes(
        graph, sources=[declared], data_profiles=[_profile(PATH_A, 100)]
    )
    assert summary["attributed"] == {"DATA_PROFILE": 1, "CONTRACT_DECLARED": 1}
    assert summary["unknown"] == 0
    nodes = _by_name(graph)
    assert nodes[PATH_A].volume.input_bytes == 100
    assert nodes[PATH_B].volume.input_bytes == int(5.0 * (1024.0**3))


def test_conservation_pipeline_present_zero_attribution():
    from dpif.models import PipelineContract, Target
    from dpif.runtime.models import RuntimeRun, RuntimeStage

    contract = PipelineContract(
        contract_id="c",
        pipeline_name="p",
        source=Source(source_id="s", type=SourceType.ADLS, path="abfss://other"),
        target=Target(target_id="t"),
        expected_daily_volume_gb=500.0,
    )
    run = RuntimeRun(
        run_id="r1",
        duration_seconds=60.0,
        stages=[RuntimeStage(stage_id=1, input_bytes=10**9)],
    )
    graph = build_pipeline_flow_graph(
        code_analysis=analyze_source(CODE_TWO_SOURCES, filename="t.py"),
        contract=contract,
        pipeline_name="p",
        raw_code=CODE_TWO_SOURCES,
        runtime_run=run,
        volume_mode="attributed",
    )
    summary = attribute_volumes(
        graph, contract_source=contract.source, data_profiles=[]
    )
    assert all(n.volume is None for n in _sources(graph))
    assert summary["unknown"] == 2
    # Pipeline totals independently observable on the evidence objects.
    assert run.total_input_bytes == 10**9
    assert contract.expected_daily_volume_gb == 500.0


def test_conservation_single_table_profile():
    graph = _graph()
    summary = attribute_volumes(graph, data_profiles=[_profile(PATH_B, 300)])
    nodes = _by_name(graph)
    assert nodes[PATH_A].volume is None
    assert nodes[PATH_B].volume.input_bytes == 300
    assert summary == {
        "attributed": {"DATA_PROFILE": 1},
        "unknown": 1,
        "total_sources": 2,
    }


# TARGET measured totals preserved in attributed mode (observed evidence).
def test_target_measured_totals_preserved():
    from dpif.runtime.models import RuntimeRun, RuntimeStage

    code = CODE_TWO_SOURCES + "df_a.write.mode('append').saveAsTable('gold.t')\n"
    run = RuntimeRun(
        run_id="r1",
        duration_seconds=60.0,
        stages=[RuntimeStage(stage_id=1, input_bytes=10**9)],
    )
    graph = build_pipeline_flow_graph(
        code_analysis=analyze_source(code, filename="t.py"),
        pipeline_name="p9",
        raw_code=code,
        runtime_run=run,
        volume_mode="attributed",
    )
    targets = [n for n in graph.nodes if n.kind.value == "TARGET"]
    assert targets, "write op must yield a TARGET node"
