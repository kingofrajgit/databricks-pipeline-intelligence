"""P9-2 focused tests: task attribution tagging (authoritative boundaries only).

Task analysis is attribution metadata ONLY: it must never create checkpoint
findings, feed rules/M5E/M5G, duplicate global findings, or change any
decision input. The combined analysis remains the sole authority for all
consumers; only a tagged deep copy reaches the flow-graph builder.
"""

from __future__ import annotations

from dpif.analyzers.alignment import ThreeLayerAlignmentAnalyzer
from dpif.analyzers.implementation import DeveloperImplementationAnalyzer
from dpif.checkpoints.definitions import build_all_checkpoints
from dpif.checkpoints.engine import CheckpointEngine
from dpif.code.parser import (
    analyze_source,
    analyze_tasks_for_attribution,
    parse_task_boundaries,
    tag_operations_with_tasks,
)
from dpif.flow.builder import build_pipeline_flow_graph

TASK_A = "df_a = spark.read.table('bronze.events')\nfiltered = df_a.filter(df_a.id > 0)\n"
TASK_B = "df_b = spark.read.parquet('abfss://lake@acct.dfs.core.windows.net/raw')\n"


def _combined() -> str:
    return (
        "# --- TASK: ingest (/etl/ingest) ---\n"
        + TASK_A
        + "\n\n# --- TASK: load (/etl/load) ---\n"
        + TASK_B
    )


# 1. Authoritative task boundary produces task_key.
def test_header_boundary_tags_operations():
    analysis = analyze_source(_combined(), filename="combined.py")
    assert analysis.operations, "fixture must yield operations"
    tagged = tag_operations_with_tasks(analysis, parse_task_boundaries(_combined()))
    assert tagged == len(analysis.operations)
    by_task: dict[str | None, int] = {}
    for op in analysis.operations:
        by_task[op.task_key] = by_task.get(op.task_key, 0) + 1
    assert set(by_task) == {"ingest", "load"}


# 2. Missing authoritative boundary produces task_key=None.
def test_no_boundary_leaves_unknown():
    analysis = analyze_source(TASK_A + TASK_B, filename="plain.py")
    assert tag_operations_with_tasks(analysis, parse_task_boundaries(TASK_A + TASK_B)) == 0
    assert all(op.task_key is None for op in analysis.operations)
    assert tag_operations_with_tasks(analysis, []) == 0
    assert tag_operations_with_tasks(analysis, None) == 0


# 3. No heuristic attribution: identical code attributed by position only.
def test_identical_code_attributed_by_position_not_content():
    read = "df = spark.read.table('bronze.events')\n"
    blob = "# --- TASK: first (/a) ---\n" + read + "\n\n# --- TASK: second (/b) ---\n" + read
    analysis = analyze_source(blob, filename="combined.py")
    assert tag_operations_with_tasks(analysis, parse_task_boundaries(blob)) == 2
    keys = [op.task_key for op in analysis.operations]
    assert keys == ["first", "second"]


# 3b. Malformed headers are ignored (no guessed boundary).
def test_malformed_header_ignored():
    blob = "# --- TASK  oops (/a) ---\n" + TASK_A
    analysis = analyze_source(blob, filename="combined.py")
    assert parse_task_boundaries(blob) == []
    assert tag_operations_with_tasks(analysis, parse_task_boundaries(blob)) == 0


# 4. Every analyzed task's authoritative operations receive the correct key.
def test_every_task_operations_correctly_keyed():
    entries = [
        {"task_key": "ingest", "path": "/etl/ingest", "source_code": TASK_A, "language": "python"},
        {"task_key": "load", "path": "/etl/load", "source_code": TASK_B, "language": "python"},
    ]
    per_task = analyze_tasks_for_attribution(entries)
    assert set(per_task) == {"ingest", "load"}
    assert per_task["ingest"] is not None and per_task["load"] is not None
    assert all(op.task_key is None for op in per_task["ingest"].operations)
    combined = analyze_source(_combined(), filename="combined.py")
    tag_operations_with_tasks(combined, parse_task_boundaries(_combined()))
    ingest_codes = {op.code for op in per_task["ingest"].operations}
    for op in combined.operations:
        if op.task_key == "ingest":
            assert op.code in ingest_codes


# 5. Unsupported/unretrievable tasks produce no fabricated operations.
def test_uncovered_tasks_yield_no_operations():
    per_task = analyze_tasks_for_attribution(
        [
            {"task_key": "dash", "task_type": "dashboard"},
            {"task_key": "gone", "source_code": ""},
            {"task_key": "bad"},
            "not-a-dict",
        ]
    )
    assert per_task == {"dash": None, "gone": None, "bad": None}


# 6/7. Synthetic child keyed by own key; missing parent stays None/UNKNOWN.
def test_synthetic_and_missing_parent_keys():
    per_task = analyze_tasks_for_attribution(
        [
            {
                "task_key": "refresh_nb_1",
                "parent_task_key": "refresh",
                "path": "/pipe/lib",
                "source_code": TASK_A,
                "language": "python",
            },
        ]
    )
    assert set(per_task) == {"refresh_nb_1"}
    assert per_task["refresh_nb_1"] is not None
    # No parent inference: the entry is keyed by its own task_key only.


# 8/11. Combined analysis findings unchanged by tagging (no duplicates).
def test_tagging_does_not_change_checkpoint_findings():
    blob = _combined()
    baseline = analyze_source(blob, filename="combined.py")
    tagged = baseline.model_copy(deep=True)
    tag_operations_with_tasks(tagged, parse_task_boundaries(blob))

    def _findings(analysis):
        cps = build_all_checkpoints(
            contract=None, data_profile=None, code_text=blob, cluster_config=None, job_config=None
        )
        results = CheckpointEngine().run_all_checkpoints(
            cps, {"pipeline_name": "p", "code_snippet": blob, "code_analysis": analysis}
        )
        return {
            (f.rule_id, f.status.value, f.severity.value) for cp in results.values() for f in cp.findings
        }

    assert _findings(baseline) == _findings(tagged)


# 9. Task analysis produces attribution metadata only (no findings attached).
def test_task_analysis_carries_no_findings():
    entries = [
        {"task_key": "ingest", "path": "/etl/ingest", "source_code": TASK_A, "language": "python"},
    ]
    per_task = analyze_tasks_for_attribution(entries)
    analysis = per_task["ingest"]
    assert analysis is not None
    assert len(analysis.operations) > 0
    # Attribution metadata only: operations exist, nothing evaluated.
    assert all(op.task_key is None for op in analysis.operations)


# 10/14. Tagging the copy leaves the consumer original untouched.
def test_original_analysis_untouched_by_tagging():
    blob = _combined()
    original = analyze_source(blob, filename="combined.py")
    tagged = original.model_copy(deep=True)
    tag_operations_with_tasks(tagged, parse_task_boundaries(blob))
    assert all(op.task_key is None for op in original.operations)
    assert any(op.task_key is not None for op in tagged.operations)


# 12. M5E/M5G outputs semantically unchanged by tagging.
def test_m5e_m5g_unchanged_by_tagging():
    blob = _combined()
    baseline = analyze_source(blob, filename="combined.py")
    tagged = baseline.model_copy(deep=True)
    tag_operations_with_tasks(tagged, parse_task_boundaries(blob))
    ctx = {"code_snippet": blob}
    m5e_before = DeveloperImplementationAnalyzer(baseline, context=dict(ctx)).analyze()
    m5e_after = DeveloperImplementationAnalyzer(tagged, context=dict(ctx)).analyze()
    assert m5e_before.overall_status == m5e_after.overall_status
    m5g_before = ThreeLayerAlignmentAnalyzer(baseline, context=dict(ctx)).analyze()
    m5g_after = ThreeLayerAlignmentAnalyzer(tagged, context=dict(ctx)).analyze()
    assert m5g_before.overall_status == m5g_after.overall_status


# 13. Graph node IDs/topology unchanged; only task_key annotations added.
def test_graph_topology_unchanged_task_keys_added():
    blob = _combined()
    baseline = analyze_source(blob, filename="combined.py")
    tagged = baseline.model_copy(deep=True)
    tag_operations_with_tasks(tagged, parse_task_boundaries(blob))
    g0 = build_pipeline_flow_graph(code_analysis=baseline, contract=None, pipeline_name="p", raw_code=blob)
    g1 = build_pipeline_flow_graph(code_analysis=tagged, contract=None, pipeline_name="p", raw_code=blob)
    assert {n.node_id for n in g0.nodes} == {n.node_id for n in g1.nodes}
    assert {(e.from_node, e.to_node) for e in g0.edges} == {(e.from_node, e.to_node) for e in g1.edges}
    assert all(n.task_key is None for n in g0.nodes)
    assert any(n.task_key in ("ingest", "load") for n in g1.nodes)
