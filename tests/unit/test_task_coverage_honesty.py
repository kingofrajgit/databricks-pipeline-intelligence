"""Phase 8 regression tests: job task topology & validation coverage honesty.

Every enumerated Databricks Job task must carry an explicit coverage state
(ANALYZED / UNSUPPORTED / UNRETRIEVABLE). No task may silently disappear,
and validation must never collapse a multi-task job into a single primary
task. Coverage gaps surface through the existing M5H/M5I UNKNOWN and
evidence-sufficiency mechanisms (never fabricated FAIL).
"""

from __future__ import annotations

import base64
from typing import Any

from click.testing import CliRunner

from dpif.analyzers.sufficiency import EvidenceSufficiencyAnalyzer
from dpif.analyzers.synthesis import DecisionRiskSynthesisAnalyzer
from dpif.code.parser import analyze_source
from dpif.providers.base import DatabricksEvidenceProvider, EvidenceCategory
from tests.unit.test_online_discovery_parity import MockDiscoveryConnector


def _b64(text: str) -> str:
    return base64.b64encode(text.encode("utf-8")).decode("ascii")


def _provider(
    tasks: list[dict[str, Any]],
    workspace_export_map: dict[str, Any] | None = None,
    sql_query_map: dict[str, Any] | None = None,
    dbfs_read_map: dict[str, Any] | None = None,
    pipeline_map: dict[str, Any] | None = None,
) -> DatabricksEvidenceProvider:
    conn = MockDiscoveryConnector(
        job_payload={"job_id": 9001, "settings": {"name": "coverage_job", "tasks": tasks}},
        workspace_export_map=workspace_export_map or {},
        sql_query_map=sql_query_map or {},
        dbfs_read_map=dbfs_read_map or {},
    )
    if pipeline_map:
        orig = conn.get_pipeline

        def _get_pipeline(pid: str) -> Any:
            if pid in pipeline_map:
                val = pipeline_map[pid]
                if isinstance(val, Exception):
                    raise val
                return val
            return orig(pid)

        conn.get_pipeline = _get_pipeline  # type: ignore[method-assign]
    return DatabricksEvidenceProvider(connector=conn)


def _code_payload(provider: DatabricksEvidenceProvider) -> dict[str, Any]:
    evidence = provider.acquire_pipeline_evidence(pipeline_id="job-9001", job_id=9001)
    item = evidence.items.get(EvidenceCategory.CODE.value)
    assert item is not None
    assert item.payload is not None
    return item.payload


def _topology_by_key(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {t["task_key"]: t for t in payload.get("task_topology", [])}


# A. Single supported task → ANALYZED.
def test_single_supported_task_analyzed():
    code = "df = spark.read.table('bronze.users')"
    payload = _code_payload(
        _provider(
            [{"task_key": "etl", "notebook_task": {"notebook_path": "/etl/nb"}}],
            workspace_export_map={"/etl/nb": {"content": _b64(code), "file_type": "PYTHON"}},
        )
    )
    topo = _topology_by_key(payload)
    assert set(topo) == {"etl"}
    assert topo["etl"]["coverage_state"] == "ANALYZED"
    assert topo["etl"]["task_type"] == "notebook"
    assert payload["coverage_summary"]["analyzed"] == 1


# B. Multiple supported tasks → every task represented.
def test_multiple_supported_tasks_all_represented():
    payload = _code_payload(
        _provider(
            [
                {"task_key": "step1", "notebook_task": {"notebook_path": "/etl/step1"}},
                {
                    "task_key": "step2",
                    "depends_on": [{"task_key": "step1"}],
                    "spark_python_task": {"python_file": "/etl/step2.py"},
                },
            ],
            workspace_export_map={
                "/etl/step1": {"content": _b64("df = spark.read.table('a.t1')"), "file_type": "PYTHON"},
                "/etl/step2.py": {"content": _b64("df2 = spark.read.table('a.t2')"), "file_type": "PYTHON"},
            },
        )
    )
    topo = _topology_by_key(payload)
    assert set(topo) == {"step1", "step2"}
    assert all(t["coverage_state"] == "ANALYZED" for t in topo.values())
    assert "TASK: step1" in payload["combined_code"]
    assert "TASK: step2" in payload["combined_code"]


# C. Mixed supported task types → each independently classified.
def test_mixed_supported_types_classified():
    payload = _code_payload(
        _provider(
            [
                {"task_key": "nb", "notebook_task": {"notebook_path": "/etl/nb"}},
                {"task_key": "qry", "sql_task": {"query": {"query_id": "q-1"}}},
            ],
            workspace_export_map={"/etl/nb": {"content": _b64("x = 1"), "file_type": "PYTHON"}},
            sql_query_map={"q-1": {"query_text": "SELECT * FROM a.t"}},
        )
    )
    topo = _topology_by_key(payload)
    assert topo["nb"]["coverage_state"] == "ANALYZED"
    assert topo["nb"]["task_type"] == "notebook"
    assert topo["qry"]["coverage_state"] == "ANALYZED"
    assert topo["qry"]["task_type"] == "sql"


# D. Unsupported task type → UNSUPPORTED (never silent, never validated).
def test_unsupported_task_type_explicit():
    payload = _code_payload(
        _provider(
            [
                {"task_key": "nb", "notebook_task": {"notebook_path": "/etl/nb"}},
                {"task_key": "dash", "dashboard_task": {"dashboard_id": "d-1"}},
                {"task_key": "child", "run_job_task": {"job_id": 123}},
            ],
            workspace_export_map={"/etl/nb": {"content": _b64("x = 1"), "file_type": "PYTHON"}},
        )
    )
    topo = _topology_by_key(payload)
    assert set(topo) == {"nb", "dash", "child"}
    assert topo["nb"]["coverage_state"] == "ANALYZED"
    assert topo["dash"]["coverage_state"] == "UNSUPPORTED"
    assert topo["dash"]["task_type"] == "dashboard"
    assert topo["child"]["coverage_state"] == "UNSUPPORTED"
    assert "no code retrieval path" in topo["dash"]["detail"]
    summary = payload["coverage_summary"]
    assert summary["unsupported"] == 2
    assert {u["task_key"] for u in summary["uncovered_tasks"]} == {"dash", "child"}


# E. Unknown/future task type → UNSUPPORTED.
def test_unknown_future_task_type_unsupported():
    payload = _code_payload(
        _provider([{"task_key": "mystery", "quantum_task": {"qubits": 5}}])
    )
    topo = _topology_by_key(payload)
    assert topo["mystery"]["coverage_state"] == "UNSUPPORTED"
    assert topo["mystery"]["task_type"] == "unknown"


# F. Supported task with retrieval failure → UNRETRIEVABLE, UNKNOWN downstream.
def test_supported_task_retrieval_failure_unretrievable():
    from dpif.connectors.live import DatabricksApiError

    payload = _code_payload(
        _provider(
            [{"task_key": "gone", "notebook_task": {"notebook_path": "/Deleted/nb"}}],
            workspace_export_map={
                "/Deleted/nb": DatabricksApiError("gone", status_code=404)
            },
        )
    )
    topo = _topology_by_key(payload)
    assert topo["gone"]["coverage_state"] == "UNRETRIEVABLE"
    assert payload["combined_code"] == ""
    assert payload["coverage_summary"]["unretrievable"] == 1


# G. Mixed success/failure → each keeps its own state.
def test_partial_failure_preserves_success():
    from dpif.connectors.live import DatabricksApiError

    payload = _code_payload(
        _provider(
            [
                {"task_key": "good", "notebook_task": {"notebook_path": "/etl/good"}},
                {"task_key": "bad", "notebook_task": {"notebook_path": "/etl/bad"}},
            ],
            workspace_export_map={
                "/etl/good": {"content": _b64("df = spark.read.table('a.t')"), "file_type": "PYTHON"},
                "/etl/bad": DatabricksApiError("boom", status_code=500),
            },
        )
    )
    topo = _topology_by_key(payload)
    assert topo["good"]["coverage_state"] == "ANALYZED"
    assert topo["bad"]["coverage_state"] == "UNRETRIEVABLE"
    assert "TASK: good" in payload["combined_code"]
    assert "TASK: bad" not in payload["combined_code"]


# H. Dependency preservation across all coverage states.
def test_depends_on_preserved():
    payload = _code_payload(
        _provider(
            [
                {"task_key": "a", "notebook_task": {"notebook_path": "/etl/a"}},
                {
                    "task_key": "b",
                    "depends_on": [{"task_key": "a"}],
                    "dashboard_task": {"dashboard_id": "d-1"},
                },
                {
                    "task_key": "c",
                    "depends_on": [{"task_key": "a"}, {"task_key": "b"}],
                    "notebook_task": {"notebook_path": "/etl/c"},
                },
            ],
            workspace_export_map={
                "/etl/a": {"content": _b64("x = 1"), "file_type": "PYTHON"},
                "/etl/c": {"content": _b64("y = 2"), "file_type": "PYTHON"},
            },
        )
    )
    topo = _topology_by_key(payload)
    assert topo["a"]["depends_on"] == []
    assert topo["b"]["depends_on"] == ["a"]
    assert topo["c"]["depends_on"] == ["a", "b"]
    assert topo["b"]["coverage_state"] == "UNSUPPORTED"


# I. No discovered_tasks[0] primary-task behavior: every task in combined code.
def test_no_primary_task_squashing():
    bodies = {f"t{i}": f"val{i} = {i}" for i in range(1, 4)}
    tasks = [
        {"task_key": k, "notebook_task": {"notebook_path": f"/etl/{k}"}} for k in bodies
    ]
    exports = {f"/etl/{k}": {"content": _b64(v), "file_type": "PYTHON"} for k, v in bodies.items()}
    payload = _code_payload(_provider(tasks, workspace_export_map=exports))
    for k in bodies:
        assert f"TASK: {k}" in payload["combined_code"]
    assert payload["coverage_summary"]["total_discovered"] == 3
    assert payload["coverage_summary"]["analyzed"] == 3


# J. M5H coverage visibility for uncovered tasks.
def test_m5h_reports_task_coverage_gap():
    topology = [
        {
            "task_key": "etl",
            "task_type": "notebook",
            "depends_on": [],
            "resource": "/etl/nb",
            "coverage_state": "ANALYZED",
            "detail": "notebook exported (python)",
            "code_refs": ["/etl/nb"],
        },
        {
            "task_key": "dash",
            "task_type": "dashboard",
            "depends_on": ["etl"],
            "resource": "",
            "coverage_state": "UNSUPPORTED",
            "detail": "task type 'dashboard' has no code retrieval path",
            "code_refs": [],
        },
    ]
    analysis = analyze_source("df = spark.read.table('a.t')", filename="etl.py")
    analyzer = EvidenceSufficiencyAnalyzer(analysis, context={"task_topology": topology})
    assessment = analyzer.analyze()
    job_cov = assessment.domain_coverages["job"]
    assert job_cov.decision_sufficient is False
    assert any("dash" in req for req in job_cov.required_evidence_for_sufficiency)
    task_dec = next(
        d for d in assessment.decisions if d.decision_name == "TASK_COVERAGE"
    )
    assert task_dec.decision_status == "UNKNOWN"
    assert task_dec.is_sufficient is False
    assert any("dash" in m for m in task_dec.missing_evidence)


# J (control). Fully covered topology keeps M5H sufficient paths intact.
def test_m5h_fully_covered_topology_passes_task_decision():
    topology = [
        {
            "task_key": "etl",
            "task_type": "notebook",
            "depends_on": [],
            "resource": "/etl/nb",
            "coverage_state": "ANALYZED",
            "detail": "notebook exported (python)",
            "code_refs": ["/etl/nb"],
        }
    ]
    analysis = analyze_source("df = spark.read.table('a.t')", filename="etl.py")
    analyzer = EvidenceSufficiencyAnalyzer(analysis, context={"task_topology": topology})
    assessment = analyzer.analyze()
    task_dec = next(
        d for d in assessment.decisions if d.decision_name == "TASK_COVERAGE"
    )
    assert task_dec.decision_status == "PASS"
    assert task_dec.is_sufficient is True


# K. M5I does not report complete validation with uncovered tasks.
def test_m5i_surfaces_task_coverage_gap():
    from tests.unit.test_decision_synthesis import (
        _make_clean_checkpoints,
        _make_dummy_readiness,
    )

    topology = [
        {
            "task_key": "gone",
            "task_type": "notebook",
            "depends_on": [],
            "resource": "/Deleted/nb",
            "coverage_state": "UNRETRIEVABLE",
            "detail": "notebook export returned no content",
            "code_refs": [],
        }
    ]
    analysis = analyze_source("df = spark.read.table('a.t')", filename="etl.py")
    suff = EvidenceSufficiencyAnalyzer(analysis, context={"task_topology": topology}).analyze()
    analyzer = DecisionRiskSynthesisAnalyzer(
        checkpoints=_make_clean_checkpoints(),
        readiness=_make_dummy_readiness(),
        evidence_sufficiency=suff,
    )
    result = analyzer.analyze()
    assert any("gone" in m for m in result.missing_evidence)
    assert result.decision_sufficiency is False


# L. Healthy all-supported job remains behaviorally stable.
def test_healthy_job_stable_end_to_end():
    from dpif.orchestration.online import OnlineValidationOrchestrator as _Orchestrator

    conn = MockDiscoveryConnector(
        job_payload={
            "job_id": 4242,
            "settings": {
                "name": "healthy",
                "tasks": [
                    {"task_key": "ingest", "notebook_task": {"notebook_path": "/etl/ingest"}},
                    {
                        "task_key": "load",
                        "depends_on": [{"task_key": "ingest"}],
                        "sql_task": {"query": {"query_id": "q-load"}},
                    },
                ],
            },
        },
        workspace_export_map={
            "/etl/ingest": {
                "content": _b64("df = spark.read.table('bronze.events')"),
                "file_type": "PYTHON",
            }
        },
        sql_query_map={"q-load": {"query_text": "SELECT * FROM bronze.events"}},
    )
    res = _Orchestrator(connector=conn).validate(job_id=4242)
    assert res.evidence_summary["code"] in ("LIVE", "PARTIAL")
    assert res.checkpoints["CP-004"].status.value in ("PASS", "WARN", "FAIL")
    code_diag = next(d for d in res.evidence_diagnostics if d.category == "code")
    assert code_diag.details.get("tasks_analyzed") == 2
    assert code_diag.details.get("tasks_unsupported") == 0
    assert code_diag.details.get("tasks_unretrievable") == 0
    assert res.evidence_sufficiency.overall_decision_sufficiency in (True, False)


# M. Realistic job: pipeline_task + dependent notebook_task.
def test_pipeline_plus_notebook_reference_topology():
    nb_code = "df = spark.read.parquet('abfss://lake@acct.dfs.core.windows.net/raw')"
    payload = _code_payload(
        _provider(
            [
                {"task_key": "refresh_pipeline", "pipeline_task": {"pipeline_id": "pipe-1"}},
                {
                    "task_key": "insights_dataset",
                    "depends_on": [{"task_key": "refresh_pipeline"}],
                    "notebook_task": {"notebook_path": "/notebooks/insights"},
                },
            ],
            workspace_export_map={
                "/notebooks/insights": {"content": _b64(nb_code), "file_type": "PYTHON"},
                "/pipe/lib_nb": {"content": _b64("d = spark.read.table('bronze.raw')"), "file_type": "PYTHON"},
            },
            pipeline_map={
                "pipe-1": {"spec": {"libraries": [{"notebook": {"path": "/pipe/lib_nb"}}]}}
            },
        )
    )
    topo = _topology_by_key(payload)
    assert set(topo) == {"refresh_pipeline", "insights_dataset"}
    assert topo["refresh_pipeline"]["coverage_state"] == "ANALYZED"
    assert topo["refresh_pipeline"]["task_type"] == "pipeline"
    assert topo["insights_dataset"]["depends_on"] == ["refresh_pipeline"]
    assert topo["insights_dataset"]["coverage_state"] == "ANALYZED"
    summary = payload["coverage_summary"]
    assert summary["analyzed"] == 2
    assert summary["unsupported"] == 0


# M (variant). Pipeline fetch failure → parent UNRETRIEVABLE, notebook intact.
def test_pipeline_fetch_failure_marks_parent_unretrievable():
    from dpif.connectors.live import DatabricksApiError

    payload = _code_payload(
        _provider(
            [
                {"task_key": "refresh_pipeline", "pipeline_task": {"pipeline_id": "pipe-x"}},
                {"task_key": "nb", "notebook_task": {"notebook_path": "/etl/nb"}},
            ],
            workspace_export_map={"/etl/nb": {"content": _b64("x = 1"), "file_type": "PYTHON"}},
            pipeline_map={"pipe-x": DatabricksApiError("nope", status_code=500)},
        )
    )
    topo = _topology_by_key(payload)
    assert topo["refresh_pipeline"]["coverage_state"] == "UNRETRIEVABLE"
    assert topo["nb"]["coverage_state"] == "ANALYZED"


# N. Offline/online alignment: offline fixture code yields an ANALYZED
# coverage entry with the same state vocabulary (no silent paths offline).
def test_offline_online_coverage_parity(tmp_path):
    contract_file = tmp_path / "contract.yaml"
    contract_file.write_text(
        "\n".join(
            [
                'contract_id: "P001"',
                'pipeline_name: "ParityPipe"',
                'environment: "production"',
                "source:",
                '  type: "adls"',
                '  path: "abfss://raw@account.dfs.core.windows.net/data"',
                '  format: "parquet"',
                "  expected_volume_gb: 10.0",
                "target:",
                '  target_id: "t1"',
                '  type: "delta"',
                '  path: "out"',
                "",
            ]
        ),
        encoding="utf-8",
    )
    code_file = tmp_path / "code.py"
    code_file.write_text("df = spark.read.table('bronze.events')", encoding="utf-8")

    from dpif.cli import cli

    runner = CliRunner()
    offline_result = runner.invoke(
        cli,
        [
            "validate",
            "--contract",
            str(contract_file),
            "--code",
            str(code_file),
            "--output-dir",
            str(tmp_path),
            "--offline",
        ],
    )
    assert offline_result.exit_code == 0, offline_result.output
    assert "Overall Score" in offline_result.output or "DPIF" in offline_result.output
