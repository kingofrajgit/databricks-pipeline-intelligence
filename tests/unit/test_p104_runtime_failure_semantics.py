"""P10-3/D4 focused tests: runtime failure/status evidence semantics.

 Acquisition taxonomy (provider/orchestrator) is preserved untouched:

     []   = valid empty result (no failures observed)
     None = acquisition failure (failure information unavailable)

 Normalization contract (new behavior under test):

 - absent/None status key            -> caller default (pre-existing)
 - allowlisted strings               -> upper-cased passthrough
 - Jobs state dict, result SUCCESS   -> "SUCCESS"
 - Jobs state dict, result FAILED    -> "FAILED"
 - anything else present             -> "UNKNOWN" (never str(dict),
   never inferred failure, never invented health)

 Analyzer contract: zero measured failures + uninterpretable status yields
 NO finding (UNKNOWN downstream via the checkpoint builder), never a
 failure-risk claim. Genuine measured failures still report.
"""

from __future__ import annotations

from dpif.runtime.analyzers import analyze_task_failures_and_retries
from dpif.runtime.models import RuntimeRun, RuntimeStage
from dpif.runtime.normalization import normalize_run_status, normalize_runtime_payload


def _run(**overrides):
    base = {"run_id": "r-1", "duration_seconds": 60.0}
    base.update(overrides)
    return normalize_runtime_payload(base)


# --- normalization contract -------------------------------------------------

def test_missing_status_keeps_default():
    assert normalize_run_status(None) == "SUCCESS"
    assert _run().status == "SUCCESS"


def test_allowlisted_strings_pass_through():
    for raw, expected in [
        ("SUCCESS", "SUCCESS"),
        ("success", "SUCCESS"),
        ("FAILED", "FAILED"),
        ("failed", "FAILED"),
        ("KILLED", "KILLED"),
        ("COMPLETE", "COMPLETE"),
        ("ERROR", "ERROR"),
    ]:
        assert normalize_run_status(raw) == expected, raw


def test_state_dict_success_normalizes():
    run = _run(state={"life_cycle_state": "TERMINATED", "result_state": "SUCCESS"})
    assert run.status == "SUCCESS"


def test_state_dict_failure_normalizes():
    run = _run(state={"life_cycle_state": "TERMINATED", "result_state": "FAILED"})
    assert run.status == "FAILED"


def test_unknown_structured_status_is_unknown():
    run = _run(state={"life_cycle_state": "RUNNING"})
    assert run.status == "UNKNOWN"


def test_unknown_string_status_is_unknown():
    assert normalize_run_status("TERMINATED") == "UNKNOWN"
    assert normalize_run_status("CANCELED") == "UNKNOWN"


def test_malformed_status_is_unknown():
    assert normalize_run_status("") == "UNKNOWN"
    assert normalize_run_status(42) == "UNKNOWN"
    assert normalize_run_status(["SUCCESS"]) == "UNKNOWN"
    run = _run(status={"unexpected": "shape"})
    assert run.status == "UNKNOWN"


def test_no_stringified_dict_status():
    run = _run(state={"life_cycle_state": "TERMINATED", "result_state": "SUCCESS"})
    assert "life_cycle_state" not in run.status
    assert "{" not in run.status


def test_stage_and_task_status_share_contract():
    run = _run(
        stages=[
            {
                "stage_id": 1,
                "status": {"life_cycle_state": "TERMINATED", "result_state": "SUCCESS"},
                "tasks": [{"task_id": 1, "stage_id": 1, "status": {"foo": "bar"}}],
            }
        ]
    )
    assert run.stages[0].status == "SUCCESS"
    assert run.stages[0].tasks[0].status == "UNKNOWN"


# --- analyzer gate ----------------------------------------------------------

# 1. Valid successful run → no failure finding.
def test_valid_success_no_finding():
    run = _run(status="SUCCESS")
    assert analyze_task_failures_and_retries(run) == []


# 2/3. Valid failed task / failed stage → existing failure finding.
def test_valid_failed_task_detected():
    run = _run(
        stages=[
            {
                "stage_id": 1,
                "status": "COMPLETE",
                "tasks": [{"task_id": 1, "stage_id": 1, "status": "FAILED"}],
            }
        ]
    )
    out = analyze_task_failures_and_retries(run)
    assert out, "genuine task failure must still report"
    assert out[0]["observed"]["failed_task_count"] == 1


def test_valid_failed_stage_detected():
    run = RuntimeRun(
        run_id="r-1",
        status="FAILED",
        stages=[RuntimeStage(stage_id=1, status="FAILED")],
    )
    out = analyze_task_failures_and_retries(run)
    assert out, "genuine stage failure must still report"
    assert out[0]["level"] == "CRITICAL"


# 4. Valid failed run keeps existing semantics.
def test_valid_failed_run_semantics():
    run = _run(status="FAILED")
    out = analyze_task_failures_and_retries(run)
    assert out, "explicit FAILED run status must still report"


# 10/12. Unknown status with zero failures → no finding (never a risk claim).
def test_unknown_status_zero_failures_no_finding():
    run = _run(
        status={"life_cycle_state": "RUNNING"},
        stages=[{"stage_id": 1, "status": "COMPLETE"}],
    )
    assert run.status == "UNKNOWN"
    assert analyze_task_failures_and_retries(run) == []


def test_no_false_zero_failure_claim():
    run = _run(state={"life_cycle_state": "TERMINATED", "result_state": "SUCCESS"})
    assert run.status == "SUCCESS"
    assert analyze_task_failures_and_retries(run) == []


# --- None vs [] through downstream consumers ---------------------------------

def _sufficiency_for(historical):
    from dpif.analyzers.sufficiency import EvidenceSufficiencyAnalyzer
    from dpif.code.parser import analyze_source

    analysis = analyze_source("df = spark.read.table('a.t')", filename="etl.py")
    analyzer = EvidenceSufficiencyAnalyzer(
        analysis, context={"historical_runs": historical}
    )
    return analyzer.analyze()


def test_valid_empty_history_no_failure():
    assessment = _sufficiency_for([])
    assert assessment.domain_coverages["historical_runs"].decision_sufficient is False


def test_none_history_is_unavailable_not_failure():
    assessment = _sufficiency_for(None)
    assert assessment.domain_coverages["historical_runs"].decision_sufficient is False
    assert assessment.overall_decision_sufficiency is False


def test_none_and_empty_agree_downstream_without_fail():
    for historical in (None, []):
        assessment = _sufficiency_for(historical)
        assert "historical_runs" in assessment.domain_coverages


# --- provider taxonomy untouched ---------------------------------------------

def test_provider_none_vs_empty_preserved():
    from unittest.mock import MagicMock

    from dpif.providers.base import DatabricksEvidenceProvider, EvidenceCategory

    none_conn = MagicMock()
    none_conn.mode.return_value = "live-api"
    none_conn.host = "https://test.databricks.net"
    none_conn._token = None
    none_conn.resource_id = "job-1"
    none_conn.get_recent_runs.return_value = None
    none_ev = DatabricksEvidenceProvider(connector=none_conn).acquire_pipeline_evidence(
        pipeline_id="job-1", job_id=1, include_historical_runs=True
    )
    none_item = none_ev.items.get(EvidenceCategory.HISTORICAL_RUNS.value)
    assert none_item.is_available is False
    assert none_item.payload is None

    empty_conn = MagicMock()
    empty_conn.mode.return_value = "live-api"
    empty_conn.host = "https://test.databricks.net"
    empty_conn._token = None
    empty_conn.resource_id = "job-1"
    empty_conn.get_recent_runs.return_value = {"runs": []}
    empty_ev = DatabricksEvidenceProvider(connector=empty_conn).acquire_pipeline_evidence(
        pipeline_id="job-1", job_id=1, include_historical_runs=True
    )
    empty_item = empty_ev.items.get(EvidenceCategory.HISTORICAL_RUNS.value)
    assert empty_item.is_available is True
    assert empty_item.payload == []


# --- 404 acquisition failure is unavailable, never a workload failure --------

def test_resource_not_found_is_unavailable():
    from unittest.mock import MagicMock

    from dpif.connectors.live import DatabricksApiError
    from dpif.providers.base import DatabricksEvidenceProvider, EvidenceCategory

    conn = MagicMock()
    conn.mode.return_value = "live-api"
    conn.host = "https://test.databricks.net"
    conn._token = None
    conn.resource_id = "job-1"
    conn.get_recent_runs.side_effect = DatabricksApiError("missing", status_code=404)
    evidence = DatabricksEvidenceProvider(connector=conn).acquire_pipeline_evidence(
        pipeline_id="job-1", job_id=1, include_historical_runs=True
    )
    item = evidence.items.get(EvidenceCategory.HISTORICAL_RUNS.value)
    assert item.is_available is False
    assert item.payload is None
    assert "RESOURCE_NOT_FOUND" in str(item.error.error_code)


# --- retry/failure detection intact -------------------------------------------

def test_retry_detection_intact():
    run = _run(
        status="SUCCESS",
        stages=[
            {
                "stage_id": 1,
                "status": "COMPLETE",
                "tasks": [
                    {"task_id": 1, "stage_id": 1, "status": "FAILED"},
                    {"task_id": 2, "stage_id": 1, "status": "SUCCESS"},
                ],
            }
        ],
    )
    out = analyze_task_failures_and_retries(run, max_failed_tasks=0)
    assert out and out[0]["observed"]["failed_task_count"] == 1
