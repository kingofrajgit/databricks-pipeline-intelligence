"""Unit tests for M5F: Rerun / Idempotency / Duplicate-Data Forensics.

Verifies deterministic forensics across:
- Scenarios A through V
- Write semantics (APPEND, OVERWRITE, MERGE)
- Duplicate-data risk and data-loss risk
- Concurrency and retry safety correlation
- Contract idempotency mismatch
- Evidence preservation (empty vs unavailable)
"""

from __future__ import annotations

from dpif.analyzers.rerun import RerunIdempotencyAnalyzer
from dpif.code.parser import analyze_source
from dpif.contract.loader import contract_cluster_job, load_contract_file
from dpif.models import (
    CheckpointStatus,
    PipelineContract,
    ReliabilityRules,
    Severity,
    Source,
    SourceFormat,
    SourceType,
    Target,
)
from dpif.models.rerun import (
    IdempotencyDimension,
    RerunScenarioKind,
)


def _analyze(code_text: str, context: dict | None = None):
    code_analysis = analyze_source(code_text, filename="pipeline.py") if code_text else None
    if code_analysis:
        code_analysis._raw_source = code_text
    ctx = dict(context or {})
    ctx["code_snippet"] = code_text
    analyzer = RerunIdempotencyAnalyzer(code_analysis, context=ctx)
    return analyzer.analyze()


# -----------------------------------------------------------------------------
# Scenario A: Append + same-input rerun -> Potential duplicate risk
# -----------------------------------------------------------------------------
def test_scenario_a_append_same_input_rerun():
    code = """
df = spark.read.table("raw_events")
df.write.mode("append").saveAsTable("events_log")
"""
    res = _analyze(code)
    sc = res.scenarios[RerunScenarioKind.SAME_INPUT.value]
    assert sc.status == CheckpointStatus.WARN
    assert sc.severity == Severity.HIGH
    assert any(f.rule_id == "RER-DUP-001" for f in sc.findings)
    assert res.duplicate_risk.status == CheckpointStatus.WARN
    assert "Potential duplicate-data risk" in res.duplicate_risk.summary or any(
        "duplicate" in f.title.lower() for f in res.duplicate_risk.findings
    )


# -----------------------------------------------------------------------------
# Scenario B: Overwrite rerun -> Idempotent write, checks data-loss risk
# -----------------------------------------------------------------------------
def test_scenario_b_overwrite_rerun():
    code = """
df = spark.read.table("source")
df.write.mode("overwrite").saveAsTable("dest_table")
"""
    res = _analyze(code)
    sc = res.scenarios[RerunScenarioKind.SAME_INPUT.value]
    assert sc.status == CheckpointStatus.PASS
    assert any(f.rule_id == "RER-IDM-005" for f in sc.findings)
    assert res.idempotency.dimensions[IdempotencyDimension.OUTPUT_WRITE_IDEMPOTENCY.value].status == CheckpointStatus.PASS


# -----------------------------------------------------------------------------
# Scenario C: MERGE with stable key and deduplication evidence -> PASS
# -----------------------------------------------------------------------------
def test_scenario_c_merge_stable_key_evidence():
    code = """
from delta.tables import DeltaTable
df = df.dropDuplicates(["user_id"])
target = DeltaTable.forName(spark, "users")
target.alias("t").merge(df.alias("s"), "t.user_id = s.user_id").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
"""
    ctx = {"job_config": {"settings": {"max_concurrent_runs": 1, "max_retries": 0}}}
    res = _analyze(code, ctx)
    sc = res.scenarios[RerunScenarioKind.SAME_INPUT.value]
    assert sc.status == CheckpointStatus.PASS
    assert any(f.rule_id == "RER-IDM-002" for f in sc.findings)
    assert res.idempotency.overall_status == CheckpointStatus.PASS
    assert res.idempotency.is_idempotent is True


# -----------------------------------------------------------------------------
# Scenario D: MERGE with unknown key safety -> UNKNOWN or WARN
# -----------------------------------------------------------------------------
def test_scenario_d_merge_unknown_key_safety():
    code = """
from delta.tables import DeltaTable
target = DeltaTable.forName(spark, "users")
target.alias("t").merge(df.alias("s"), "t.status != s.status").whenMatchedUpdateAll().execute()
"""
    res = _analyze(code)
    sc = res.scenarios[RerunScenarioKind.SAME_INPUT.value]
    assert sc.status in (CheckpointStatus.WARN, CheckpointStatus.UNKNOWN)
    assert any(f.rule_id == "RER-IDM-004" for f in sc.findings)


# -----------------------------------------------------------------------------
# Scenario E: Overlapping input + deduplication -> PASS
# -----------------------------------------------------------------------------
def test_scenario_e_overlapping_input_with_dedup():
    code = """
df = spark.read.table("events").dropDuplicates(["event_id"])
df.write.mode("append").saveAsTable("clean_events")
"""
    res = _analyze(code)
    sc = res.scenarios[RerunScenarioKind.OVERLAPPING_INPUT.value]
    assert sc.status == CheckpointStatus.PASS
    assert any(f.rule_id == "RER-DUP-003" for f in sc.findings)


# -----------------------------------------------------------------------------
# Scenario F: Overlapping input without deduplication -> WARN / risk
# -----------------------------------------------------------------------------
def test_scenario_f_overlapping_input_without_dedup():
    code = """
df = spark.read.table("events")
df.write.mode("append").saveAsTable("clean_events")
"""
    res = _analyze(code)
    sc = res.scenarios[RerunScenarioKind.OVERLAPPING_INPUT.value]
    assert sc.status == CheckpointStatus.WARN
    assert any(f.rule_id == "RER-DUP-002" for f in sc.findings)


# -----------------------------------------------------------------------------
# Scenario G: Job retry + append -> duplicate risk under retry
# -----------------------------------------------------------------------------
def test_scenario_g_retry_append():
    code = """
df = spark.read.table("events")
df.write.mode("append").saveAsTable("events_out")
"""
    ctx = {"job_config": {"settings": {"tasks": [{"max_retries": 3}]}}}
    res = _analyze(code, ctx)
    sc = res.scenarios[RerunScenarioKind.JOB_RETRY.value]
    assert sc.status == CheckpointStatus.WARN
    assert any(f.rule_id == "RER-RET-001" for f in sc.findings)


# -----------------------------------------------------------------------------
# Scenario H: Job retry + safe MERGE evidence -> retry safe
# -----------------------------------------------------------------------------
def test_scenario_h_retry_safe_merge():
    code = """
from delta.tables import DeltaTable
df = df.dropDuplicates(["id"])
target = DeltaTable.forName(spark, "events")
target.alias("t").merge(df.alias("s"), "t.id = s.id").whenMatchedUpdateAll().execute()
"""
    ctx = {"job_config": {"settings": {"tasks": [{"max_retries": 2}]}}}
    res = _analyze(code, ctx)
    sc = res.scenarios[RerunScenarioKind.JOB_RETRY.value]
    assert sc.status == CheckpointStatus.PASS
    assert any(f.rule_id == "RER-RET-002" for f in sc.findings)


# -----------------------------------------------------------------------------
# Scenario I: Concurrent execution -> Concurrency race condition
# -----------------------------------------------------------------------------
def test_scenario_i_concurrent_execution():
    code = """
df = spark.read.table("events")
df.write.mode("append").saveAsTable("shared_table")
"""
    ctx = {"job_config": {"settings": {"max_concurrent_runs": 4}}}
    res = _analyze(code, ctx)
    sc = res.scenarios[RerunScenarioKind.CONCURRENT_EXECUTION.value]
    assert sc.status == CheckpointStatus.WARN
    assert any(f.rule_id == "RER-CON-001" for f in sc.findings)


# -----------------------------------------------------------------------------
# Scenario J: Partial failure + append -> duplicate risk
# -----------------------------------------------------------------------------
def test_scenario_j_partial_failure_append():
    code = """
df = spark.read.table("events")
df.write.mode("append").saveAsTable("stage1")
spark.sql("SELECT * FROM invalid_table")
"""
    res = _analyze(code)
    sc = res.scenarios[RerunScenarioKind.PARTIAL_FAILURE.value]
    assert sc.status == CheckpointStatus.WARN
    assert any(f.rule_id == "RER-DUP-004" for f in sc.findings)


# -----------------------------------------------------------------------------
# Scenario K: Partial failure + idempotent write -> transactional recovery
# -----------------------------------------------------------------------------
def test_scenario_k_partial_failure_idempotent_write():
    code = """
from delta.tables import DeltaTable
df = df.dropDuplicates(["id"])
target = DeltaTable.forName(spark, "target")
target.alias("t").merge(df.alias("s"), "t.id = s.id").whenMatchedUpdateAll().execute()
"""
    res = _analyze(code)
    sc = res.scenarios[RerunScenarioKind.PARTIAL_FAILURE.value]
    assert sc.status == CheckpointStatus.PASS
    assert any(f.rule_id == "RER-RET-004" for f in sc.findings)


# -----------------------------------------------------------------------------
# Scenario L: Incremental filtering -> predicate detection
# -----------------------------------------------------------------------------
def test_scenario_l_incremental_filtering():
    code = """
df = spark.read.table("events").filter("created_at >= current_date() - interval 1 day")
df.write.mode("append").saveAsTable("daily_log")
"""
    res = _analyze(code)
    sc = res.scenarios[RerunScenarioKind.INCREMENTAL_INPUT.value]
    assert sc.status == CheckpointStatus.PASS
    assert any(f.rule_id == "RER-INC-001" for f in sc.findings)


# -----------------------------------------------------------------------------
# Scenario M: Checkpoint alone -> not automatic input-idempotency PASS (UNKNOWN)
# -----------------------------------------------------------------------------
def test_scenario_m_checkpoint_evidence():
    code = """
df = spark.read.table("input_table")
df.checkpoint()
df.write.saveAsTable("dest_table")
"""
    res = _analyze(code)
    sc = res.scenarios[RerunScenarioKind.INCREMENTAL_INPUT.value]
    assert sc.status == CheckpointStatus.UNKNOWN
    assert any("Checkpointing mechanism detected" in ev for ev in sc.evidence)
    assert any(f.rule_id == "RER-INC-002" for f in sc.findings)
    assert res.idempotency.dimensions[IdempotencyDimension.INPUT_IDEMPOTENCY.value].status == CheckpointStatus.UNKNOWN


# -----------------------------------------------------------------------------
# Scenario N: Offset evidence
# -----------------------------------------------------------------------------
def test_scenario_n_offset_evidence():
    code = """
df = spark.read.format("delta").option("startingVersion", 10).load("/data")
"""
    res = _analyze(code)
    sc = res.scenarios[RerunScenarioKind.INCREMENTAL_INPUT.value]
    assert sc.status == CheckpointStatus.PASS
    assert any("Offset" in ev for ev in sc.evidence)


# -----------------------------------------------------------------------------
# Scenario O: Watermark evidence
# -----------------------------------------------------------------------------
def test_scenario_o_watermark_evidence():
    code = """
df = spark.readStream.table("events").withWatermark("timestamp", "10 minutes")
"""
    res = _analyze(code)
    sc = res.scenarios[RerunScenarioKind.LATE_ARRIVING_DATA.value]
    assert sc.status == CheckpointStatus.PASS
    assert any(f.rule_id == "RER-LATE-001" for f in sc.findings)


# -----------------------------------------------------------------------------
# Scenario P: Late-arriving-data handling via lookback + deduplication
# -----------------------------------------------------------------------------
def test_scenario_p_late_arriving_data_lookback():
    code = """
df = spark.read.table("events").filter("event_time >= date_sub(current_date(), 7)").dropDuplicates(["event_id"])
"""
    res = _analyze(code)
    sc = res.scenarios[RerunScenarioKind.LATE_ARRIVING_DATA.value]
    assert sc.status == CheckpointStatus.PASS
    assert any(f.rule_id == "RER-LATE-002" for f in sc.findings)


# -----------------------------------------------------------------------------
# Scenario Q: Contract idempotency mismatch
# -----------------------------------------------------------------------------
def test_scenario_q_contract_idempotency_mismatch():
    code = """
df = spark.read.table("source")
df.write.mode("append").saveAsTable("dest")
"""
    contract = PipelineContract(
        contract_id="test_contract",
        pipeline_name="mismatch_pipeline",
        source=Source(
            source_id="src_1",
            name="source_name",
            type=SourceType.LOCAL_FILE,
            path="/data/in",
            format=SourceFormat.PARQUET,
        ),
        target=Target(
            target_id="tgt_1",
            type="table",
            path="/data/out",
            format="parquet",
        ),
        reliability=ReliabilityRules(idempotent=True),
    )

    res = _analyze(code, {"pipeline_contract": contract})
    assert res.overall_status == CheckpointStatus.FAIL
    assert any(f.rule_id == "RER-IDM-003" and f.status == CheckpointStatus.FAIL for f in res.all_findings)


# -----------------------------------------------------------------------------
# Scenario R: Missing evidence -> UNKNOWN
# -----------------------------------------------------------------------------
def test_scenario_r_missing_evidence_unknown():
    res = _analyze("", {})
    assert res.scenarios[RerunScenarioKind.SAME_INPUT.value].status == CheckpointStatus.UNKNOWN
    assert res.idempotency.overall_status == CheckpointStatus.UNKNOWN
    assert res.duplicate_risk.status == CheckpointStatus.UNKNOWN
    assert res.data_loss_risk.status == CheckpointStatus.UNKNOWN


# -----------------------------------------------------------------------------
# Scenario S: Empty historical runs -> available empty
# -----------------------------------------------------------------------------
def test_scenario_s_empty_historical_runs():
    code = "df = spark.read.table('t')"
    res = _analyze(code, {"historical_runs": []})
    assert res.pipeline_name is not None
    # Empty history does not equate to missing context


# -----------------------------------------------------------------------------
# Scenario T: Unavailable historical runs -> preserved as None
# -----------------------------------------------------------------------------
def test_scenario_t_unavailable_historical_runs():
    code = "df = spark.read.table('t')"
    analyzer = RerunIdempotencyAnalyzer(None, {"code_snippet": code, "historical_runs": None})
    assert analyzer.historical_runs is None


# -----------------------------------------------------------------------------
# Scenario U: Overwrite potential data-loss risk (filtered unpartitioned overwrite)
# -----------------------------------------------------------------------------
def test_scenario_u_overwrite_data_loss_risk():
    code = """
df = spark.read.table("in").filter("date = current_date()")
df.write.mode("overwrite").saveAsTable("unpartitioned_dest")
"""
    res = _analyze(code)
    assert res.data_loss_risk.status == CheckpointStatus.WARN
    assert any(f.rule_id == "RER-LOSS-001" for f in res.data_loss_risk.findings)


# -----------------------------------------------------------------------------
# Scenario V: Repeated full processing performance risk
# -----------------------------------------------------------------------------
def test_scenario_v_repeated_full_processing_perf_risk():
    code = """
df = spark.read.table("full_source_table")
df.write.mode("append").saveAsTable("full_dest_table")
"""
    res = _analyze(code)
    assert any(f.rule_id == "RER-PERF-001" for f in res.performance_risks)


# -----------------------------------------------------------------------------
# Offline fixture pipeline (deterministic, no runtime or network evidence):
#   tests/fixtures/code/rerun_retry_risk_pipeline.py
#   tests/fixtures/contracts/rerun_retry_risk_pipeline.yaml
# APPEND write + checkpointLocation + max_retries=3 + max_concurrent_runs=1.
# -----------------------------------------------------------------------------
def _analyze_rerun_fixture(contracts_dir, code_dir):
    contract = load_contract_file(contracts_dir / "rerun_retry_risk_pipeline.yaml")
    code_text = (code_dir / "rerun_retry_risk_pipeline.py").read_text(encoding="utf-8")
    _, job_config = contract_cluster_job(contract)
    return _analyze(code_text, {"pipeline_contract": contract, "job_config": job_config})


# -----------------------------------------------------------------------------
# Scenario W: Fixture demonstrates retry risk (not a concurrency risk)
# -----------------------------------------------------------------------------
def test_scenario_w_fixture_retry_risk(contracts_dir, code_dir):
    res = _analyze_rerun_fixture(contracts_dir, code_dir)
    sc = res.scenarios[RerunScenarioKind.JOB_RETRY.value]
    assert sc.status == CheckpointStatus.WARN
    finding = next(f for f in sc.findings if f.rule_id == "RER-RET-001")
    assert finding.severity == Severity.HIGH
    assert finding.observed["max_retries"] == 3
    assert finding.observed["write_mode"] == "APPEND"
    assert (
        res.idempotency.dimensions[IdempotencyDimension.RETRY_IDEMPOTENCY.value].status
        == CheckpointStatus.WARN
    )
    # max_concurrent_runs=1 keeps the retry risk from being a race-condition risk.
    assert (
        res.scenarios[RerunScenarioKind.CONCURRENT_EXECUTION.value].status
        == CheckpointStatus.PASS
    )


# -----------------------------------------------------------------------------
# Scenario X: Fixture carries real checkpoint evidence for incremental input
# -----------------------------------------------------------------------------
def test_scenario_x_fixture_checkpoint_evidence(contracts_dir, code_dir):
    res = _analyze_rerun_fixture(contracts_dir, code_dir)
    sc = res.scenarios[RerunScenarioKind.INCREMENTAL_INPUT.value]
    assert sc.status == CheckpointStatus.PASS
    finding = next(f for f in sc.findings if f.rule_id == "RER-INC-001")
    assert any("Checkpointing mechanism detected" in ev for ev in sc.evidence)
    assert finding.observed["has_checkpoint"] is True


# -----------------------------------------------------------------------------
# Scenario Y: Fixture duplicate-data + idempotency assessment
# -----------------------------------------------------------------------------
def test_scenario_y_fixture_duplicate_and_idempotency(contracts_dir, code_dir):
    res = _analyze_rerun_fixture(contracts_dir, code_dir)
    for scenario in (
        RerunScenarioKind.SAME_INPUT,
        RerunScenarioKind.OVERLAPPING_INPUT,
        RerunScenarioKind.PARTIAL_FAILURE,
    ):
        assert res.scenarios[scenario.value].status == CheckpointStatus.WARN, scenario
    assert any(f.rule_id == "RER-DUP-001" for f in res.all_findings)
    assert res.duplicate_risk.status == CheckpointStatus.WARN
    assert res.duplicate_risk.risk_level == Severity.HIGH
    assert len(res.duplicate_risk.potential_duplicate_sources) == 4
    # An append-only write cannot lose data: it can only duplicate it.
    assert res.data_loss_risk.status == CheckpointStatus.PASS
    assert res.idempotency.overall_status == CheckpointStatus.WARN
    assert res.idempotency.is_idempotent is False
    assert res.overall_status == CheckpointStatus.WARN


# -----------------------------------------------------------------------------
# Scenario Z: Fixture raises no FAIL and keeps unproven areas UNKNOWN
# -----------------------------------------------------------------------------
def test_scenario_z_fixture_no_fail_and_honest_unknowns(contracts_dir, code_dir):
    res = _analyze_rerun_fixture(contracts_dir, code_dir)
    assert res.pipeline_name == "payment_events_rerun_fixture"
    assert not [f for f in res.all_findings if f.status == CheckpointStatus.FAIL]
    # The contract declares reliability.idempotent: false, so the honest
    # non-idempotent implementation must NOT be reported as a contract mismatch.
    assert not [f for f in res.all_findings if f.rule_id == "RER-IDM-003"]
    # No watermark and no upsert: late data is UNKNOWN, never a false PASS.
    late = res.scenarios[RerunScenarioKind.LATE_ARRIVING_DATA.value]
    assert late.status == CheckpointStatus.UNKNOWN
    assert not late.findings


# =============================================================================
# Hardened Regression Tests A through J (Requirement 6)
# =============================================================================


def test_regression_a_merge_id_key_no_uniqueness_unknown():
    """6A: MERGE with id-like key but no uniqueness/dedup evidence -> UNKNOWN/WARN."""
    code = """
from delta.tables import DeltaTable
target = DeltaTable.forName(spark, "users")
target.alias("t").merge(df.alias("s"), "t.id = s.id").whenMatchedUpdateAll().execute()
"""
    res = _analyze(code)
    sc = res.scenarios[RerunScenarioKind.SAME_INPUT.value]
    assert sc.status in (CheckpointStatus.UNKNOWN, CheckpointStatus.WARN)
    assert any(f.rule_id == "RER-IDM-004" for f in sc.findings)
    assert res.idempotency.dimensions[IdempotencyDimension.OUTPUT_WRITE_IDEMPOTENCY.value].status == CheckpointStatus.UNKNOWN


def test_regression_b_merge_explicit_dedup_stronger_status():
    """6B: MERGE with explicit deduplication on key -> stronger status (PASS)."""
    code = """
from delta.tables import DeltaTable
df_clean = df.dropDuplicates(["id"])
target = DeltaTable.forName(spark, "users")
target.alias("t").merge(df_clean.alias("s"), "t.id = s.id").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
"""
    res = _analyze(code)
    sc = res.scenarios[RerunScenarioKind.SAME_INPUT.value]
    assert sc.status == CheckpointStatus.PASS
    assert any(f.rule_id == "RER-IDM-002" for f in sc.findings)
    assert res.idempotency.dimensions[IdempotencyDimension.OUTPUT_WRITE_IDEMPOTENCY.value].status == CheckpointStatus.PASS


def test_regression_c_checkpoint_alone_not_automatic_input_pass():
    """6C: Checkpoint alone -> not automatic input-idempotency PASS (UNKNOWN)."""
    code = """
df = spark.read.table("stream_in")
df.write.option("checkpointLocation", "/tmp/cp").saveAsTable("out")
"""
    res = _analyze(code)
    sc = res.scenarios[RerunScenarioKind.INCREMENTAL_INPUT.value]
    assert sc.status == CheckpointStatus.UNKNOWN
    assert any(f.rule_id == "RER-INC-002" for f in sc.findings)
    assert res.idempotency.dimensions[IdempotencyDimension.INPUT_IDEMPOTENCY.value].status == CheckpointStatus.UNKNOWN


def test_regression_d_checkpoint_with_boundary_stronger_status():
    """6D: Checkpoint + actual incremental boundary -> stronger status (PASS)."""
    code = """
df = spark.readStream.table("stream_in").filter("event_time >= current_date()")
df.writeStream.option("checkpointLocation", "/tmp/cp").start()
"""
    res = _analyze(code)
    sc = res.scenarios[RerunScenarioKind.INCREMENTAL_INPUT.value]
    assert sc.status == CheckpointStatus.PASS
    assert any(f.rule_id == "RER-INC-001" for f in sc.findings)
    assert res.idempotency.dimensions[IdempotencyDimension.INPUT_IDEMPOTENCY.value].status == CheckpointStatus.PASS


def test_regression_e_watermark_evidence_not_blanket_correctness():
    """6E: Watermark -> late-data evidence but not blanket correctness."""
    code = """
df = spark.readStream.table("events").withWatermark("timestamp", "15 minutes")
"""
    res = _analyze(code)
    sc = res.scenarios[RerunScenarioKind.LATE_ARRIVING_DATA.value]
    assert sc.status == CheckpointStatus.PASS
    finding = next(f for f in sc.findings if f.rule_id == "RER-LATE-001")
    assert finding.observed["has_watermark"] is True
    assert finding.observed["blanket_correctness"] is False


def test_regression_f_merge_alone_not_blanket_late_data_pass():
    """6F: MERGE alone -> not blanket late-data PASS (UNKNOWN)."""
    code = """
from delta.tables import DeltaTable
target = DeltaTable.forName(spark, "users")
target.alias("t").merge(df.alias("s"), "t.id = s.id").whenMatchedUpdateAll().execute()
"""
    res = _analyze(code)
    sc = res.scenarios[RerunScenarioKind.LATE_ARRIVING_DATA.value]
    assert sc.status == CheckpointStatus.UNKNOWN
    assert any(f.rule_id == "RER-LATE-003" for f in sc.findings)


def test_regression_g_overwrite_full_table_destructive_replacement_not_actual_loss():
    """6G: Overwrite full-table -> potential destructive replacement, not actual loss."""
    code = """
df = spark.read.table("in").filter("created_at >= '2026-01-01'")
df.write.mode("overwrite").saveAsTable("unpartitioned_dest")
"""
    res = _analyze(code)
    assert res.data_loss_risk.status == CheckpointStatus.WARN
    finding = next(f for f in res.data_loss_risk.findings if f.rule_id == "RER-LOSS-001")
    assert "potential data-loss risk" in finding.title.lower()
    assert "actual loss" not in finding.title.lower()
    assert "destructive replacement" in finding.description.lower()


def test_regression_h_safe_complete_source_overwrite_non_failing():
    """6H: Safe complete-source overwrite -> appropriate non-failing assessment (PASS) with rebuildable target."""
    code = """
# target: rebuildable
df = spark.read.table("complete_source")
df.write.mode("overwrite").saveAsTable("derived_target")
"""
    res = _analyze(code)
    assert res.data_loss_risk.status == CheckpointStatus.PASS
    assert any(f.rule_id == "RER-LOSS-002" for f in res.data_loss_risk.findings)


def test_regression_i_output_write_pass_concurrency_warn_overall_not_pass():
    """6I: Output write PASS + concurrency WARN -> overall pipeline NOT PASS."""
    code = """
df = spark.read.table("source")
df.write.mode("overwrite").saveAsTable("target_table")
"""
    ctx = {"job_config": {"settings": {"max_concurrent_runs": 4}}}
    res = _analyze(code, ctx)
    assert res.idempotency.dimensions[IdempotencyDimension.OUTPUT_WRITE_IDEMPOTENCY.value].status == CheckpointStatus.PASS
    assert res.idempotency.dimensions[IdempotencyDimension.CONCURRENT_EXECUTION_SAFETY.value].status == CheckpointStatus.WARN
    assert res.idempotency.overall_status != CheckpointStatus.PASS
    assert res.idempotency.overall_status == CheckpointStatus.WARN
    assert res.idempotency.is_idempotent is False


def test_regression_j_output_write_pass_partial_failure_unknown_preserves_uncertainty():
    """6J: Output write PASS + partial-failure UNKNOWN -> preserve uncertainty (UNKNOWN)."""
    code = """
df = spark.read.table("source")
df.write.mode("overwrite").saveAsTable("unpartitioned_dest")
"""
    ctx = {"job_config": {"settings": {"max_concurrent_runs": 1, "max_retries": 0}}}
    res = _analyze(code, ctx)
    assert res.idempotency.dimensions[IdempotencyDimension.OUTPUT_WRITE_IDEMPOTENCY.value].status == CheckpointStatus.PASS
    assert res.idempotency.dimensions[IdempotencyDimension.PARTIAL_FAILURE_RECOVERY.value].status == CheckpointStatus.UNKNOWN
    assert res.idempotency.overall_status == CheckpointStatus.UNKNOWN
    assert res.idempotency.is_idempotent is None


# =============================================================================
# Focused Semantic Hardening Regression Tests: Issues 1, 2, and 3
# =============================================================================


def test_issue1_duplicate_risk_merge_id_key_no_uniqueness_unknown():
    """Issue 1: MERGE t.id = s.id without uniqueness or deduplication yields Idempotency=UNKNOWN and Duplicate-risk=UNKNOWN."""
    code = """
from delta.tables import DeltaTable
target = DeltaTable.forName(spark, "users")
target.alias("t").merge(df.alias("s"), "t.id = s.id").whenMatchedUpdateAll().execute()
"""
    res = _analyze(code)
    # Missing uniqueness evidence must keep Idempotency UNKNOWN
    assert res.idempotency.overall_status == CheckpointStatus.UNKNOWN
    # Duplicate-risk must NOT become PASS without established duplicate protection
    assert res.duplicate_risk.status == CheckpointStatus.UNKNOWN
    assert any(f.rule_id == "RER-IDM-004" for f in res.duplicate_risk.findings)


def test_issue2_concurrent_merge_max_runs_4_warns():
    """Issue 2A: max_concurrent_runs=4 + MERGE -> WARN with static isolation notice."""
    code = """
from delta.tables import DeltaTable
df_clean = df.dropDuplicates(["id"])
target = DeltaTable.forName(spark, "users")
target.alias("t").merge(df_clean.alias("s"), "t.id = s.id").whenMatchedUpdateAll().execute()
"""
    ctx = {"job_config": {"settings": {"max_concurrent_runs": 4}}}
    res = _analyze(code, ctx)
    conc_dim = res.idempotency.dimensions[IdempotencyDimension.CONCURRENT_EXECUTION_SAFETY.value]
    assert conc_dim.status == CheckpointStatus.WARN
    assert any(f.rule_id == "RER-CON-003" for f in conc_dim.findings)
    finding = next(f for f in conc_dim.findings if f.rule_id == "RER-CON-003")
    assert "Concurrent MERGE executions are permitted; static evidence does not establish that concurrent source/key ranges are isolated." in finding.evidence
    # Concurrency WARN prevents overall PASS
    assert res.idempotency.overall_status == CheckpointStatus.WARN


def test_issue2_concurrent_merge_max_runs_1_passes():
    """Issue 2B: max_concurrent_runs=1 + MERGE -> PASS."""
    code = """
from delta.tables import DeltaTable
df_clean = df.dropDuplicates(["id"])
target = DeltaTable.forName(spark, "users")
target.alias("t").merge(df_clean.alias("s"), "t.id = s.id").whenMatchedUpdateAll().execute()
"""
    ctx = {"job_config": {"settings": {"max_concurrent_runs": 1}}}
    res = _analyze(code, ctx)
    conc_dim = res.idempotency.dimensions[IdempotencyDimension.CONCURRENT_EXECUTION_SAFETY.value]
    assert conc_dim.status == CheckpointStatus.PASS
    assert any(f.rule_id == "RER-CON-002" for f in conc_dim.findings)


def test_issue2_concurrent_merge_max_runs_unavailable_unknown():
    """Issue 2C: max_concurrent_runs unavailable -> UNKNOWN."""
    code = """
from delta.tables import DeltaTable
df_clean = df.dropDuplicates(["id"])
target = DeltaTable.forName(spark, "users")
target.alias("t").merge(df_clean.alias("s"), "t.id = s.id").whenMatchedUpdateAll().execute()
"""
    res = _analyze(code, {})
    conc_dim = res.idempotency.dimensions[IdempotencyDimension.CONCURRENT_EXECUTION_SAFETY.value]
    assert conc_dim.status == CheckpointStatus.UNKNOWN


def test_issue3_complete_source_overwrite_unknown_target_semantics_unknown():
    """Issue 3A: Complete source + overwrite + unknown target semantics -> UNKNOWN data-loss risk."""
    code = """
df = spark.read.table("complete_source")
df.write.mode("overwrite").saveAsTable("some_target")
"""
    res = _analyze(code)
    # Write idempotency on same input is PASS
    assert res.scenarios[RerunScenarioKind.SAME_INPUT.value].status == CheckpointStatus.PASS
    # But data loss safety remains UNKNOWN because rebuildability is not established
    assert res.data_loss_risk.status == CheckpointStatus.UNKNOWN
    assert any(f.rule_id == "RER-LOSS-003" for f in res.data_loss_risk.findings)


def test_issue3_explicitly_declared_rebuildable_derived_target_passes():
    """Issue 3B: Explicitly declared rebuildable derived target -> PASS data-loss risk."""
    code = """
# target: rebuildable
df = spark.read.table("complete_source")
df.write.mode("overwrite").saveAsTable("derived_target")
"""
    res = _analyze(code)
    assert res.data_loss_risk.status == CheckpointStatus.PASS
    assert any(f.rule_id == "RER-LOSS-002" for f in res.data_loss_risk.findings)


def test_issue3_filtered_source_overwrite_warns():
    """Issue 3C: Filtered source + overwrite -> WARN potential destructive replacement risk."""
    code = """
df = spark.read.table("in").filter("status = 'ACTIVE'")
df.write.mode("overwrite").saveAsTable("unpartitioned_target")
"""
    res = _analyze(code)
    assert res.data_loss_risk.status == CheckpointStatus.WARN
    assert any(f.rule_id == "RER-LOSS-001" for f in res.data_loss_risk.findings)

