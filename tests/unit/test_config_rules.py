"""Comprehensive Unit Tests for Phase 6 Configuration Rules (CONFIG-JOB & CONFIG-CLUSTER).

Covers all 12 rules with 5+ test cases each:
Positive, Negative, Edge, Parameter Override, and Mismatch/Context scenarios.
"""

from __future__ import annotations

from dpif.discovery import analyzers
from dpif.discovery.models import (
    AutoscalingConfig,
    ClusterPolicy,
    DatabricksCluster,
    DatabricksJob,
    DatabricksTask,
    JobSchedule,
)
from dpif.models import (
    PipelineContract,
    ReliabilityRules,
    ScheduleRules,
    Source,
    Target,
)


def _make_contract(
    name: str = "test_pipeline",
    environment: str = "production",
    daily_volume_gb: float = 100.0,
    retry_count: int = 2,
    timeout_minutes: int = 60,
    frequency: str = "daily",
    schedule_time: str = "02:00",
    spark_version: str | None = None,
    code_file: str | None = None,
) -> PipelineContract:
    source = Source(
        source_id="source-001",
        name=f"{name}-source",
        type="adls",
        format="parquet",
        path="abfss://raw@acct.dfs.core.windows.net/data/",
        expected_volume_gb=daily_volume_gb,
        config={"code_file": code_file} if code_file else {},
    )
    target = Target(
        target_id="target-001",
        type="delta",
        catalog="prod",
        schema="gold",
        path="abfss://gold@acct.dfs.core.windows.net/data/",
    )
    contract = PipelineContract(
        contract_id="contract-001",
        pipeline_name=name,
        environment=environment,
        source=source,
        target=target,
        expected_daily_volume_gb=daily_volume_gb,
        reliability=ReliabilityRules(retry_count=retry_count, timeout_minutes=timeout_minutes),
        schedule=ScheduleRules(frequency=frequency, time=schedule_time),
        processing="incremental",
    )
    if spark_version:
        contract._cluster_raw = {"spark_version": spark_version}
    if code_file:
        contract._job_raw = {"code_file": code_file}
    return contract


# ====================================================================
# CONFIG-JOB-001: Missing Job Retries
# ====================================================================
class TestConfigJob001Retries:
    def test_positive_zero_retries_triggers(self):
        job = DatabricksJob(
            job_id=1,
            name="no_retries",
            tasks=[DatabricksTask(task_key="t1", max_retries=0)],
        )
        res = analyzers.analyze_job_retries(job, None, min_retries=1)
        assert len(res) == 1
        assert res[0]["triggered"] is True
        assert res[0]["level"] == "HIGH"

    def test_negative_sufficient_retries_passes(self):
        job = DatabricksJob(
            job_id=2,
            name="good_retries",
            tasks=[DatabricksTask(task_key="t1", max_retries=2)],
        )
        res = analyzers.analyze_job_retries(job, None, min_retries=1)
        assert len(res) == 0

    def test_edge_job_level_retries_honored(self):
        job = DatabricksJob(
            job_id=3,
            name="job_level",
            max_retries=2,
            tasks=[DatabricksTask(task_key="t1", max_retries=0)],
        )
        res = analyzers.analyze_job_retries(job, None, min_retries=1)
        assert len(res) == 0

    def test_param_override_min_retries(self):
        job = DatabricksJob(
            job_id=4,
            name="custom_threshold",
            tasks=[DatabricksTask(task_key="t1", max_retries=2)],
        )
        res = analyzers.analyze_job_retries(job, None, min_retries=3)
        assert len(res) == 1
        assert res[0]["expected"]["min_retries"] == 3

    def test_contract_reliability_retry_target(self):
        contract = _make_contract(retry_count=3)
        job = DatabricksJob(
            job_id=5,
            name="contract_mismatch",
            tasks=[DatabricksTask(task_key="t1", max_retries=1)],
        )
        res = analyzers.analyze_job_retries(job, contract, min_retries=1)
        assert len(res) == 1
        assert res[0]["expected"]["min_retries"] == 3


# ====================================================================
# CONFIG-JOB-002: Missing Task Timeout
# ====================================================================
class TestConfigJob002Timeout:
    def test_positive_zero_timeout_triggers(self):
        job = DatabricksJob(
            job_id=1,
            name="no_timeout",
            tasks=[DatabricksTask(task_key="t1", timeout_seconds=0)],
        )
        res = analyzers.analyze_task_timeouts(job, None, min_timeout_seconds=60)
        assert len(res) == 1
        assert res[0]["triggered"] is True

    def test_negative_explicit_timeout_passes(self):
        job = DatabricksJob(
            job_id=2,
            name="good_timeout",
            tasks=[DatabricksTask(task_key="t1", timeout_seconds=1800)],
        )
        res = analyzers.analyze_task_timeouts(job, None, min_timeout_seconds=60)
        assert len(res) == 0

    def test_edge_job_level_timeout_satisfies(self):
        job = DatabricksJob(
            job_id=3,
            name="job_level_timeout",
            timeout_seconds=3600,
            tasks=[DatabricksTask(task_key="t1", timeout_seconds=0)],
        )
        res = analyzers.analyze_task_timeouts(job, None, min_timeout_seconds=60)
        assert len(res) == 0

    def test_param_override_min_timeout(self):
        job = DatabricksJob(
            job_id=4,
            name="short_timeout",
            tasks=[DatabricksTask(task_key="t1", timeout_seconds=60)],
        )
        # Threshold requires at least 300s
        res = analyzers.analyze_task_timeouts(job, None, min_timeout_seconds=300)
        assert len(res) == 1

    def test_multiple_tasks_partial_timeout(self):
        job = DatabricksJob(
            job_id=5,
            name="multi_task",
            tasks=[
                DatabricksTask(task_key="t1", timeout_seconds=1800),
                DatabricksTask(task_key="t2", timeout_seconds=0),
            ],
        )
        res = analyzers.analyze_task_timeouts(job, None, min_timeout_seconds=60)
        assert len(res) == 1
        assert "t2" in res[0]["evidence"][0]


# ====================================================================
# CONFIG-JOB-003: Excessive Retries
# ====================================================================
class TestConfigJob003ExcessiveRetries:
    def test_positive_excessive_retries_triggers(self):
        job = DatabricksJob(
            job_id=1,
            name="too_many_retries",
            tasks=[DatabricksTask(task_key="t1", max_retries=10)],
        )
        res = analyzers.analyze_excessive_retries(job, None, max_allowed_retries=5)
        assert len(res) == 1
        assert res[0]["triggered"] is True
        assert res[0]["level"] == "MEDIUM"

    def test_negative_moderate_retries_passes(self):
        job = DatabricksJob(
            job_id=2,
            name="normal_retries",
            tasks=[DatabricksTask(task_key="t1", max_retries=3)],
        )
        res = analyzers.analyze_excessive_retries(job, None, max_allowed_retries=5)
        assert len(res) == 0

    def test_edge_boundary_retries_passes(self):
        job = DatabricksJob(
            job_id=3,
            name="boundary_retries",
            tasks=[DatabricksTask(task_key="t1", max_retries=5)],
        )
        res = analyzers.analyze_excessive_retries(job, None, max_allowed_retries=5)
        assert len(res) == 0

    def test_param_override_max_retries(self):
        job = DatabricksJob(
            job_id=4,
            name="strict_retries",
            tasks=[DatabricksTask(task_key="t1", max_retries=3)],
        )
        res = analyzers.analyze_excessive_retries(job, None, max_allowed_retries=2)
        assert len(res) == 1
        assert res[0]["expected"]["max_allowed_retries"] == 2

    def test_job_level_excessive_retries(self):
        job = DatabricksJob(
            job_id=5,
            name="job_excessive",
            max_retries=6,
            tasks=[DatabricksTask(task_key="t1", max_retries=1)],
        )
        res = analyzers.analyze_excessive_retries(job, None, max_allowed_retries=5)
        assert len(res) == 1


# ====================================================================
# CONFIG-JOB-004: Concurrency Risk
# ====================================================================
class TestConfigJob004Concurrency:
    def test_positive_unrestricted_concurrency_triggers(self):
        job = DatabricksJob(job_id=1, name="multi_run", max_concurrent_runs=5)
        res = analyzers.analyze_concurrency(job, None, max_allowed_concurrency=1)
        assert len(res) == 1
        assert res[0]["triggered"] is True

    def test_negative_single_concurrency_passes(self):
        job = DatabricksJob(job_id=2, name="single_run", max_concurrent_runs=1)
        res = analyzers.analyze_concurrency(job, None, max_allowed_concurrency=1)
        assert len(res) == 0

    def test_streaming_workload_allows_concurrency(self):
        contract = _make_contract()
        contract.processing = "streaming"
        job = DatabricksJob(job_id=3, name="stream_job", max_concurrent_runs=3)
        res = analyzers.analyze_concurrency(job, contract, max_allowed_concurrency=1)
        assert len(res) == 0

    def test_param_override_concurrency(self):
        job = DatabricksJob(job_id=4, name="custom_concurrency", max_concurrent_runs=3)
        res = analyzers.analyze_concurrency(job, None, max_allowed_concurrency=3)
        assert len(res) == 0

    def test_concurrency_observed_details(self):
        job = DatabricksJob(job_id=5, name="detail_check", max_concurrent_runs=4)
        res = analyzers.analyze_concurrency(job, None, max_allowed_concurrency=1)
        assert res[0]["observed"]["actual_max_concurrent_runs"] == 4


# ====================================================================
# CONFIG-JOB-005: Schedule Mismatch
# ====================================================================
class TestConfigJob005ScheduleMismatch:
    def test_positive_prod_schedule_paused_triggers(self):
        contract = _make_contract(environment="production")
        job = DatabricksJob(
            job_id=1,
            name="prod_job",
            schedule=JobSchedule(quartz_cron_expression="0 0 2 * * ?", pause_status="PAUSED"),
        )
        res = analyzers.analyze_schedule_mismatch(job, contract)
        assert len(res) == 1
        assert "PAUSED" in res[0]["evidence"][0]

    def test_positive_dev_schedule_unpaused_triggers(self):
        contract = _make_contract(environment="development")
        job = DatabricksJob(
            job_id=2,
            name="dev_job",
            schedule=JobSchedule(quartz_cron_expression="0 0 * * * ?", pause_status="UNPAUSED"),
        )
        res = analyzers.analyze_schedule_mismatch(job, contract)
        assert len(res) == 1
        assert "UNPAUSED" in res[0]["evidence"][0]

    def test_positive_cron_hour_mismatch(self):
        contract = _make_contract(environment="production", schedule_time="04:00")
        job = DatabricksJob(
            job_id=3,
            name="cron_mismatch",
            schedule=JobSchedule(quartz_cron_expression="0 0 2 * * ?", pause_status="UNPAUSED"),
        )
        res = analyzers.analyze_schedule_mismatch(job, contract)
        assert len(res) == 1
        assert "does not match contract scheduled time" in res[0]["evidence"][0]

    def test_negative_matching_prod_schedule_passes(self):
        contract = _make_contract(environment="production", schedule_time="02:00")
        job = DatabricksJob(
            job_id=4,
            name="matching_job",
            schedule=JobSchedule(quartz_cron_expression="0 0 2 * * ?", pause_status="UNPAUSED"),
        )
        res = analyzers.analyze_schedule_mismatch(job, contract)
        assert len(res) == 0

    def test_edge_no_schedule_on_job_does_not_falsely_trigger(self):
        contract = _make_contract(environment="production")
        job = DatabricksJob(job_id=5, name="no_sched_job")
        res = analyzers.analyze_schedule_mismatch(job, contract)
        assert len(res) == 0


# ====================================================================
# CONFIG-JOB-006: Source Mismatch
# ====================================================================
class TestConfigJob006SourceMismatch:
    def test_positive_wrong_script_path_triggers(self):
        contract = _make_contract(code_file="scripts/orders_etl.py")
        job = DatabricksJob(
            job_id=1,
            name="wrong_source",
            tasks=[
                DatabricksTask(task_key="t1", spark_python_task_file="/workspace/legacy_job.py")
            ],
        )
        res = analyzers.analyze_source_mismatch(job, contract)
        assert len(res) == 1
        assert res[0]["triggered"] is True

    def test_negative_matching_script_path_passes(self):
        contract = _make_contract(code_file="scripts/orders_etl.py")
        job = DatabricksJob(
            job_id=2,
            name="correct_source",
            tasks=[
                DatabricksTask(
                    task_key="t1", spark_python_task_file="abfss://scripts/orders_etl.py"
                )
            ],
        )
        res = analyzers.analyze_source_mismatch(job, contract)
        assert len(res) == 0

    def test_negative_no_expected_code_file_passes(self):
        contract = _make_contract()
        job = DatabricksJob(
            job_id=3,
            name="no_contract_file",
            tasks=[DatabricksTask(task_key="t1", spark_python_task_file="test.py")],
        )
        res = analyzers.analyze_source_mismatch(job, contract)
        assert len(res) == 0

    def test_edge_notebook_path_matching(self):
        contract = _make_contract(code_file="/Repos/prod/ingest_notebook")
        job = DatabricksJob(
            job_id=4,
            name="notebook_job",
            tasks=[DatabricksTask(task_key="t1", notebook_path="/Repos/prod/ingest_notebook")],
        )
        res = analyzers.analyze_source_mismatch(job, contract)
        assert len(res) == 0

    def test_multiple_tasks_one_matching_passes(self):
        contract = _make_contract(code_file="transform.py")
        job = DatabricksJob(
            job_id=5,
            name="dag_job",
            tasks=[
                DatabricksTask(task_key="t1", notebook_path="/init"),
                DatabricksTask(task_key="t2", spark_python_task_file="/scripts/transform.py"),
            ],
        )
        res = analyzers.analyze_source_mismatch(job, contract)
        assert len(res) == 0


# ====================================================================
# CONFIG-CLUSTER-001: Outdated Runtime Version
# ====================================================================
class TestConfigCluster001Runtime:
    def test_positive_outdated_dbr_triggers_critical(self):
        cluster = DatabricksCluster(
            cluster_id="c1",
            cluster_name="legacy",
            spark_version="11.3.x-scala2.12",
        )
        res = analyzers.analyze_cluster_runtime(cluster, None, min_version="14.3")
        assert len(res) == 1
        assert res[0]["triggered"] is True
        assert res[0]["level"] == "CRITICAL"

    def test_negative_modern_dbr_passes(self):
        cluster = DatabricksCluster(
            cluster_id="c2",
            cluster_name="modern",
            spark_version="15.4.x-scala2.12",
        )
        res = analyzers.analyze_cluster_runtime(cluster, None, min_version="14.3")
        assert len(res) == 0

    def test_edge_approved_versions_list(self):
        cluster = DatabricksCluster(
            cluster_id="c3",
            cluster_name="unapproved",
            spark_version="14.1.x-scala2.12",
        )
        res = analyzers.analyze_cluster_runtime(
            cluster, None, approved_versions=["14.3.x-scala2.12", "15.4.x-scala2.12"]
        )
        assert len(res) == 1
        assert "not in approved runtime list" in res[0]["evidence"][0]

    def test_contract_expected_spark_version_mismatch(self):
        contract = _make_contract(spark_version="15.4.x-scala2.12")
        cluster = DatabricksCluster(
            cluster_id="c4",
            cluster_name="mismatch",
            spark_version="14.3.x-scala2.12",
        )
        res = analyzers.analyze_cluster_runtime(cluster, contract, min_version="14.0")
        assert len(res) == 1
        assert "mismatch" in res[0]["evidence"][0]

    def test_param_override_min_version(self):
        cluster = DatabricksCluster(
            cluster_id="c5",
            cluster_name="custom_min",
            spark_version="14.3.x-scala2.12",
        )
        res = analyzers.analyze_cluster_runtime(cluster, None, min_version="15.0")
        assert len(res) == 1


# ====================================================================
# CONFIG-CLUSTER-002: Missing Autoscaling
# ====================================================================
class TestConfigCluster002Autoscaling:
    def test_positive_fixed_cluster_when_autoscaling_required(self):
        cluster = DatabricksCluster(
            cluster_id="c1",
            cluster_name="fixed_cluster",
            num_workers=8,
        )
        res = analyzers.analyze_autoscaling(cluster, None, require_autoscaling=True)
        assert len(res) == 1
        assert res[0]["triggered"] is True

    def test_negative_autoscaling_configured_passes(self):
        cluster = DatabricksCluster(
            cluster_id="c2",
            cluster_name="autoscale_cluster",
            autoscale=AutoscalingConfig(min_workers=2, max_workers=8),
        )
        res = analyzers.analyze_autoscaling(cluster, None, require_autoscaling=True)
        assert len(res) == 0

    def test_negative_fixed_cluster_not_required_passes(self):
        cluster = DatabricksCluster(
            cluster_id="c3",
            cluster_name="fixed_ok",
            num_workers=4,
        )
        res = analyzers.analyze_autoscaling(cluster, None, require_autoscaling=False)
        assert len(res) == 0

    def test_edge_zero_workers_single_node_not_flagged_for_small_workload(self):
        cluster = DatabricksCluster(
            cluster_id="c4",
            cluster_name="single_node",
            num_workers=0,
        )
        res = analyzers.analyze_autoscaling(cluster, None, require_autoscaling=False)
        assert len(res) == 0

    def test_variable_growth_contract_triggers_autoscaling(self):
        contract = _make_contract()
        contract.source.growth_rate_percent = 25.0
        cluster = DatabricksCluster(
            cluster_id="c5",
            cluster_name="fixed_on_growth",
            num_workers=4,
        )
        res = analyzers.analyze_autoscaling(cluster, contract, require_autoscaling=True)
        assert len(res) == 1


# ====================================================================
# CONFIG-CLUSTER-003: Autoscaling Range
# ====================================================================
class TestConfigCluster003AutoscalingRange:
    def test_positive_excessive_ratio_triggers(self):
        cluster = DatabricksCluster(
            cluster_id="c1",
            cluster_name="wide_range",
            autoscale=AutoscalingConfig(min_workers=2, max_workers=30),  # 15x
        )
        res = analyzers.analyze_autoscaling_range(cluster, None, max_ratio=10.0)
        assert len(res) == 1
        assert res[0]["triggered"] is True

    def test_positive_exceeds_max_workers_limit(self):
        cluster = DatabricksCluster(
            cluster_id="c2",
            cluster_name="huge_max",
            autoscale=AutoscalingConfig(min_workers=20, max_workers=150),
        )
        res = analyzers.analyze_autoscaling_range(cluster, None, max_workers_limit=128)
        assert len(res) == 1
        assert "exceeds limit" in res[0]["evidence"][0]

    def test_negative_healthy_range_passes(self):
        cluster = DatabricksCluster(
            cluster_id="c3",
            cluster_name="healthy_range",
            autoscale=AutoscalingConfig(min_workers=2, max_workers=8),  # 4x
        )
        res = analyzers.analyze_autoscaling_range(
            cluster, None, max_ratio=10.0, max_workers_limit=128
        )
        assert len(res) == 0

    def test_param_override_max_ratio(self):
        cluster = DatabricksCluster(
            cluster_id="c4",
            cluster_name="tight_ratio",
            autoscale=AutoscalingConfig(min_workers=2, max_workers=8),  # 4x
        )
        res = analyzers.analyze_autoscaling_range(cluster, None, max_ratio=3.0)
        assert len(res) == 1
        assert res[0]["expected"]["max_ratio"] == 3.0

    def test_edge_fixed_cluster_not_applicable(self):
        cluster = DatabricksCluster(
            cluster_id="c5",
            cluster_name="fixed_cluster",
            num_workers=8,
        )
        res = analyzers.analyze_autoscaling_range(cluster, None)
        assert len(res) == 0


# ====================================================================
# CONFIG-CLUSTER-004: Photon Disabled on Heavy Workload
# ====================================================================
class TestConfigCluster004Photon:
    def test_positive_large_volume_without_photon_triggers(self):
        contract = _make_contract(daily_volume_gb=500.0)
        contract._cluster_raw = {"photon": True}
        cluster = DatabricksCluster(
            cluster_id="c1",
            cluster_name="standard_large",
            runtime_engine="STANDARD",
        )
        res = analyzers.analyze_photon(cluster, contract)
        assert len(res) == 1
        assert res[0]["triggered"] is True

    def test_negative_photon_enabled_passes(self):
        contract = _make_contract(daily_volume_gb=500.0)
        contract._cluster_raw = {"photon": True}
        cluster = DatabricksCluster(
            cluster_id="c2",
            cluster_name="photon_large",
            runtime_engine="PHOTON",
        )
        res = analyzers.analyze_photon(cluster, contract)
        assert len(res) == 0

    def test_negative_small_volume_without_photon_passes(self):
        contract = _make_contract(daily_volume_gb=20.0)
        cluster = DatabricksCluster(
            cluster_id="c3",
            cluster_name="standard_small",
            runtime_engine="STANDARD",
        )
        res = analyzers.analyze_photon(cluster, contract)
        assert len(res) == 0

    def test_edge_contract_requests_photon_explicitly(self):
        contract = _make_contract(daily_volume_gb=10.0)
        contract._cluster_raw = {"photon": True}
        cluster = DatabricksCluster(
            cluster_id="c4",
            cluster_name="explicit_request_violated",
            runtime_engine="STANDARD",
        )
        res = analyzers.analyze_photon(cluster, contract)
        assert len(res) == 1

    def test_no_contract_defaults_clean(self):
        cluster = DatabricksCluster(
            cluster_id="c5",
            cluster_name="no_contract",
            runtime_engine="STANDARD",
        )
        res = analyzers.analyze_photon(cluster, None)
        assert len(res) == 0


# ====================================================================
# CONFIG-CLUSTER-005: Cluster Policy Compliance
# ====================================================================
class TestConfigCluster005Policy:
    def test_positive_missing_policy_id_triggers(self):
        cluster = DatabricksCluster(cluster_id="c1", cluster_name="no_policy")
        policy = ClusterPolicy(policy_id="corp-pol", name="Standard", definition={})
        res = analyzers.analyze_cluster_policy(cluster, policy)
        assert len(res) == 1
        assert "does not use required cluster policy" in res[0]["evidence"][0]

    def test_positive_policy_fixed_value_violation(self):
        cluster = DatabricksCluster(
            cluster_id="c2",
            cluster_name="wrong_timeout",
            policy_id="corp-pol",
            autotermination_minutes=60,
        )
        policy = ClusterPolicy(
            policy_id="corp-pol",
            name="Standard",
            definition={"autotermination_minutes": {"type": "fixed", "value": 20}},
        )
        res = analyzers.analyze_cluster_policy(cluster, policy)
        assert len(res) == 1
        assert "violates policy" in res[0]["evidence"][0]

    def test_negative_compliant_cluster_passes(self):
        cluster = DatabricksCluster(
            cluster_id="c3",
            cluster_name="compliant",
            policy_id="corp-pol",
            autotermination_minutes=20,
            spark_version="15.4.x-scala2.12",
        )
        policy = ClusterPolicy(
            policy_id="corp-pol",
            name="Standard",
            definition={
                "autotermination_minutes": {"type": "fixed", "value": 20},
                "spark_version": {"type": "fixed", "value": "15.4.x-scala2.12"},
            },
        )
        res = analyzers.analyze_cluster_policy(cluster, policy)
        assert len(res) == 0

    def test_edge_policy_range_constraint(self):
        cluster = DatabricksCluster(
            cluster_id="c4",
            cluster_name="range_test",
            policy_id="corp-pol",
            autotermination_minutes=120,
        )
        policy = ClusterPolicy(
            policy_id="corp-pol",
            name="Standard",
            definition={"autotermination_minutes": {"type": "range", "maxValue": 60}},
        )
        res = analyzers.analyze_cluster_policy(cluster, policy)
        assert len(res) == 1

    def test_edge_no_policy_provided_clean(self):
        cluster = DatabricksCluster(cluster_id="c5", cluster_name="plain")
        res = analyzers.analyze_cluster_policy(cluster, None)
        assert len(res) == 0


# ====================================================================
# CONFIG-CLUSTER-006: Auto-Termination
# ====================================================================
class TestConfigCluster006AutoTermination:
    def test_positive_all_purpose_zero_autotermination_triggers(self):
        cluster = DatabricksCluster(
            cluster_id="c1",
            cluster_name="interactive",
            autotermination_minutes=0,
            cluster_source="UI",
        )
        res = analyzers.analyze_autotermination(cluster, None, max_autotermination_minutes=60)
        assert len(res) == 1
        assert res[0]["triggered"] is True

    def test_negative_all_purpose_reasonable_autotermination_passes(self):
        cluster = DatabricksCluster(
            cluster_id="c2",
            cluster_name="interactive_good",
            autotermination_minutes=30,
            cluster_source="UI",
        )
        res = analyzers.analyze_autotermination(cluster, None, max_autotermination_minutes=60)
        assert len(res) == 0

    def test_negative_job_cluster_zero_autotermination_passes(self):
        # Job clusters terminate automatically when the job completes
        cluster = DatabricksCluster(
            cluster_id="c3",
            cluster_name="job_cluster",
            autotermination_minutes=0,
            cluster_source="JOB",
        )
        res = analyzers.analyze_autotermination(cluster, None, max_autotermination_minutes=60)
        assert len(res) == 0

    def test_positive_excessive_autotermination_triggers(self):
        cluster = DatabricksCluster(
            cluster_id="c4",
            cluster_name="long_idle",
            autotermination_minutes=180,
            cluster_source="UI",
        )
        res = analyzers.analyze_autotermination(cluster, None, max_autotermination_minutes=60)
        assert len(res) == 1
        assert "expected <= 60" in res[0]["evidence"][0]

    def test_param_override_max_autotermination(self):
        cluster = DatabricksCluster(
            cluster_id="c5",
            cluster_name="strict_idle",
            autotermination_minutes=45,
            cluster_source="UI",
        )
        res = analyzers.analyze_autotermination(cluster, None, max_autotermination_minutes=30)
        assert len(res) == 1
        assert res[0]["expected"]["max_autotermination_minutes"] == 30
