"""Unit tests for Phase 6 Databricks Domain Models."""

from __future__ import annotations

from dpif.discovery.models import (
    AccessControlEntry,
    AutoscalingConfig,
    ClusterPolicy,
    DatabricksCluster,
    DatabricksJob,
    DatabricksPipeline,
    DatabricksTask,
    DatabricksWorkspace,
    JobSchedule,
    PermissionConfiguration,
    TaskDependency,
)


class TestDatabricksModels:
    def test_databricks_workspace_model(self):
        ws = DatabricksWorkspace(
            workspace_id="1234567890123456",
            workspace_url="https://adb-1234567890123456.7.azuredatabricks.net",
            cloud_provider="azure",
            region="eastus2",
            pricing_tier="premium",
        )
        assert ws.workspace_id == "1234567890123456"
        assert ws.cloud_provider == "azure"
        assert ws.pricing_tier == "premium"
        assert ws.tags == {}

    def test_cluster_autoscaling_and_workers(self):
        # Cluster with autoscaling
        auto_cluster = DatabricksCluster(
            cluster_id="cluster-001",
            cluster_name="prod-etl-cluster",
            spark_version="15.4.x-scala2.12",
            node_type_id="Standard_D8ds_v5",
            autoscale=AutoscalingConfig(min_workers=2, max_workers=8),
            autotermination_minutes=30,
            runtime_engine="PHOTON",
        )
        assert auto_cluster.has_autoscaling is True
        assert auto_cluster.effective_workers == 8
        assert auto_cluster.is_photon is True
        assert auto_cluster.dbr_major_version == "15.4"

        # Fixed cluster
        fixed_cluster = DatabricksCluster(
            cluster_id="cluster-002",
            cluster_name="fixed-cluster",
            spark_version="14.3.x-scala2.12",
            node_type_id="i3.xlarge",
            num_workers=4,
            autotermination_minutes=0,
            runtime_engine="STANDARD",
        )
        assert fixed_cluster.has_autoscaling is False
        assert fixed_cluster.effective_workers == 4
        assert fixed_cluster.is_photon is False
        assert fixed_cluster.dbr_major_version == "14.3"

    def test_cluster_policy_model(self):
        policy = ClusterPolicy(
            policy_id="policy-abc-123",
            name="Restricted Compute Policy",
            definition={
                "autotermination_minutes": {"type": "fixed", "value": 30},
                "spark_version": {"type": "fixed", "value": "15.4.x-scala2.12"},
            },
        )
        assert policy.policy_id == "policy-abc-123"
        assert policy.name == "Restricted Compute Policy"
        assert policy.definition["autotermination_minutes"]["value"] == 30

    def test_job_and_tasks_model(self):
        job = DatabricksJob(
            job_id=987654,
            name="daily_sales_pipeline",
            schedule=JobSchedule(
                quartz_cron_expression="0 0 2 * * ?",
                timezone_id="UTC",
                pause_status="UNPAUSED",
            ),
            max_concurrent_runs=1,
            timeout_seconds=3600,
            tasks=[
                DatabricksTask(
                    task_key="extract_step",
                    description="Extract raw data",
                    notebook_path="/Shared/ETL/extract",
                    max_retries=2,
                    min_retry_interval_millis=30000,
                    timeout_seconds=1800,
                ),
                DatabricksTask(
                    task_key="transform_step",
                    description="Run PySpark transform",
                    spark_python_task_file=(
                        "abfss://curated@acct.dfs.core.windows.net/scripts/transform.py"
                    ),
                    depends_on=[TaskDependency(task_key="extract_step")],
                    max_retries=1,
                    timeout_seconds=1800,
                ),
            ],
        )
        assert job.job_id == 987654
        assert job.is_paused is False
        assert len(job.tasks) == 2
        assert job.task_keys == {"extract_step", "transform_step"}
        assert job.tasks[0].source_path == "/Shared/ETL/extract"
        assert (
            job.tasks[1].source_path
            == "abfss://curated@acct.dfs.core.windows.net/scripts/transform.py"
        )

    def test_job_paused_property(self):
        paused_job = DatabricksJob(
            job_id=111,
            name="paused_job",
            schedule=JobSchedule(
                quartz_cron_expression="0 0 * * * ?",
                pause_status="PAUSED",
            ),
        )
        assert paused_job.is_paused is True

        no_sched_job = DatabricksJob(job_id=222, name="ad_hoc_job")
        assert no_sched_job.is_paused is False

    def test_dlt_pipeline_model(self):
        pipeline = DatabricksPipeline(
            pipeline_id="dlt-abc-789",
            name="orders_dlt_pipeline",
            storage="abfss://dlt@storage.dfs.core.windows.net/orders/",
            edition="ADVANCED",
            channel="CURRENT",
            continuous=False,
            development=False,
            photon=True,
            target="gold_orders",
        )
        assert pipeline.pipeline_id == "dlt-abc-789"
        assert pipeline.edition == "ADVANCED"
        assert pipeline.photon is True
        assert pipeline.development is False

    def test_permission_configuration_model(self):
        perm = PermissionConfiguration(
            object_id="job/987654",
            object_type="job",
            access_control_list=[
                AccessControlEntry(
                    user_name="data-eng-lead@company.com", permission_level="CAN_MANAGE"
                ),
                AccessControlEntry(group_name="data_engineers", permission_level="CAN_RUN"),
                AccessControlEntry(
                    service_principal_name="sp-dpif-runner", permission_level="CAN_MANAGE"
                ),
            ],
        )
        assert perm.object_id == "job/987654"
        assert len(perm.access_control_list) == 3
        assert perm.access_control_list[0].user_name == "data-eng-lead@company.com"
        assert perm.access_control_list[1].group_name == "data_engineers"
        assert perm.access_control_list[2].service_principal_name == "sp-dpif-runner"
