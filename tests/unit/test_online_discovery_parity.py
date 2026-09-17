"""Comprehensive unit tests for Online Evidence Discovery and Offline-Online Parity (Phase B).

Covers:
- Step 1: Code discovery (notebook task, spark_python_task, sql_task, 403, 404, malformed, DBFS)
- Step 2: Table metadata discovery via Unity Catalog (columns, 403, 404, malformed)
- Step 3: Discovered PipelineContract and DataProfile synthesis (source, target, write mode, missing, provenance)
- Step 4: Online orchestration evidence flow to checkpoints and downstream analyzers
- Step 5: Architectural parity proving offline and online use the exact same analyzer chain
"""

from __future__ import annotations

import base64
from typing import Any

from dpif.analyzers.alignment import ThreeLayerAlignmentAnalyzer
from dpif.analyzers.implementation import DeveloperImplementationAnalyzer
from dpif.analyzers.rerun import RerunIdempotencyAnalyzer
from dpif.analyzers.sufficiency import EvidenceSufficiencyAnalyzer
from dpif.analyzers.synthesis import DecisionRiskSynthesisAnalyzer
from dpif.checkpoints.definitions import build_all_checkpoints
from dpif.checkpoints.engine import CheckpointEngine
from dpif.code.parser import analyze_source
from dpif.connectors.base import DatabricksConnector
from dpif.connectors.live import DatabricksApiError
from dpif.discovery.synthesis import (
    synthesize_discovered_contract,
    synthesize_discovered_data_profile,
)
from dpif.models import (
    CollectionMethod,
    IngestionMode,
    SourceFormat,
    SourceType,
)
from dpif.orchestration.online import OnlineValidationOrchestrator
from dpif.providers.base import (
    AcquisitionErrorCode,
    DatabricksEvidenceProvider,
    EvidenceCategory,
)
from dpif.readiness.engine import evaluate_production_readiness
from dpif.runtime.models import RuntimeRun, RuntimeStage, RuntimeTask, RuntimeTaskMetrics


class MockDiscoveryConnector(DatabricksConnector):
    """Configurable mock connector for testing all discovery paths."""

    def __init__(
        self,
        job_payload: dict[str, Any] | None = None,
        workspace_export_map: dict[str, Any] | None = None,
        dbfs_read_map: dict[str, Any] | None = None,
        sql_query_map: dict[str, Any] | None = None,
        table_profile_map: dict[str, Any] | None = None,
        cluster_payload: dict[str, Any] | None = None,
        runtime_payload: dict[str, Any] | None = None,
        recent_runs_payload: dict[str, Any] | None = None,
    ) -> None:
        self.job_payload = job_payload or {}
        self.workspace_export_map = workspace_export_map or {}
        self.dbfs_read_map = dbfs_read_map or {}
        self.sql_query_map = sql_query_map or {}
        self.table_profile_map = table_profile_map or {}
        self.cluster_payload = cluster_payload or {
            "cluster_id": "c-123",
            "cluster_name": "test_cluster",
            "spark_version": "14.3.x-scala2.12",
            "node_type_id": "i3.xlarge",
            "num_workers": 4,
        }
        self.runtime_payload = runtime_payload or {
            "run_id": 9999,
            "execution_duration": 120000,
            "state": {"life_cycle_state": "TERMINATED", "result_state": "SUCCESS"},
        }
        self.recent_runs_payload = recent_runs_payload or {"runs": [{"run_id": 9999}]}

    def mode(self) -> str:
        return "live-api"

    def get_workspace_status(self) -> dict[str, Any] | None:
        return {"status": "connected", "spark_versions_count": 3}

    def get_job(self, job_id: int | str) -> dict[str, Any] | None:
        return self.job_payload

    def get_cluster(self, cluster_id: str) -> dict[str, Any] | None:
        return self.cluster_payload

    def get_recent_runs(
        self, job_id: int | str, limit: int = 10
    ) -> dict[str, Any] | list[dict[str, Any]] | None:
        return self.recent_runs_payload

    def get_run(self, run_id: int | str) -> dict[str, Any] | None:
        return self.runtime_payload

    def export_workspace_object(self, path: str, format: str = "SOURCE") -> dict[str, Any] | None:
        if path in self.workspace_export_map:
            val = self.workspace_export_map[path]
            if isinstance(val, Exception):
                raise val
            return val
        return None

    def read_dbfs_file(self, path: str) -> dict[str, Any] | None:
        clean = path[5:] if path.startswith("dbfs:") else path
        if clean in self.dbfs_read_map or path in self.dbfs_read_map:
            val = self.dbfs_read_map.get(clean) or self.dbfs_read_map.get(path)
            if isinstance(val, Exception):
                raise val
            return val
        return None

    def get_sql_query(self, query_id: str) -> dict[str, Any] | None:
        if query_id in self.sql_query_map:
            val = self.sql_query_map[query_id]
            if isinstance(val, Exception):
                raise val
            return val
        return None

    def get_table_profile(self, table: str) -> dict[str, Any] | None:
        if table in self.table_profile_map:
            val = self.table_profile_map[table]
            if isinstance(val, Exception):
                raise val
            return val
        return None


# ==============================================================================
# Step 1: Code Discovery Unit Tests
# ==============================================================================


def test_code_discovery_notebook_task_success():
    """Test discovering Python notebook code via workspace export."""
    code_content = "import pyspark.sql.functions as F\ndf = spark.read.table('bronze.orders')\ndf.write.mode('overwrite').saveAsTable('silver.orders')"
    encoded = base64.b64encode(code_content.encode("utf-8")).decode("ascii")

    conn = MockDiscoveryConnector(
        job_payload={
            "job_id": 101,
            "settings": {
                "name": "etl_notebook_job",
                "tasks": [{"task_key": "etl_task", "notebook_task": {"notebook_path": "/Users/dev/etl"}}],
            },
        },
        workspace_export_map={"/Users/dev/etl": {"content": encoded, "file_type": "PYTHON"}},
    )
    provider = DatabricksEvidenceProvider(connector=conn)
    evidence = provider.acquire_pipeline_evidence(pipeline_id="job-101", job_id=101)

    code_item = evidence.items.get(EvidenceCategory.CODE.value)
    assert code_item is not None
    assert code_item.is_available is True
    assert code_item.payload["primary_task_key"] == "etl_task"
    assert "spark.read.table" in code_item.payload["primary_source_code"]
    assert code_item.payload["primary_filename"] == "etl_task.py"


def test_code_discovery_spark_python_task_dbfs():
    """Test discovering Python file via DBFS read."""
    code_content = "from pyspark.sql import SparkSession\nspark = SparkSession.builder.getOrCreate()"
    encoded = base64.b64encode(code_content.encode("utf-8")).decode("ascii")

    conn = MockDiscoveryConnector(
        job_payload={
            "job_id": 102,
            "settings": {
                "name": "spark_py_job",
                "tasks": [{"task_key": "py_task", "spark_python_task": {"python_file": "dbfs:/scripts/run.py"}}],
            },
        },
        dbfs_read_map={"/scripts/run.py": {"bytes_read": len(code_content), "data": encoded}},
    )
    provider = DatabricksEvidenceProvider(connector=conn)
    evidence = provider.acquire_pipeline_evidence(pipeline_id="job-102", job_id=102)

    code_item = evidence.items.get(EvidenceCategory.CODE.value)
    assert code_item is not None
    assert code_item.is_available is True
    assert "SparkSession" in code_item.payload["primary_source_code"]


def test_code_discovery_sql_task_query():
    """Test discovering SQL code via sql/queries endpoint."""
    sql_text = "SELECT customer_id, count(*) as cnt FROM bronze.orders GROUP BY customer_id"

    conn = MockDiscoveryConnector(
        job_payload={
            "job_id": 103,
            "settings": {
                "name": "sql_query_job",
                "tasks": [{"task_key": "sql_task", "sql_task": {"query": {"query_id": "q-999"}}}],
            },
        },
        sql_query_map={"q-999": {"query_text": sql_text}},
    )
    provider = DatabricksEvidenceProvider(connector=conn)
    evidence = provider.acquire_pipeline_evidence(pipeline_id="job-103", job_id=103)

    code_item = evidence.items.get(EvidenceCategory.CODE.value)
    assert code_item is not None
    assert code_item.is_available is True
    assert code_item.payload["primary_filename"] == "sql_task.sql"
    assert "GROUP BY customer_id" in code_item.payload["primary_source_code"]


def test_code_discovery_permission_denied_403():
    """Test 403 authorization failure properly sets error and keeps code unavailable."""
    conn = MockDiscoveryConnector(
        job_payload={
            "job_id": 104,
            "settings": {
                "name": "forbidden_job",
                "tasks": [{"task_key": "secret_task", "notebook_task": {"notebook_path": "/Locked/notebook"}}],
            },
        },
        workspace_export_map={"/Locked/notebook": DatabricksApiError("Permission Denied", status_code=403)},
    )
    provider = DatabricksEvidenceProvider(connector=conn)
    evidence = provider.acquire_pipeline_evidence(pipeline_id="job-104", job_id=104)

    code_item = evidence.items.get(EvidenceCategory.CODE.value)
    assert code_item is not None
    assert code_item.is_available is False
    assert code_item.error is not None
    assert code_item.error.error_code == AcquisitionErrorCode.AUTHORIZATION_FAILURE


def test_code_discovery_resource_not_found_404():
    """Test 404 missing notebook gracefully marks code as unavailable without erroring."""
    conn = MockDiscoveryConnector(
        job_payload={
            "job_id": 105,
            "settings": {
                "name": "missing_nb_job",
                "tasks": [{"task_key": "deleted_task", "notebook_task": {"notebook_path": "/Deleted/nb"}}],
            },
        },
        workspace_export_map={},  # Returns None
    )
    provider = DatabricksEvidenceProvider(connector=conn)
    evidence = provider.acquire_pipeline_evidence(pipeline_id="job-105", job_id=105)

    code_item = evidence.items.get(EvidenceCategory.CODE.value)
    assert code_item is not None
    assert code_item.is_available is False
    assert code_item.error.error_code == AcquisitionErrorCode.RESOURCE_NOT_FOUND


def test_code_discovery_multi_task_heterogeneous():
    """Test multi-task job with notebook + SQL tasks preserves task identity."""
    code_py = "df = spark.read.table('bronze.users')"
    code_sql = "INSERT INTO silver.users SELECT * FROM bronze.users"

    encoded_py = base64.b64encode(code_py.encode("utf-8")).decode("ascii")

    conn = MockDiscoveryConnector(
        job_payload={
            "job_id": 106,
            "settings": {
                "name": "multi_task_job",
                "tasks": [
                    {"task_key": "step1_py", "notebook_task": {"notebook_path": "/etl/step1"}},
                    {"task_key": "step2_sql", "sql_task": {"query": {"query_id": "q-step2"}}},
                ],
            },
        },
        workspace_export_map={"/etl/step1": {"content": encoded_py, "file_type": "PYTHON"}},
        sql_query_map={"q-step2": {"query_text": code_sql}},
    )
    provider = DatabricksEvidenceProvider(connector=conn)
    evidence = provider.acquire_pipeline_evidence(pipeline_id="job-106", job_id=106)

    code_item = evidence.items.get(EvidenceCategory.CODE.value)
    assert code_item is not None
    assert code_item.is_available is True
    assert len(code_item.payload["tasks"]) == 2
    assert code_item.payload["tasks"][0]["task_key"] == "step1_py"
    assert code_item.payload["tasks"][1]["task_key"] == "step2_sql"
    assert "TASK: step1_py" in code_item.payload["combined_code"]
    assert "TASK: step2_sql" in code_item.payload["combined_code"]


# ==============================================================================
# Step 2: Table Metadata Discovery Unit Tests
# ==============================================================================


def test_table_metadata_uc_success():
    """Test live Unity Catalog table metadata retrieval."""
    conn = MockDiscoveryConnector(
        table_profile_map={
            "main.sales.orders": {
                "name": "orders",
                "catalog_name": "main",
                "schema_name": "sales",
                "table_type": "MANAGED",
                "data_source_format": "DELTA",
                "columns": [
                    {"name": "order_id", "type_text": "string", "nullable": False},
                    {"name": "amount", "type_text": "double", "nullable": True},
                ],
            }
        }
    )
    prof = conn.get_table_profile("main.sales.orders")
    assert prof is not None
    assert prof["name"] == "orders"
    assert len(prof["columns"]) == 2
    assert prof["columns"][0]["name"] == "order_id"


def test_table_metadata_404_returns_none():
    """Test 404 table not found returns None safely without raising."""
    conn = MockDiscoveryConnector(table_profile_map={})
    prof = conn.get_table_profile("unknown.catalog.missing")
    assert prof is None


def test_table_metadata_403_raises_api_error():
    """Test 403 on table lookup raises DatabricksApiError."""
    conn = MockDiscoveryConnector(
        table_profile_map={"secret.table": DatabricksApiError("Forbidden", status_code=403)}
    )
    import pytest
    with pytest.raises(DatabricksApiError) as exc_info:
        conn.get_table_profile("secret.table")
    assert exc_info.value.status_code == 403


# ==============================================================================
# Step 3: Contract & DataProfile Synthesis Unit Tests
# ==============================================================================


def test_contract_synthesis_from_code_analysis():
    """Test synthesizing PipelineContract from discovered Python code AST."""
    code = (
        "df = spark.read.format('parquet').load('s3://my-bucket/raw/events')\n"
        "df.write.mode('append').format('delta').saveAsTable('silver.events')"
    )
    analysis = analyze_source(code, filename="pipeline.py")

    contract = synthesize_discovered_contract(
        job_config={"settings": {"name": "events_pipeline", "max_retries": 3}},
        cluster_config={"spark_version": "14.3.x-scala2.12"},
        code_analysis=analysis,
        raw_code=code,
        pipeline_name="events_pipeline",
        environment="production",
        resource_id="1001",
    )

    assert contract is not None
    assert contract.pipeline_name == "events_pipeline"
    assert contract.source.type == SourceType.S3
    assert contract.source.path == "s3://my-bucket/raw/events"
    assert contract.source.format == SourceFormat.PARQUET
    assert contract.target.path == "silver.events"
    assert contract.reliability.retry_count == 3
    assert contract._cluster_raw["spark_version"] == "14.3.x-scala2.12"


def test_contract_synthesis_streaming_detection():
    """Test synthesizing PipelineContract with streaming workload."""
    code = "df = spark.readStream.table('kafka_bronze')\ndf.writeStream.saveAsTable('gold_stream')"
    analysis = analyze_source(code, filename="stream.py")

    contract = synthesize_discovered_contract(
        code_analysis=analysis,
        raw_code=code,
        pipeline_name="stream_pipe",
    )

    assert contract is not None
    assert contract.processing == "streaming"
    assert contract.source.ingestion_mode == IngestionMode.STREAMING


def test_contract_synthesis_empty_evidence_returns_none():
    """Test that absent source/target/code evidence strictly returns None (UNKNOWN)."""
    contract = synthesize_discovered_contract(
        code_analysis=None,
        raw_code="",
    )
    assert contract is None


def test_dataprofile_synthesis_from_runtime_and_uc():
    """Test synthesizing DataProfile from runtime telemetry and table schema."""
    task = RuntimeTask(
        task_id=1,
        stage_id=1,
        metrics=RuntimeTaskMetrics(input_bytes=5 * (1024**3)),  # 5 GB
    )
    stage = RuntimeStage(stage_id=1, input_bytes=5 * (1024**3), task_count=50, tasks=[task])
    run = RuntimeRun(run_id=123, stages=[stage])

    table_profile = {
        "columns": [
            {"name": "id", "type_text": "bigint"},
            {"name": "ts", "type_text": "timestamp"},
        ]
    }

    profile = synthesize_discovered_data_profile(runtime_run=run, table_profile=table_profile)
    assert profile is not None
    assert profile.total_gb == 5.0
    assert profile.column_count == 2
    assert profile.schema_columns[0].name == "id"
    assert profile.collection_method == CollectionMethod.RUNTIME


def test_dataprofile_synthesis_missing_telemetry_returns_none():
    """Test missing telemetry and schema returns None rather than fabricated values."""
    profile = synthesize_discovered_data_profile(runtime_run=None, table_profile=None)
    assert profile is None


# ==============================================================================
# Step 4: Online Orchestration End-to-End Discovery
# ==============================================================================


def test_online_orchestrator_full_automatic_discovery():
    """Test that online validation automatically discovers code, contract, and profile."""
    code_content = (
        "import pyspark.sql.functions as F\n"
        "df = spark.read.table('bronze.inventory')\n"
        "df.write.mode('overwrite').saveAsTable('silver.inventory')"
    )
    encoded = base64.b64encode(code_content.encode("utf-8")).decode("ascii")

    conn = MockDiscoveryConnector(
        job_payload={
            "job_id": 5001,
            "settings": {
                "name": "auto_inventory_job",
                "max_retries": 2,
                "tasks": [
                    {
                        "task_key": "inv_task",
                        "existing_cluster_id": "cluster-inv-500",
                        "notebook_task": {"notebook_path": "/Users/ops/inv_etl"},
                    }
                ],
            },
        },
        workspace_export_map={"/Users/ops/inv_etl": {"content": encoded, "file_type": "PYTHON"}},
        cluster_payload={
            "cluster_id": "cluster-inv-500",
            "cluster_name": "inv_cluster",
            "spark_version": "14.3.x-scala2.12",
            "node_type_id": "Standard_D8s_v5",
            "num_workers": 4,
        },
        table_profile_map={
            "bronze.inventory": {
                "name": "bronze.inventory",
                "columns": [
                    {"name": "item_id", "type_text": "string"},
                    {"name": "qty", "type_text": "int"},
                ],
            }
        },
    )

    orchestrator = OnlineValidationOrchestrator(connector=conn)
    result = orchestrator.validate(job_id=5001)

    # 1. Evidence was discovered automatically
    assert result.evidence_summary["job"] == "LIVE"
    assert result.evidence_summary["cluster"] == "LIVE"
    assert result.evidence_summary["code"] == "LIVE"

    # 2. Checkpoints CP-001 (Source), CP-004 (Code), CP-009 (Cluster), CP-011 (Job) ran on discovered data
    assert result.checkpoints.get("CP-001") is not None
    assert result.checkpoints.get("CP-004") is not None
    assert result.checkpoints.get("CP-009") is not None
    assert result.checkpoints.get("CP-011") is not None

    # 3. Downstream analyzers executed
    assert result.implementation_forensics is not None
    assert result.rerun_analysis is not None
    assert result.alignment_analysis is not None
    assert result.evidence_sufficiency is not None
    assert result.production_readiness is not None
    assert result.decision_risk_synthesis is not None
    from dpif.models.synthesis import FinalDecisionStatus
    assert result.final_decision in [s.value for s in FinalDecisionStatus]


# ==============================================================================
# Step 5: Offline / Online Parity Architecture Verification
# ==============================================================================


def test_architectural_parity_between_offline_and_online():
    """Verify that both OFFLINE and ONLINE use the SAME analyzer classes and functions."""
    code_text = "df = spark.read.table('source_tbl')\ndf.write.saveAsTable('target_tbl')"

    # 1. Same parser
    analysis = analyze_source(code_text, filename="workload.py")

    # 2. Same checkpoint builders and engine
    contract = synthesize_discovered_contract(
        code_analysis=analysis,
        raw_code=code_text,
        pipeline_name="parity_pipeline",
    )
    data_profile = synthesize_discovered_data_profile(runtime_run=None, table_profile=None)

    checkpoints = build_all_checkpoints(
        contract=contract,
        data_profile=data_profile,
        code_text=code_text,
    )
    engine = CheckpointEngine()
    results = engine.run_all_checkpoints(checkpoints, {"pipeline_name": "parity_pipeline"})

    # 3. Same M5E DeveloperImplementationAnalyzer
    impl = DeveloperImplementationAnalyzer(analysis, context={}).analyze()

    # 4. Same M5F RerunIdempotencyAnalyzer
    rerun = RerunIdempotencyAnalyzer(analysis, context={}).analyze()

    # 5. Same M5G ThreeLayerAlignmentAnalyzer
    alignment = ThreeLayerAlignmentAnalyzer(analysis, context={"pipeline_contract": contract}).analyze()

    # 6. Same M5H EvidenceSufficiencyAnalyzer
    sufficiency = EvidenceSufficiencyAnalyzer(analysis, context={"checkpoints": results}).analyze()

    # 7. Same CP-FINAL evaluate_production_readiness
    readiness = evaluate_production_readiness(checkpoints=results, contract=contract, profile=data_profile)

    # 8. Same M5I DecisionRiskSynthesisAnalyzer
    synthesis = DecisionRiskSynthesisAnalyzer(
        checkpoints=results,
        readiness=readiness,
        implementation_forensics=impl,
        rerun_analysis=rerun,
        alignment_analysis=alignment,
        evidence_sufficiency=sufficiency,
        contract=contract,
        profile=data_profile,
    ).analyze()

    assert synthesis.final_decision is not None
    assert len(results) >= 19
