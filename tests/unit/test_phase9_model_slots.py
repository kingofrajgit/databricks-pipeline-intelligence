"""P9-1 focused tests: additive Phase 9 model slots.

Scope is model slots ONLY (no attribution, no inference):
- every new field defaults to None (UNKNOWN), never numeric zero;
- explicit values round-trip through serialization;
- existing construction without the new fields keeps working;
- provider-generated synthetic entries carry the authoritative
  parent_task_key; all other entries carry None (no inference).
"""

from __future__ import annotations

import base64

from dpif.code.models import Operation, OperationType
from dpif.flow.models import (
    FlowNode,
    FlowNodeKind,
    OperationRuntimeCorrelation,
    VolumeObservation,
)
from dpif.models import DataProfile
from dpif.providers.base import DatabricksEvidenceProvider, EvidenceCategory
from dpif.runtime.models import (
    QueryHistoryEntry,
    RuntimeStage,
    RuntimeTask,
    RuntimeTaskMetrics,
)
from tests.unit.test_online_discovery_parity import MockDiscoveryConnector


def _b64(text: str) -> str:
    return base64.b64encode(text.encode("utf-8")).decode("ascii")


# 1. New fields default to None.
def test_operation_task_key_defaults_none():
    op = Operation(operation_type=OperationType.READ, line=3)
    assert op.task_key is None


def test_flow_node_task_key_defaults_none():
    node = FlowNode(node_id="src:3:0", kind=FlowNodeKind.SOURCE)
    assert node.task_key is None
    assert node.volume is None


def test_runtime_lineage_slots_default_empty_or_none():
    stage = RuntimeStage(stage_id=1)
    assert stage.tables == []
    assert stage.query_ids == []
    task = RuntimeTask(task_id=1, stage_id=1)
    assert task.query_id is None
    entry = QueryHistoryEntry(query_id="q-1")
    assert entry.read_bytes is None
    assert entry.written_bytes is None
    assert entry.rows is None


def test_data_profile_source_name_defaults_none():
    assert DataProfile().source_name is None


def test_correlation_volume_defaults_none():
    assert OperationRuntimeCorrelation(operation_node_id="sql:query:0").volume is None


# 6. Volume-related fields never default to numeric zero.
def test_no_numeric_zero_defaults():
    assert VolumeObservation().is_known is False
    entry = QueryHistoryEntry(query_id="q-1")
    assert entry.read_bytes is None
    assert entry.written_bytes is None
    assert entry.rows is None
    assert DataProfile().total_bytes == 0  # pre-existing genuine-zero default, unchanged
    assert DataProfile().source_name is None


# 2. Explicit values serialize/deserialize correctly.
def test_explicit_values_round_trip():
    op = Operation(operation_type=OperationType.READ, line=3, task_key="etl")
    assert Operation.model_validate(op.to_dict()).task_key == "etl"
    node = FlowNode(node_id="src:3:0", kind=FlowNodeKind.SOURCE, task_key="etl")
    assert FlowNode.model_validate(node.model_dump(mode="json")).task_key == "etl"
    vol = VolumeObservation(input_bytes=10)
    corr = OperationRuntimeCorrelation(operation_node_id="sql:query:0", volume=vol)
    assert OperationRuntimeCorrelation.model_validate(corr.model_dump(mode="json")).volume is not None
    stage = RuntimeStage(stage_id=1, tables=["a.t"], query_ids=["q-1"])
    assert RuntimeStage.model_validate(stage.model_dump(mode="json")).tables == ["a.t"]
    task = RuntimeTask(task_id=1, stage_id=1, query_id="q-1")
    assert RuntimeTask.model_validate(task.model_dump(mode="json")).query_id == "q-1"
    entry = QueryHistoryEntry(query_id="q-1", read_bytes=100, written_bytes=50, rows=10)
    assert QueryHistoryEntry.model_validate(entry.model_dump(mode="json")).rows == 10
    profile = DataProfile(source_name="a.t")
    assert DataProfile.model_validate(profile.to_dict()).source_name == "a.t"
    assert profile.to_dict()["source_name"] == "a.t"


# 3/4/7. Existing construction stays compatible (no new required fields,
# existing consumers unbroken: metrics default, task without metrics, graph
# node without task_key all still validate).
def test_existing_construction_compatible():
    RuntimeTaskMetrics()
    RuntimeTask(task_id="t-1", stage_id=2)
    RuntimeStage(stage_id=2, tasks=[RuntimeTask(task_id="t-1", stage_id=2)])
    FlowNode(node_id="op:read:1:0", kind=FlowNodeKind.OPERATION)
    Operation(operation_type=OperationType.JOIN, line=9)
    DataProfile(total_bytes=100, file_count=2)


# 5a. Provider-generated synthetic entry carries the authoritative parent.
def test_synthetic_entry_carries_parent_task_key():
    conn = MockDiscoveryConnector(
        job_payload={
            "job_id": 9101,
            "settings": {
                "tasks": [
                    {"task_key": "refresh", "pipeline_task": {"pipeline_id": "pipe-1"}},
                ]
            },
        },
        workspace_export_map={
            "/pipe/lib": {"content": _b64("df = spark.read.table('a.t')"), "file_type": "PYTHON"},
        },
    )

    def _get_pipeline(pid: str):
        assert pid == "pipe-1"
        return {"spec": {"libraries": [{"notebook": {"path": "/pipe/lib"}}]}}

    conn.get_pipeline = _get_pipeline  # type: ignore[method-assign]
    provider = DatabricksEvidenceProvider(connector=conn)
    evidence = provider.acquire_pipeline_evidence(pipeline_id="job-9101", job_id=9101)
    payload = evidence.items.get(EvidenceCategory.CODE.value).payload
    assert payload is not None
    synthetic = [t for t in payload["tasks"] if t["task_key"].startswith("refresh_")]
    assert len(synthetic) == 1
    assert synthetic[0]["parent_task_key"] == "refresh"


# 5b. Direct-pipeline fallback entries and real tasks carry None (no inference).
def test_fallback_and_real_entries_parent_none():
    conn = MockDiscoveryConnector(
        job_payload={
            "job_id": 9102,
            "settings": {
                "tasks": [
                    {"task_key": "etl", "notebook_task": {"notebook_path": "/etl/nb"}},
                ]
            },
        },
        workspace_export_map={
            "/etl/nb": {"content": _b64("x = 1"), "file_type": "PYTHON"},
        },
    )
    provider = DatabricksEvidenceProvider(connector=conn)
    evidence = provider.acquire_pipeline_evidence(pipeline_id="job-9102", job_id=9102)
    payload = evidence.items.get(EvidenceCategory.CODE.value).payload
    assert payload is not None
    assert payload["tasks"][0].get("parent_task_key") is None
