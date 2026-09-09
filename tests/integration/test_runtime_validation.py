"""Integration tests for Phase 7 Runtime Performance Intelligence.

Validates:
- CheckpointEngine execution with CP-008 and CP-023
- Offline fixture connector integration
- Strict UNKNOWN semantics when runtime evidence is missing
- Checkpoint status updates and scoring behavior
"""

from __future__ import annotations

import json
from pathlib import Path

from dpif.checkpoints.definitions import build_all_checkpoints
from dpif.checkpoints.engine import CheckpointEngine
from dpif.connectors.offline import OfflineDatabricksConnector
from dpif.contract.loader import load_contract_file
from dpif.models import CheckpointStatus
from dpif.runtime.models import RuntimeRun
from dpif.runtime.normalization import normalize_runtime_payload
from dpif.scoring.engine import score_checkpoints


def _load_fixture_run(name: str) -> RuntimeRun:
    p = Path(f"tests/fixtures/runtime/{name}")
    raw = json.loads(p.read_text(encoding="utf-8"))
    return normalize_runtime_payload(raw)


def test_validation_without_runtime_data():
    contract = load_contract_file("examples/customer_daily.yaml")
    engine = CheckpointEngine()
    checkpoints = build_all_checkpoints(contract=contract, runtime_data=None)

    results = engine.run_all_checkpoints(checkpoints, {"pipeline_contract": contract})

    cp008 = results.get("CP-008")
    assert cp008 is not None
    # Strictly UNKNOWN without runtime data (never fake PASS)
    assert cp008.status == CheckpointStatus.UNKNOWN
    assert cp008.evidence.confidence == 0.0

    cp023 = results.get("CP-023")
    assert cp023 is not None
    assert cp023.status == CheckpointStatus.UNKNOWN


def test_validation_with_healthy_run():
    contract = load_contract_file("examples/customer_daily.yaml")
    engine = CheckpointEngine()
    healthy_run = _load_fixture_run("healthy_run.json")

    checkpoints = build_all_checkpoints(contract=contract, runtime_data=healthy_run)
    context = {
        "pipeline_contract": contract,
        "runtime_run": healthy_run,
        "runtime_data": healthy_run,
    }
    results = engine.run_all_checkpoints(checkpoints, context)

    cp008 = results.get("CP-008")
    assert cp008 is not None
    assert cp008.status == CheckpointStatus.PASS
    assert len(cp008.findings) == 0

    score, _ = score_checkpoints(results)
    assert score.categories.get("performance") == 100.0


def test_validation_with_high_shuffle_run():
    contract = load_contract_file("examples/customer_daily.yaml")
    engine = CheckpointEngine()
    high_shuffle = _load_fixture_run("high_shuffle_run.json")

    checkpoints = build_all_checkpoints(contract=contract, runtime_data=high_shuffle)
    context = {
        "pipeline_contract": contract,
        "runtime_run": high_shuffle,
        "runtime_data": high_shuffle,
    }
    results = engine.run_all_checkpoints(checkpoints, context)

    cp008 = results.get("CP-008")
    assert cp008 is not None
    assert cp008.status in (CheckpointStatus.WARN, CheckpointStatus.FAIL)
    # Confirms findings generated
    rule_ids = [f.rule_id for f in cp008.findings]
    assert "RUNTIME-PERF-002" in rule_ids or "RUNTIME-PERF-003" in rule_ids


def test_validation_with_memory_spill_run():
    contract = load_contract_file("examples/customer_daily.yaml")
    engine = CheckpointEngine()
    spill_run = _load_fixture_run("memory_spill_run.json")

    checkpoints = build_all_checkpoints(contract=contract, runtime_data=spill_run)
    context = {
        "pipeline_contract": contract,
        "runtime_run": spill_run,
        "runtime_data": spill_run,
    }
    results = engine.run_all_checkpoints(checkpoints, context)

    cp008 = results.get("CP-008")
    assert cp008 is not None
    assert cp008.status == CheckpointStatus.FAIL
    rule_ids = [f.rule_id for f in cp008.findings]
    assert "RUNTIME-PERF-006" in rule_ids


def test_cp023_sla_exceeded():
    contract = load_contract_file("examples/customer_daily.yaml")
    engine = CheckpointEngine()
    # Contract expected_duration_minutes is typically ~60m. Simulate 4 hours run (14400s)
    long_run = RuntimeRun(run_id="slow_1", duration_seconds=14400.0)

    checkpoints = build_all_checkpoints(contract=contract, runtime_data=long_run)
    context = {
        "pipeline_contract": contract,
        "runtime_run": long_run,
        "runtime_data": long_run,
    }
    results = engine.run_all_checkpoints(checkpoints, context)

    cp023 = results.get("CP-023")
    assert cp023 is not None
    # When SLA is exceeded, CP-023 must FAIL
    assert cp023.status == CheckpointStatus.FAIL


def test_offline_connector_runtime_methods():
    connector = OfflineDatabricksConnector()
    assert connector.mode() == "offline-fixture"

    # Fetch runtime run
    run_data = connector.get_runtime_run("healthy_run")
    assert run_data is not None
    assert run_data["_connector"] == "offline-fixture"
    assert run_data["evidence_source"] == "FIXTURE"

    # Fetch via get_run alias
    alias_data = connector.get_run("healthy_run")
    assert alias_data is not None
    assert alias_data["run_id"] == run_data["run_id"]
