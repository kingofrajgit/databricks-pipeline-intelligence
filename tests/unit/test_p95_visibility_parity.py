"""P9-5 focused tests: attribution visibility, parity, and decision stability.

UNKNOWN volume attribution is NOT PASS, NOT FAIL, and NOT a blocker.
Coverage is exposed through existing M5H/M5I mechanics with zero policy,
scoring, readiness, or checkpoint changes. Offline and online paths agree
on attribution counts, UNKNOWN counts, nature, and sufficiency
interpretation for equivalent evidence.
"""

from __future__ import annotations

import base64
import json
from typing import Any

from click.testing import CliRunner

from dpif.analyzers.sufficiency import EvidenceSufficiencyAnalyzer
from dpif.analyzers.synthesis import DecisionRiskSynthesisAnalyzer
from dpif.checkpoints.definitions import build_all_checkpoints
from dpif.checkpoints.engine import CheckpointEngine
from dpif.code.parser import analyze_source
from dpif.flow.correlation import normalize_query_history
from dpif.models.sufficiency import EvidenceSufficiencyAssessment
from dpif.orchestration.online import OnlineValidationOrchestrator
from tests.unit.test_decision_synthesis import (
    _make_clean_checkpoints,
    _make_dummy_readiness,
    _make_dummy_sufficiency,
)
from tests.unit.test_online_discovery_parity import MockDiscoveryConnector


def _b64(text: str) -> str:
    return base64.b64encode(text.encode("utf-8")).decode("ascii")


def _sufficiency(context_extra: dict[str, Any] | None = None):
    analysis = analyze_source("df = spark.read.table('a.t')", filename="etl.py")
    context: dict[str, Any] = dict(context_extra or {})
    return EvidenceSufficiencyAnalyzer(analysis, context=context).analyze()


# D. Attribution coverage is exposed through M5H (counts only, no policy).
def test_m5h_exposes_volume_attribution_counts():
    summary = {"attributed": {"DATA_PROFILE": 1}, "unknown": 2, "total_sources": 3}
    assessment = _sufficiency({"volume_attribution": summary})
    assert assessment.volume_attribution == summary
    assert assessment.to_dict()["volume_attribution"] == summary


def test_m5h_defaults_empty_attribution():
    assessment = _sufficiency()
    assert assessment.volume_attribution == {
        "attributed": {},
        "unknown": 0,
        "total_sources": 0,
    }
    assert assessment.to_dict()["volume_attribution"] == assessment.volume_attribution


# D (control). Exposure never changes sufficiency outcomes by itself.
def test_m5h_exposure_does_not_change_sufficiency():
    summary = {"attributed": {"DATA_PROFILE": 1}, "unknown": 2, "total_sources": 3}
    without = _sufficiency()
    with_counts = _sufficiency({"volume_attribution": summary})
    assert without.overall_decision_sufficiency == with_counts.overall_decision_sufficiency
    assert without.overall_confidence == with_counts.overall_confidence
    assert without.coverage_score == with_counts.coverage_score
    assert len(without.decisions) == len(with_counts.decisions)


# A/B/C. UNKNOWN volume creates no blocker, no FAIL, no PASS in M5I.
def test_m5i_unknown_volume_creates_nothing():
    suff = _sufficiency(
        {"volume_attribution": {"attributed": {}, "unknown": 3, "total_sources": 3}}
    )
    analyzer = DecisionRiskSynthesisAnalyzer(
        checkpoints=_make_clean_checkpoints(),
        readiness=_make_dummy_readiness(),
        evidence_sufficiency=suff,
    )
    result = analyzer.analyze()
    assert result.blockers == []
    # No attribution-driven risk content: identical risk set with and
    # without the volume_attribution key on the SAME sufficiency input.
    charlie = DecisionRiskSynthesisAnalyzer(
        checkpoints=_make_clean_checkpoints(),
        readiness=_make_dummy_readiness(),
        evidence_sufficiency=_sufficiency(),
    ).analyze()
    assert [r.risk_id for r in result.top_risks] == [
        r.risk_id for r in charlie.top_risks
    ]
    assert result.final_decision == charlie.final_decision
    assert result.decision_sufficiency == charlie.decision_sufficiency


# E. M5I manufactures no new blocker for unattributed volumes.
def test_m5i_no_new_blocker_ids():
    suff = _sufficiency(
        {"volume_attribution": {"attributed": {}, "unknown": 5, "total_sources": 5}}
    )
    result = DecisionRiskSynthesisAnalyzer(
        checkpoints=_make_clean_checkpoints(),
        readiness=_make_dummy_readiness(),
        evidence_sufficiency=suff,
    ).analyze()
    assert result.blockers == []
    assert all(r.priority != "P0" for r in result.remediations)


# F. CP-FINAL behavior unchanged by attribution presence.
def test_cp_final_unchanged_by_attribution():
    from dpif.readiness.engine import evaluate_production_readiness

    cps = build_all_checkpoints(code_text="df = spark.read.table('a.t')")
    engine = CheckpointEngine()
    base = engine.run_all_checkpoints(
        [cp.model_copy(deep=True) for cp in cps],
        {"pipeline_name": "p", "code_snippet": "df = spark.read.table('a.t')"},
    )
    ward = engine.run_all_checkpoints(
        [cp.model_copy(deep=True) for cp in cps],
        {
            "pipeline_name": "p",
            "code_snippet": "df = spark.read.table('a.t')",
            "volume_attribution": {"attributed": {}, "unknown": 1, "total_sources": 1},
        },
    )
    assert {k: v.status for k, v in base.items()} == {
        k: v.status for k, v in ward.items()
    }
    assert (
        evaluate_production_readiness(checkpoints=base).status
        == evaluate_production_readiness(checkpoints=ward).status
    )


# G. Phase 7 confidence/blocker gate unchanged.
def test_phase7_gate_unchanged():
    from dpif.readiness.models import CrossDomainRisk

    high = CrossDomainRisk(
        risk_id="XDOM-003",
        title="High Network Shuffle & Spill Risk",
        severity="HIGH",
        contributing_domains=["Performance", "Scalability"],
        evidence_sources=["Runtime Telemetry"],
        description="Observed shuffle volume exceeds safe network thresholds.",
        recommendation="Enable AQE.",
        confidence=0.85,
    )
    low = high.model_copy(update={"confidence": 0.0})
    for xr, expect_block in ((high, True), (low, False)):
        readiness = _make_dummy_readiness()
        readiness.cross_domain_risks = [xr]
        result = DecisionRiskSynthesisAnalyzer(
            checkpoints=_make_clean_checkpoints(),
            readiness=readiness,
            evidence_sufficiency=_make_dummy_sufficiency(),
        ).analyze()
        assert any(b.source == "XDOM-003" for b in result.blockers) is expect_block


# H. Offline/online parity on equivalent evidence.
#
# NOTE (pre-existing boundary, not P9-5 scope): provider-combined code
# always carries `# --- TASK:` headers, and the SQL parser yields no queries
# for header-prefixed blobs. Live notebook SQL therefore reaches correlation
# differently from bare offline SQL files. Parity is proven two ways: (1) a
# live-code-path scenario (parquet reads, UNKNOWN attribution both sides);
# (2) a measured-SQL scenario via identical file content on both paths.
def _parity_contract_and_code(tmp_path):
    sql_text = "SELECT o.id, o.total FROM bronze.orders o"
    contract_file = tmp_path / "contract.yaml"
    contract_file.write_text(
        "\n".join(
            [
                'contract_id: "P901"',
                'pipeline_name: "ParityVol"',
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
    return contract_file, sql_text


def test_offline_online_attribution_parity_live_code_path(tmp_path):
    from dpif.cli import cli

    code_text = (
        "df_a = spark.read.parquet('abfss://lake@acct.dfs.core.windows.net/a')\n"
        "df_b = spark.read.parquet('abfss://lake@acct.dfs.core.windows.net/b')\n"
    )
    contract_file, _ = _parity_contract_and_code(tmp_path)
    code_file = tmp_path / "code.py"
    code_file.write_text(code_text, encoding="utf-8")

    offline = CliRunner().invoke(
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
    assert offline.exit_code == 0, offline.output
    reports = list(tmp_path.rglob("validation.json"))
    assert reports, "offline validation must persist validation.json"
    offline_doc = json.loads(reports[0].read_text(encoding="utf-8"))

    conn = MockDiscoveryConnector(
        job_payload={
            "job_id": 9108,
            "settings": {
                "tasks": [{"task_key": "etl", "notebook_task": {"notebook_path": "/etl/nb"}}]
            },
        },
        workspace_export_map={"/etl/nb": {"content": _b64(code_text), "file_type": "PYTHON"}},
    )
    online = OnlineValidationOrchestrator(connector=conn).validate(
        job_id=9108,
        contract_path=str(contract_file),
        include_historical_runs=False,
    )
    online_attr = (
        online.evidence_sufficiency.volume_attribution
        if online.evidence_sufficiency
        else {}
    )
    offline_attr = offline_doc["evidence_sufficiency"]["volume_attribution"]
    # Same counts, same natures (all UNKNOWN), same UNKNOWN node volumes.
    assert online_attr == offline_attr
    assert offline_attr["unknown"] == 2
    online_nodes = {
        (n.dataset.name, n.volume.input_bytes if n.volume else None)
        for n in (online.flow_graph.nodes if online.flow_graph else [])
        if n.kind.value == "SOURCE" and n.dataset is not None
    }
    offline_nodes = {
        (n["dataset"]["name"], (n.get("volume") or {}).get("input_bytes"))
        for n in offline_doc["pipeline_flow_graph"]["nodes"]
        if n["kind"] == "SOURCE" and n.get("dataset")
    }
    assert online_nodes == offline_nodes

    def _task_decision(suff):
        return next(d for d in suff.decisions if d.decision_name == "TASK_COVERAGE")

    assert (
        _task_decision(online.evidence_sufficiency).decision_status
        == _task_decision(
            EvidenceSufficiencyAssessment.model_validate(
                offline_doc["evidence_sufficiency"]
            )
        ).decision_status
        == "PASS"
    )


def test_offline_online_attribution_parity_measured_sql(tmp_path):
    contract_file, sql_text = _parity_contract_and_code(tmp_path)
    code_file = tmp_path / "q.sql"
    code_file.write_text(sql_text, encoding="utf-8")
    hist_file = tmp_path / "history.json"
    hist_file.write_text(
        json.dumps(
            {
                "queries": [
                    {
                        "query_id": "q-par",
                        "statement_id": "s-par",
                        "query_text": sql_text,
                        "tables": ["bronze.orders"],
                        "status": "FINISHED",
                        "read_bytes": 52428800,
                        "written_bytes": 1048576,
                        "rows": 12000,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    from dpif.cli import cli

    offline = CliRunner().invoke(
        cli,
        [
            "validate",
            "--contract",
            str(contract_file),
            "--code",
            str(code_file),
            "--query-history",
            str(hist_file),
            "--output-dir",
            str(tmp_path),
            "--offline",
        ],
    )
    assert offline.exit_code == 0, offline.output
    reports = list(tmp_path.rglob("validation.json"))
    assert reports, "offline validation must persist validation.json"
    offline_doc = json.loads(reports[0].read_text(encoding="utf-8"))
    offline_attr = offline_doc["evidence_sufficiency"]["volume_attribution"]
    offline_nodes = {
        (n["dataset"]["name"], (n.get("volume") or {}).get("input_bytes"))
        for n in offline_doc["pipeline_flow_graph"]["nodes"]
        if n["kind"] == "SOURCE" and n.get("dataset")
    }
    assert ("bronze.orders", 52428800) in offline_nodes

    entry = {
        "query_id": "q-par",
        "statement_id": "s-par",
        "query_text": sql_text,
        "tables": ["bronze.orders"],
        "status": "FINISHED",
        "read_bytes": 52428800,
        "written_bytes": 1048576,
        "rows": 12000,
    }
    conn = MockDiscoveryConnector(
        job_payload={
            "job_id": 9107,
            "settings": {
                "tasks": [{"task_key": "qry", "notebook_task": {"notebook_path": "/etl/q"}}]
            },
        },
        workspace_export_map={"/etl/q": {"content": _b64(sql_text), "file_type": "SQL"}},
    )
    conn.get_query_history = lambda limit=25: {"res": [dict(entry)]}  # type: ignore[method-assign]
    # Identical file content on both paths (code_path override): the live
    # task enumeration still runs, but analysis consumes the same SQL text.
    online = OnlineValidationOrchestrator(connector=conn).validate(
        job_id=9107,
        contract_path=str(contract_file),
        code_path=str(code_file),
        include_historical_runs=False,
    )
    online_attr = (
        online.evidence_sufficiency.volume_attribution
        if online.evidence_sufficiency
        else {}
    )
    online_nodes = {
        (n.dataset.name, n.volume.input_bytes if n.volume else None)
        for n in (online.flow_graph.nodes if online.flow_graph else [])
        if n.kind.value == "SOURCE" and n.dataset is not None
    }

    # Same counts, same natures, same per-source volumes on both paths.
    assert online_attr == offline_attr
    assert online_nodes == offline_nodes
    assert ("bronze.orders", 52428800) in online_nodes

    def _task_decision(suff):
        return next(d for d in suff.decisions if d.decision_name == "TASK_COVERAGE")

    assert (
        _task_decision(online.evidence_sufficiency).decision_status
        == _task_decision(
            EvidenceSufficiencyAssessment.model_validate(
                offline_doc["evidence_sufficiency"]
            )
        ).decision_status
    )


# I. M5E/M5G outputs semantically stable with attribution present.
def test_m5e_m5g_stable_with_attribution():
    from dpif.analyzers.alignment import ThreeLayerAlignmentAnalyzer
    from dpif.analyzers.implementation import DeveloperImplementationAnalyzer

    analysis = analyze_source("df = spark.read.table('a.t')", filename="etl.py")
    plain = {"code_snippet": "df = spark.read.table('a.t')"}
    attributed_ctx = dict(plain)
    attributed_ctx["volume_attribution"] = {
        "attributed": {"DATA_PROFILE": 1},
        "unknown": 0,
        "total_sources": 1,
    }
    assert (
        DeveloperImplementationAnalyzer(analysis, context=dict(plain))
        .analyze()
        .overall_status
        == DeveloperImplementationAnalyzer(analysis, context=attributed_ctx)
        .analyze()
        .overall_status
    )
    assert (
        ThreeLayerAlignmentAnalyzer(analysis, context=dict(plain))
        .analyze()
        .overall_status
        == ThreeLayerAlignmentAnalyzer(analysis, context=attributed_ctx)
        .analyze()
        .overall_status
    )


# J. Checkpoint findings stable with attribution present.
def test_checkpoint_findings_stable_with_attribution():
    cps = build_all_checkpoints(code_text="df = spark.read.table('a.t')")
    engine = CheckpointEngine()
    base_ctx: dict[str, object] = {
        "pipeline_name": "p",
        "code_snippet": "df = spark.read.table('a.t')",
    }
    attr_ctx = dict(base_ctx)
    attr_ctx["volume_attribution"] = {
        "attributed": {"DATA_PROFILE": 1},
        "unknown": 0,
        "total_sources": 1,
    }
    base = engine.run_all_checkpoints(
        [cp.model_copy(deep=True) for cp in cps], base_ctx
    )
    ward = engine.run_all_checkpoints(
        [cp.model_copy(deep=True) for cp in cps], attr_ctx
    )
    assert {k: v.status for k, v in base.items()} == {
        k: v.status for k, v in ward.items()
    }


# Fixture measurement forwarding: present, absent, and malformed.
def test_normalize_forwards_measurements():
    entries = normalize_query_history(
        [
            {
                "query_id": "q-1",
                "query_text": "SELECT 1",
                "read_bytes": 100,
                "written_bytes": 20,
                "rows": 5,
            }
        ]
    )
    assert entries is not None and entries[0].read_bytes == 100
    assert entries[0].written_bytes == 20
    assert entries[0].rows == 5


def test_normalize_missing_measurements_stay_unknown():
    entries = normalize_query_history([{"query_id": "q-1", "query_text": "SELECT 1"}])
    assert entries is not None
    assert entries[0].read_bytes is None
    assert entries[0].written_bytes is None
    assert entries[0].rows is None


def test_normalize_malformed_measurements_stay_unknown():
    entries = normalize_query_history(
        [
            {
                "query_id": "q-1",
                "query_text": "SELECT 1",
                "read_bytes": -5,
                "written_bytes": "lots",
                "rows": True,
            }
        ]
    )
    assert entries is not None
    assert entries[0].read_bytes is None
    assert entries[0].written_bytes is None
    assert entries[0].rows is None
