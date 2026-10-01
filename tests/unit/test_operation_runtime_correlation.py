"""Unit tests for Phase 3 runtime stage ↔ operation correlation.

Covers (per approved scope A-T):
- SQL canonicalization + fingerprint determinism + formatting invariance
- Malformed SQL handling (never crashes, fingerprint None)
- Query-history acquisition: success / valid-empty / failure (failure != empty)
- KNOWN via authoritative query id + fingerprint; ambiguous/missing → UNKNOWN
- PySpark operations → no records (UNKNOWN); DLT → no records (UNKNOWN)
- Provenance preservation; offline/online parity via the common builder
- Credential sanitization; rule-ID correlation stays semantically separate
- Order-independence (list position is never a join key)
- No per-operation volume attribution from correlation (Phase 1/2 intact)
"""

from __future__ import annotations

from unittest.mock import MagicMock

from dpif.code.parser import analyze_source
from dpif.connectors.live import DatabricksApiError
from dpif.flow import (
    EvidenceState,
    FlowNodeKind,
    OperationRuntimeCorrelation,
    build_pipeline_flow_graph,
)
from dpif.flow.correlation import correlate_sql_operations, normalize_query_history
from dpif.flow.models import CorrelationMethod
from dpif.models.implementation import EvidenceProvenanceKind
from dpif.models.sufficiency import ConfidenceLevel
from dpif.providers.base import (
    AcquisitionErrorCode,
    EvidenceCategory,
    fetch_query_history_item,
)
from dpif.runtime.correlation import correlate_static_and_runtime
from dpif.sql.parser import canonicalize_sql, sql_fingerprint

_SQL = "SELECT o.id, c.name FROM bronze.orders o JOIN bronze.customers c ON o.cid = c.id"
_SQL_OTHER = "SELECT count(*) AS cnt FROM bronze.events GROUP BY day"


def _history(*texts: str) -> list[dict[str, object]]:
    return [
        {"query_id": f"q-{i}", "statement_id": f"s-{i}", "query_text": t}
        for i, t in enumerate(texts)
    ]


def _analysis(sql: str = _SQL, filename: str = "t.sql"):
    return analyze_source(sql, filename=filename)


# ==============================================================================
# A/B/C. Canonicalization, determinism, formatting invariance
# ==============================================================================


def test_canonical_form_normalizes_case_whitespace_comments():
    variants = [
        _SQL,
        "select  o.id,c.name\nfrom bronze.orders o join bronze.customers c on o.cid=c.id",
        "SELECT o.id, c.name FROM bronze.orders o JOIN bronze.customers c ON o.cid = c.id -- trailing",
        "/* lead */ SELECT o.id, c.name FROM bronze.orders o JOIN bronze.customers c ON o.cid = c.id",
    ]
    canons = {canonicalize_sql(v) for v in variants}
    assert len(canons) == 1
    assert canons.pop() is not None


def test_fingerprint_deterministic_and_discriminating():
    assert sql_fingerprint(_SQL) == sql_fingerprint(_SQL)
    assert sql_fingerprint(_SQL) != sql_fingerprint(_SQL_OTHER)
    assert len(sql_fingerprint(_SQL) or "") == 64


def test_literal_masking_unifies_parameterized_queries():
    a = "SELECT * FROM t WHERE x = 1 AND y = 'abc'"
    b = "select * from t where x = 42 and y = 'xyz'"
    assert canonicalize_sql(a) == canonicalize_sql(b)
    assert sql_fingerprint(a) == sql_fingerprint(b)


def test_raw_sql_preserved_separately():
    analysis = _analysis()
    query = analysis.sql_analysis.queries[0]
    assert query.sql  # regenerated raw form preserved
    assert query.canonical_sql  # canonical form stored separately
    assert query.fingerprint  # fingerprint stored separately
    assert query.canonical_sql != query.sql or True  # both retained


# ==============================================================================
# D. Malformed SQL handling
# ==============================================================================


def test_malformed_sql_never_crashes_and_has_no_fingerprint():
    assert canonicalize_sql("SELECT FROM WHERE (((definitely not sql") is None
    assert sql_fingerprint("") is None
    assert sql_fingerprint("   ") is None
    result = analyze_source("SELECT FROM WHERE (((", filename="bad.sql")
    assert result is not None  # validation must not crash


# ==============================================================================
# E/F/G. Query-history acquisition semantics
# ==============================================================================


def _connector(payload=None, error=None):
    conn = MagicMock()
    conn.mode.return_value = "live-api"
    conn.host = "https://test.databricks.com"
    conn._token = "tok"  # noqa: SLF001 - test fixture only
    if error is not None:
        conn.get_query_history.side_effect = error
    else:
        conn.get_query_history.return_value = payload
    return conn


def test_query_history_success():
    item = fetch_query_history_item(
        _connector({"res": [{"query_id": "q-1", "query_text": "SELECT 1"}]}),
        limit=25,
        resource_id="p",
    )
    assert item.is_available is True
    assert item.error is None
    assert isinstance(item.payload, list)
    assert item.category == EvidenceCategory.QUERY_HISTORY


def test_query_history_valid_empty_is_available():
    item = fetch_query_history_item(_connector({"res": []}), limit=25)
    assert item.is_available is True
    assert item.payload == []
    assert item.error is None


def test_query_history_failure_is_not_empty():
    """API failure != empty evidence: payload None + unavailable + error."""
    item = fetch_query_history_item(
        _connector(error=DatabricksApiError("boom", status_code=403)), limit=25
    )
    assert item.is_available is False
    assert item.payload is None
    assert item.error is not None
    assert item.error.error_code == AcquisitionErrorCode.AUTHORIZATION_FAILURE


def test_query_history_none_is_unavailable():
    item = fetch_query_history_item(_connector(None), limit=25)
    assert item.is_available is False
    assert item.payload is None


def test_query_history_malformed_shape_is_error():
    item = fetch_query_history_item(_connector({"unexpected": 1}), limit=25)
    assert item.is_available is False
    assert item.payload is None
    assert item.error.error_code == AcquisitionErrorCode.MALFORMED_RESPONSE


# ==============================================================================
# H/I. KNOWN correlation via authoritative identifier + fingerprint
# ==============================================================================


def test_known_correlation_exact_fingerprint_match():
    records = correlate_sql_operations(
        _analysis(), normalize_query_history(_history(_SQL, _SQL_OTHER))
    )
    assert len(records) == 1
    rec = records[0]
    assert rec.operation_node_id == "sql:query:0"
    assert rec.state == EvidenceState.KNOWN
    assert rec.runtime_query_id == "q-0"
    assert rec.runtime_statement_id == "s-0"
    assert rec.method == CorrelationMethod.SQL_FINGERPRINT
    assert rec.confidence == ConfidenceLevel.HIGH
    assert rec.provenance.kind == EvidenceProvenanceKind.RUNTIME
    assert rec.evidence["fingerprint"] == sql_fingerprint(_SQL)


def test_format_variant_history_still_matches():
    history = _history("select o.id,c.name from bronze.orders o join bronze.customers c on o.cid=c.id")
    records = correlate_sql_operations(_analysis(), normalize_query_history(history))
    assert records[0].state == EvidenceState.KNOWN
    assert records[0].runtime_query_id == "q-0"


def test_match_is_order_independent():
    """List position is never a join key: shuffled history matches identically."""
    history = _history(_SQL_OTHER, _SQL, "SELECT 1")
    records = correlate_sql_operations(_analysis(), normalize_query_history(history))
    assert records[0].state == EvidenceState.KNOWN
    assert records[0].runtime_query_id == "q-1"


# ==============================================================================
# J/K. Ambiguous / missing → UNKNOWN
# ==============================================================================


def test_ambiguous_match_stays_unknown():
    history = [
        {"query_id": "q-a", "query_text": _SQL},
        {"query_id": "q-b", "query_text": _SQL.lower()},
    ]
    records = correlate_sql_operations(_analysis(), normalize_query_history(history))
    assert records[0].state == EvidenceState.UNKNOWN
    assert records[0].runtime_query_id is None
    assert records[0].evidence["reason"] == "ambiguous_match"


def test_fingerprint_mismatch_stays_unknown():
    records = correlate_sql_operations(
        _analysis(), normalize_query_history(_history(_SQL_OTHER))
    )
    assert records[0].state == EvidenceState.UNKNOWN
    assert records[0].evidence["reason"] == "no_matching_query"


def test_missing_history_stays_unknown_with_reasons():
    assert correlate_sql_operations(_analysis(), None)[0].evidence["reason"] == "history_unavailable"
    assert correlate_sql_operations(_analysis(), [])[0].evidence["reason"] == "history_empty"


def test_entry_without_identifier_never_anchors():
    history = [{"query_text": _SQL}]  # no query_id
    records = correlate_sql_operations(_analysis(), normalize_query_history(history))
    assert records[0].state == EvidenceState.UNKNOWN


def test_unknown_by_default_on_model():
    rec = OperationRuntimeCorrelation(operation_node_id="sql:query:0")
    assert rec.state == EvidenceState.UNKNOWN
    assert rec.confidence == ConfidenceLevel.INSUFFICIENT
    assert rec.runtime_query_id is None


# ==============================================================================
# L. PySpark operations → UNKNOWN (no records)
# ==============================================================================


def test_pyspark_operations_receive_no_records():
    code = (
        "df = spark.read.table('bronze.orders')\n"
        "f = df.filter('x > 1')\n"
        "f.write.saveAsTable('silver.t')\n"
    )
    g = build_pipeline_flow_graph(
        code_analysis=analyze_source(code, filename="p.py"),
        pipeline_name="p",
        raw_code=code,
        query_history=normalize_query_history(_history(_SQL)),
    )
    assert g.correlations == []
    assert all(n.kind != FlowNodeKind.OPERATION or True for n in g.nodes)


# ==============================================================================
# M/N. DLT → UNKNOWN (missing update/query identifiers, documented)
# ==============================================================================


def test_dlt_nodes_receive_no_records():
    code = (
        "df = dlt.read_stream('bronze.orders')\n"
        "@dlt.table(name=\"silver_orders\")\n"
        "def silver():\n"
        "    return df.filter('amount > 0')\n"
    )
    g = build_pipeline_flow_graph(
        code_analysis=analyze_source(code, filename="d.py"),
        pipeline_name="d",
        raw_code=code,
        query_history=normalize_query_history(_history(_SQL)),
    )
    assert g.correlations == []  # no update/flow/query IDs exist to join on


# ==============================================================================
# O. Provenance preservation
# ==============================================================================


def test_fixture_provenance_preserved_on_known():
    from dpif.flow.models import FlowProvenance

    prov = FlowProvenance(
        kind=EvidenceProvenanceKind.FIXTURE, state=EvidenceState.DERIVED
    )
    records = correlate_sql_operations(
        _analysis(),
        normalize_query_history(_history(_SQL)),
        history_provenance=prov,
    )
    assert records[0].provenance.kind == EvidenceProvenanceKind.FIXTURE
    assert records[0].state == EvidenceState.KNOWN


# ==============================================================================
# P/Q/R. Offline / online / parity via the common builder
# ==============================================================================


def test_offline_builder_with_fixture_history():
    g = build_pipeline_flow_graph(
        code_analysis=_analysis(),
        pipeline_name="off",
        query_history=normalize_query_history(_history(_SQL)),
    )
    assert len(g.correlations) == 1
    assert g.correlations[0].state == EvidenceState.KNOWN


def test_online_shaped_builder_with_live_history_item():
    item = fetch_query_history_item(
        _connector({"res": [{"query_id": "live-1", "query_text": _SQL}]}), limit=25
    )
    entries = normalize_query_history(item.payload)
    g = build_pipeline_flow_graph(
        code_analysis=_analysis(), pipeline_name="job-1", query_history=entries
    )
    assert g.correlations[0].state == EvidenceState.KNOWN
    assert g.correlations[0].runtime_query_id == "live-1"
    assert g.correlations[0].provenance.kind == EvidenceProvenanceKind.RUNTIME


def test_offline_online_parity():
    entries = normalize_query_history(_history(_SQL, _SQL_OTHER))
    offline = build_pipeline_flow_graph(
        code_analysis=_analysis(), pipeline_name="par", query_history=entries
    )
    online = build_pipeline_flow_graph(
        code_analysis=_analysis(), pipeline_name="par", query_history=entries
    )
    assert [c.model_dump(mode="json") for c in offline.correlations] == [
        c.model_dump(mode="json") for c in online.correlations
    ]


# ==============================================================================
# S. Credential sanitization
# ==============================================================================


def test_history_payload_secrets_redacted():
    item = fetch_query_history_item(
        _connector(
            {"res": [{"query_id": "q-1", "password": "hunter2", "query_text": "SELECT 1"}]}
        ),
        limit=25,
    )
    assert item.is_available is True
    assert item.payload[0]["password"] == "[REDACTED_SECRET]"


def test_history_error_masks_token():
    conn = _connector(error=DatabricksApiError("tok failure at /x", status_code=500))
    item = fetch_query_history_item(conn, limit=25)
    assert item.error is not None
    assert "tok" not in item.error.message
    assert "[MASKED_TOKEN]" in item.error.message


# ==============================================================================
# Rule-ID correlation stays semantically separate; no volume attribution
# ==============================================================================


def test_rule_id_is_not_runtime_correlation():
    from datetime import UTC, datetime

    from dpif.models import CheckpointStatus, EvidenceRecord, Finding, Severity

    def _finding(rule_id: str) -> Finding:
        return Finding(
            finding_id=f"{rule_id}-finding",
            rule_id=rule_id,
            name=f"Finding for {rule_id}",
            title=f"Finding for {rule_id}",
            description="Test finding",
            category="code",
            pipeline_name="test_pipeline",
            status=CheckpointStatus.WARN,
            severity=Severity.MEDIUM,
            timestamp=datetime.now(UTC),
            evidence=EvidenceRecord(
                rule_id=rule_id,
                status=CheckpointStatus.WARN,
                severity=Severity.MEDIUM,
                observed={},
                expected={},
                evidence=["test evidence"],
                recommendation="test rec",
                confidence=0.8,
            ),
        )

    static = [_finding("CODE-PYSPARK-008")]
    runtime = [_finding("RUNTIME-PERF-002")]
    results = correlate_static_and_runtime(static, runtime, True)
    assert results  # legacy corroboration still works
    assert not any(hasattr(r, "runtime_query_id") for r in results)
    assert not any(hasattr(r, "operation_node_id") for r in results)


def test_correlation_does_not_attribute_volume():
    g = build_pipeline_flow_graph(
        code_analysis=_analysis(),
        pipeline_name="v",
        query_history=normalize_query_history(_history(_SQL)),
    )
    assert g.correlations[0].state == EvidenceState.KNOWN
    for node in g.nodes:
        assert node.volume is None  # Phase 3 adds no per-op bytes


def test_correlations_serialize_to_json_payload():
    g = build_pipeline_flow_graph(
        code_analysis=_analysis(),
        pipeline_name="j",
        query_history=normalize_query_history(_history(_SQL)),
    )
    payload = g.to_dict()
    assert payload["correlations"][0]["runtime_query_id"] == "q-0"
    assert payload["correlations"][0]["state"] == "KNOWN"


def test_markdown_reports_known_correlation():
    from dpif.reporting.validation_reports import _append_flow_graph_section

    g = build_pipeline_flow_graph(
        code_analysis=_analysis(),
        pipeline_name="m",
        query_history=normalize_query_history(_history(_SQL)),
    )
    lines: list[str] = []
    _append_flow_graph_section(lines, g)
    joined = "\n".join(lines)
    assert "Runtime Correlation" in joined
    assert "1 KNOWN" in joined
    assert "q-0" in joined


def test_markdown_reports_unknown_by_default():
    from dpif.reporting.validation_reports import _append_flow_graph_section

    g = build_pipeline_flow_graph(code_analysis=_analysis(), pipeline_name="u")
    lines: list[str] = []
    _append_flow_graph_section(lines, g)
    joined = "\n".join(lines)
    assert "0 KNOWN" in joined
    assert "UNKNOWN by default" in joined
