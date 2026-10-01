"""Operation↔runtime correlation engine (Phase 3).

One shared correlator for BOTH validation paths (no offline/online split):
offline feeds fixture query history, online feeds live query history;
the join logic is identical.

UNKNOWN BY DEFAULT: every SQL operation gets a record starting at
UNKNOWN and only exact, unambiguous, identifier-backed matches upgrade
to KNOWN. PySpark operations and DLT nodes have no runtime-joinable
identifier and receive no records (absence == UNKNOWN).

Anti-fabrication: matching is by canonical SQL fingerprint plus an
authoritative ``query_id``. Operation index, stage index, list
position, execution order, duration similarity, and rule IDs are never
used as join keys.
"""

from __future__ import annotations

from typing import Any

from dpif.code.models import CodeAnalysis
from dpif.flow.models import (
    CorrelationMethod,
    EvidenceState,
    FlowProvenance,
    OperationRuntimeCorrelation,
)
from dpif.models.implementation import EvidenceProvenanceKind
from dpif.models.sufficiency import ConfidenceLevel
from dpif.runtime.models import QueryHistoryEntry
from dpif.sql.parser import sql_fingerprint


def normalize_query_history(raw: Any) -> list[QueryHistoryEntry] | None:
    """Normalize raw query-history payloads to entries.

    Returns ``None`` when ``raw`` is ``None`` (unavailable — never an
    empty success). Accepts a bare list, ``{"queries": [...]}`` (fixture
    convention), or ``{"res": [...]}`` (Databricks history shape).
    Entries without a ``query_id`` are skipped: without an authoritative
    identifier they can never anchor a correlation. Never raises.
    """
    if raw is None:
        return None
    if isinstance(raw, dict):
        for key in ("queries", "res", "result"):
            if isinstance(raw.get(key), list):
                raw = raw[key]
                break
        else:
            return []
    if not isinstance(raw, list):
        return []
    entries: list[QueryHistoryEntry] = []
    for item in raw:
        try:
            if isinstance(item, QueryHistoryEntry):
                entry = item
            elif isinstance(item, dict):
                query_id = item.get("query_id") or item.get("statement_id") or ""
                if not str(query_id).strip():
                    continue
                tables = item.get("tables") or item.get("tables_accessed") or []
                entry = QueryHistoryEntry(
                    query_id=str(query_id),
                    statement_id=item.get("statement_id"),
                    query_text=item.get("query_text") or item.get("query") or item.get("sql"),
                    tables=[str(t) for t in tables] if isinstance(tables, list) else [],
                    status=item.get("status"),
                )
            else:
                continue
            if entry.query_id.strip():
                entries.append(entry)
        except Exception:
            continue
    return entries


def _entry_fingerprint(entry: QueryHistoryEntry) -> str | None:
    if not entry.query_text or not entry.query_text.strip():
        return None
    return sql_fingerprint(entry.query_text)


def correlate_sql_operations(
    code_analysis: CodeAnalysis | None,
    query_history: list[QueryHistoryEntry] | list[dict[str, Any]] | None,
    history_provenance: FlowProvenance | None = None,
) -> list[OperationRuntimeCorrelation]:
    """Correlate static SQL operations to query-history entries.

    One record per SQL query in the analysis (``sql:query:{i}``), UNKNOWN
    by default. ``query_history=None`` means unavailable; ``[]`` means a
    valid empty result — both stay UNKNOWN with distinct reasons.
    """
    if code_analysis is None or code_analysis.sql_analysis is None:
        return []
    queries = code_analysis.sql_analysis.queries
    if not queries:
        return []

    entries = normalize_query_history(query_history)
    runtime_kind = (
        history_provenance.kind if history_provenance else EvidenceProvenanceKind.RUNTIME
    )

    def unknown_record(node_id: str, reason: str, static_gap: bool = False) -> OperationRuntimeCorrelation:
        return OperationRuntimeCorrelation(
            operation_node_id=node_id,
            evidence={"reason": reason},
            provenance=FlowProvenance(
                kind=EvidenceProvenanceKind.STATIC_CODE if static_gap else runtime_kind,
                state=EvidenceState.UNKNOWN,
            ),
        )

    records: list[OperationRuntimeCorrelation] = []
    for idx, query in enumerate(queries):
        node_id = f"sql:query:{idx}"
        fingerprint = getattr(query, "fingerprint", None)
        if not fingerprint:
            records.append(unknown_record(node_id, "fingerprint_unavailable", static_gap=True))
            continue
        if entries is None:
            records.append(unknown_record(node_id, "history_unavailable"))
            continue
        if not entries:
            records.append(unknown_record(node_id, "history_empty"))
            continue
        candidates: list[QueryHistoryEntry] = []
        for entry in entries:
            try:
                candidate = (
                    entry
                    if isinstance(entry, QueryHistoryEntry)
                    else QueryHistoryEntry.model_validate(entry)
                )
            except Exception:
                continue
            if not candidate.query_id.strip():
                continue
            if _entry_fingerprint(candidate) == fingerprint:
                candidates.append(candidate)
        if not candidates:
            records.append(
                OperationRuntimeCorrelation(
                    operation_node_id=node_id,
                    evidence={"reason": "no_matching_query", "fingerprint": fingerprint},
                    provenance=FlowProvenance(kind=runtime_kind, state=EvidenceState.UNKNOWN),
                )
            )
            continue
        distinct_ids = sorted({c.query_id for c in candidates})
        if len(distinct_ids) > 1:
            records.append(
                OperationRuntimeCorrelation(
                    operation_node_id=node_id,
                    evidence={
                        "reason": "ambiguous_match",
                        "fingerprint": fingerprint,
                        "candidate_count": str(len(distinct_ids)),
                    },
                    provenance=FlowProvenance(kind=runtime_kind, state=EvidenceState.UNKNOWN),
                )
            )
            continue
        match = candidates[0]
        evidence: dict[str, str] = {
            "fingerprint": fingerprint,
            "query_id": match.query_id,
        }
        if match.statement_id:
            evidence["statement_id"] = match.statement_id
        records.append(
            OperationRuntimeCorrelation(
                operation_node_id=node_id,
                runtime_query_id=match.query_id,
                runtime_statement_id=match.statement_id,
                method=CorrelationMethod.SQL_FINGERPRINT,
                state=EvidenceState.KNOWN,
                confidence=ConfidenceLevel.HIGH,
                evidence=evidence,
                provenance=FlowProvenance(
                    kind=history_provenance.kind if history_provenance else EvidenceProvenanceKind.RUNTIME,
                    state=EvidenceState.KNOWN,
                    reference=f"query_history:{match.query_id}",
                ),
            )
        )
    return records
