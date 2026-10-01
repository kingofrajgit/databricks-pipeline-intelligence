# DPIF — Runtime Operation Correlation (Phase 3)

**Status:** Implemented (minimal approved scope).
**Basis:** `docs/PHASE_3_RUNTIME_OPERATION_CORRELATION_AUDIT.md` — the audit proved
reliable correlation is impossible without shared identifiers; this phase adds the
smallest identifier-backed foundation.
**Rule:** UNKNOWN BY DEFAULT. No per-operation volume attribution (explicitly out of scope).

---

## 1. What was built

```
static SQL ──▶ canonicalize + sha256 ──▶ fingerprint
                                            ↕  (exact match, exactly one query_id)
query history ──▶ normalize ──▶ QueryHistoryEntry ──▶ fingerprint
                                            ↕
                          OperationRuntimeCorrelation (sidecar on PipelineFlowGraph)
```

One shared correlator (`dpif.flow.correlation.correlate_sql_operations`) serves
BOTH validation paths: offline feeds a `--query-history` fixture, online feeds
live `sql/history/queries` evidence. Same model, same join logic.

## 2. SQL fingerprint (`src/dpif/sql/parser.py`)

- `canonicalize_sql(sql, dialect) -> str | None`: regenerate through sqlglot
  (normalizes keyword case, spacing, comments), lowercase, mask string/number
  literals, strip comments, collapse whitespace. Failure → `None` (UNKNOWN).
- `sql_fingerprint(sql, dialect) -> str | None`: sha256 of canonical form.
- `SQLQuery` gains `canonical_sql: str = ""` and `fingerprint: str | None = None`
  (`src/dpif/sql/models.py`); raw `sql` and legacy `normalized_sql` untouched.
- No semantic equivalence is claimed beyond textual-canonical equality. If the
  parser cannot establish it, the fingerprint is `None`.

## 3. Query-history evidence

- Connector: `DatabricksConnector.get_query_history(limit=25)` (default `None`);
  live impl `GET /api/2.0/sql/history/queries` with bounded `max_results`
  (`src/dpif/connectors/live.py`). Read-only; existing credential resolution,
  error classification, and no-secret logging.
- Provider: `EvidenceCategory.QUERY_HISTORY` + `fetch_query_history_item()`
  (`src/dpif/providers/base.py`) — sanitized payload, masked errors, structured
  `AcquisitionError`. Valid empty (`[]`) stays available; failure yields
  `payload=None` + `is_available=False`. API failure ≠ empty evidence.
- Normalized shape: `QueryHistoryEntry` (`src/dpif/runtime/models.py`:
  `query_id` required, `statement_id`, `query_text`, `tables`, `status`).
  Entries without a `query_id` can never anchor a correlation and are skipped
  (`normalize_query_history` in `src/dpif/flow/correlation.py`).
- Offline: `--query-history PATH` fixture (list or `{"queries"|"res": [...]}`),
  `FIXTURE` provenance (`src/dpif/cli.py`).
- Online: retrieved ONLY when static SQL fingerprints exist (narrow scope);
  `LIVE`/`ERROR`/`UNAVAILABLE` diagnostics + `summary["query_history"]`
  (`src/dpif/orchestration/online.py`).

## 4. Correlation model (`src/dpif/flow/models.py`, logic in `src/dpif/flow/correlation.py`)

`OperationRuntimeCorrelation`: `operation_node_id` (`sql:query:{i}` within the
analysis — an internal enumerator, never a cross-domain join key),
`runtime_query_id`/`runtime_statement_id`, `method` (`CorrelationMethod.SQL_FINGERPRINT`
— the only admitted method), `state` (`EvidenceState`, default UNKNOWN),
`confidence` (`ConfidenceLevel`, default INSUFFICIENT), `evidence` dict,
`provenance` (`FlowProvenance`). No new provenance or confidence vocabulary.

Match rules (all enforced, all tested):
- fingerprint missing / history `None` / history `[]` → UNKNOWN (distinct reasons).
- zero fingerprint matches → UNKNOWN (`no_matching_query`).
- ≥2 distinct `query_id`s → UNKNOWN (`ambiguous_match`) — never promoted.
- exactly one match → KNOWN, `HIGH` confidence, `RUNTIME` (or fixture) provenance
  with `reference=query_history:{id}`.

`PipelineFlowGraph.correlations` carries the sidecar list (default `[]`);
`to_dict()` serializes it automatically. The legacy rule-ID `_CORRELATION_MAP`
(`src/dpif/runtime/correlation.py`) is untouched and semantically separate:
corroboration, not attribution.

## 5. Why PySpark and DLT remain UNKNOWN

- **PySpark:** static ops expose type/line/var-name/snippet only — none exists in
  any runtime payload, and Spark plans cannot be reconstructed from AST. No
  heuristic matching was built; PySpark ops receive no records.
- **DLT:** only table-name strings + lines are captured; update/flow/query IDs
  require the Updates/Events API (a broad platform integration, out of scope).
  DLT nodes receive no records. Missing evidence documented, not worked around.

## 6. Reporting

- JSON: `pipeline_flow_graph.correlations[]` with state/method/ids/provenance.
- Markdown (flow section, additive): `Runtime Correlation: N KNOWN / M recorded
  (UNKNOWN by default)` plus one line per KNOWN record. No stage IDs invented;
  no existing checkpoint/decision reporting changed.

## 7. Anti-fabrication rules (enforced by design + tests)

Operation/stage index, list position, execution order, duration similarity,
arbitrary names, and rule ID alone are never join keys (tested: shuffled
history matches identically; rule-ID results carry no identity fields).
`[]`, `None`, missing identifiers, and API failures never become correlations.
