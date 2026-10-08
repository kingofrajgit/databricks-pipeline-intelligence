"""P10-2 focused tests: SQL TASK-header boundary remediation.

Provider `# --- TASK:` headers are authoritative task metadata, not SQL.
Before this fix, header-prefixed SQL blobs failed SQL parsing (zero queries,
no fingerprints, no query-history correlation) while bare files parsed fine.
The remediation blank-line-replaces ONLY strict-header lines on a sanitized
COPY handed to SQLParser: line numbers preserved, original blob untouched
for P9-2 task attribution, fingerprint algorithm untouched.
"""

from __future__ import annotations

from dpif.code.parser import (
    analyze_source,
    parse_task_boundaries,
    strip_task_headers_for_sql,
)
from dpif.sql.parser import sql_fingerprint

BARE = "SELECT *\nFROM catalog.schema.table;"
ENVELOPE = "# --- TASK: task_1 (example.sql) ---\n\nSELECT *\nFROM catalog.schema.table;"


def _queries(code: str, filename: str = "q.sql"):
    analysis = analyze_source(code, filename=filename)
    assert analysis.sql_analysis is not None
    return analysis.sql_analysis.queries


# Helper: blank replacement preserves line count and non-header content.
def test_strip_preserves_line_count_and_content():
    code = "# --- TASK: t1 (/a) ---\nSELECT 1\n\n# --- TASK: t2 (/b) ---\nSELECT 2"
    stripped = strip_task_headers_for_sql(code)
    assert len(stripped.splitlines()) == len(code.splitlines())
    assert "SELECT 1" in stripped and "SELECT 2" in stripped
    assert "# --- TASK" not in stripped


def test_strip_no_headers_is_identity():
    code = "SELECT *\nFROM t;\n-- comment\n"
    assert strip_task_headers_for_sql(code) == code


# 1. Bare SQL remains unchanged.
def test_bare_sql_unchanged():
    before = analyze_source(BARE, filename="q.sql")
    after = analyze_source(BARE, filename="q.sql")
    assert len(before.sql_analysis.queries) == len(after.sql_analysis.queries) == 1
    assert (
        before.sql_analysis.queries[0].fingerprint
        == after.sql_analysis.queries[0].fingerprint
    )


# 2. Single TASK header + SQL parses.
def test_single_header_sql_parses():
    queries = _queries(ENVELOPE)
    assert len(queries) == 1
    assert queries[0].fingerprint is not None
    assert any(t.name == "table" for t in queries[0].tables)


# 3. Multiple TASK headers preserve boundaries (queries per segment).
def test_multiple_headers_preserve_boundaries():
    blob = (
        "# --- TASK: one (/a.sql) ---\n"
        "SELECT id FROM bronze.orders;\n"
        "\n"
        "# --- TASK: two (/b.sql) ---\n"
        "SELECT id FROM bronze.customers;\n"
    )
    queries = _queries(blob)
    assert len(queries) == 2
    tables = [[t.name for t in q.tables] for q in queries]
    assert tables[0] == ["orders"]
    assert tables[1] == ["customers"]
    assert parse_task_boundaries(blob) == [("one", 1), ("two", 4)]


# 4. Multiple SQL statements preserve ordering.
def test_multiple_statements_ordering():
    blob = ENVELOPE + "\nSELECT id FROM bronze.other;\n"
    queries = _queries(blob)
    assert len(queries) == 2
    assert [t.name for t in queries[0].tables] == ["table"]
    assert [t.name for t in queries[1].tables] == ["other"]


# 5. Multiline SQL / CTE / window expressions remain intact.
def test_multiline_cte_window_intact():
    body = (
        "WITH ranked AS (\n"
        "  SELECT id, ROW_NUMBER() OVER (PARTITION BY day ORDER BY ts) AS rn\n"
        "  FROM bronze.events\n"
        ")\n"
        "SELECT * FROM ranked WHERE rn = 1;\n"
    )
    queries = _queries("# --- TASK: w (/w.sql) ---\n" + body)
    assert len(queries) == 1
    assert queries[0].fingerprint == _queries(body)[0].fingerprint


# 6. SQL comments remain intact.
def test_sql_comments_intact():
    body = "-- lead comment\nSELECT id /* inline */ FROM bronze.orders;\n"
    queries = _queries("# --- TASK: c (/c.sql) ---\n" + body)
    assert len(queries) == 1
    assert queries[0].fingerprint == _queries(body)[0].fingerprint


# 7. '#' inside SQL string literals remains intact.
def test_hash_inside_string_literal_intact():
    body = "SELECT '# --- TASK: fake (...) ---' AS value;\n"
    bare = _queries(body)
    assert len(bare) == 1
    assert bare[0].fingerprint is not None
    # The literal line does not start with the header marker, so the strict
    # full-line regex leaves it untouched: envelope parses identically.
    enveloped = _queries("# --- TASK: t (/t.sql) ---\n" + body)
    assert len(enveloped) == 1
    assert enveloped[0].fingerprint == bare[0].fingerprint


# 8. Embedded spark.sql("...") behavior remains unchanged.
def test_embedded_spark_sql_unchanged():
    code = 'df = spark.sql("SELECT id FROM bronze.orders")\n'
    before = analyze_source(code, filename="nb.py")
    assert before.language == "python"
    assert before.sql_analysis is not None
    assert len(before.sql_analysis.queries) == 1


# 9. Malformed SQL remains UNKNOWN / fails through the honest path.
def test_malformed_sql_unknown():
    bad = "# --- TASK: t (/t.sql) ---\nSELECT FROM WHERE (((definitely not sql"
    analysis = analyze_source(bad, filename="q.sql")
    assert analysis.sql_analysis is not None
    assert not analysis.sql_analysis.queries or analysis.parse_error is not None
    assert sql_fingerprint(bad) is None


# 10. Fingerprint equivalence: bare SQL == header-envelope SQL.
def test_fingerprint_equivalence():
    bare_fp = _queries(BARE)[0].fingerprint
    env_fp = _queries(ENVELOPE)[0].fingerprint
    assert bare_fp is not None and env_fp is not None
    assert bare_fp == env_fp


# 11. Task attribution remains based on the original blob.
def test_task_attribution_uses_original_blob():
    from dpif.code.parser import tag_operations_with_tasks

    blob = "# --- TASK: t1 (/a.py) ---\ndf = spark.read.table('a.t')\n"
    analysis = analyze_source(blob, filename="combined.py")
    assert analysis.language == "python"
    assert parse_task_boundaries(blob) == [("t1", 1)]
    assert tag_operations_with_tasks(analysis, parse_task_boundaries(blob)) >= 1
    assert {op.task_key for op in analysis.operations} == {"t1"}


# 12. Query-history correlation works when an exact known correlation exists.
def test_query_history_correlation_exact():
    from dpif.flow.correlation import correlate_sql_operations, normalize_query_history

    analysis = analyze_source(ENVELOPE, filename="q.sql")
    assert len(analysis.sql_analysis.queries) == 1
    history = normalize_query_history(
        [
            {
                "query_id": "q-1",
                "statement_id": "s-1",
                "query_text": BARE,
                "tables": ["catalog.schema.table"],
                "status": "FINISHED",
            }
        ]
    )
    assert history is not None
    records = correlate_sql_operations(analysis, history)
    known = [r for r in records if r.state.value == "KNOWN"]
    assert len(known) == 1
    assert known[0].runtime_query_id == "q-1"


# 13. Missing runtime measurement remains UNKNOWN.
def test_missing_measurement_unknown():
    from dpif.flow.attribution import attribute_volumes
    from dpif.flow.builder import build_pipeline_flow_graph
    from dpif.flow.models import FlowNodeKind

    analysis = analyze_source(ENVELOPE, filename="q.sql")
    graph = build_pipeline_flow_graph(
        code_analysis=analysis,
        pipeline_name="p",
        raw_code=ENVELOPE,
        volume_mode="attributed",
    )
    summary = attribute_volumes(graph, query_entries=[])
    assert summary["attributed"] == {}
    assert all(n.volume is None for n in graph.nodes if n.kind == FlowNodeKind.SOURCE)


# 14. Offline/online semantic parity for equivalent SQL.
def test_offline_online_semantic_parity():
    offline = analyze_source(BARE, filename="q.sql")
    online = analyze_source(ENVELOPE, filename="q.sql")
    assert len(offline.sql_analysis.queries) == len(online.sql_analysis.queries) == 1
    assert (
        offline.sql_analysis.queries[0].fingerprint
        == online.sql_analysis.queries[0].fingerprint
    )
    assert [t.name for t in offline.sql_analysis.queries[0].tables] == [
        t.name for t in online.sql_analysis.queries[0].tables
    ]


# 15. Existing SQL parser tests remain green (enforced via regression run).
def test_no_header_only_blob_regression():
    assert strip_task_headers_for_sql("") == ""
    assert strip_task_headers_for_sql("# --- TASK: t (/a) ---") == ""


# P10-2 rework: newline representation preservation (LF vs CRLF).
def test_lf_header_free_input_byte_identical():
    code = "SELECT *\nFROM t;\n-- comment\n"
    assert strip_task_headers_for_sql(code) == code


def test_crlf_header_free_input_byte_identical():
    code = "SELECT *\r\nFROM t;\r\n-- comment\r\n"
    assert strip_task_headers_for_sql(code) == code
    assert "\r\n" in strip_task_headers_for_sql(code)
    assert strip_task_headers_for_sql(code).count("\r\n") == 3


def test_lf_header_becomes_blank_line():
    stripped = strip_task_headers_for_sql(ENVELOPE)
    lines = stripped.splitlines()
    assert lines[0] == ""
    assert len(stripped.splitlines()) == len(ENVELOPE.splitlines())
    assert "SELECT *" in stripped


def test_crlf_header_becomes_blank_line_crlf_remains():
    blob = (
        "# --- TASK: task_1 (example.sql) ---\r\n"
        "\r\n"
        "SELECT *\r\n"
        "FROM catalog.schema.table;\r\n"
    )
    stripped = strip_task_headers_for_sql(blob)
    assert stripped.splitlines(keepends=True)[0] == "\r\n"
    assert stripped.count("\r\n") == blob.count("\r\n")
    assert len(stripped.splitlines()) == len(blob.splitlines())
    assert "SELECT *" in stripped
    assert _queries(blob) and _queries(blob)[0].fingerprint == _queries(BARE)[0].fingerprint


def test_multiple_crlf_headers():
    blob = (
        "# --- TASK: one (/a.sql) ---\r\n"
        "SELECT id FROM bronze.orders;\r\n"
        "\r\n"
        "# --- TASK: two (/b.sql) ---\r\n"
        "SELECT id FROM bronze.customers;\r\n"
    )
    stripped = strip_task_headers_for_sql(blob)
    assert stripped.count("\r\n") == blob.count("\r\n")
    assert len(stripped.splitlines()) == len(blob.splitlines())
    queries = _queries(blob)
    assert len(queries) == 2
    assert [t.name for t in queries[0].tables] == ["orders"]
    assert [t.name for t in queries[1].tables] == ["customers"]


def test_hash_content_untouched_with_crlf():
    body = "SELECT '#' AS hash, id FROM t;\r\n"
    assert strip_task_headers_for_sql(body) == body
    assert len(_queries(body)) == 1
