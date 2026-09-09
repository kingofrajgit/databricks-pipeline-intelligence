"""Tests for SQL Intelligence (Phase 5).

Verifies CODE-SQL-001 through CODE-SQL-010 across positive, negative,
edge cases, false-positive resistance, data volume contexts, embedded
spark.sql(), SQL in variables, and CP-004 checkpoint integration.
"""

from __future__ import annotations

import pytest

from dpif.checkpoints.definitions import cp004_code
from dpif.checkpoints.engine import CheckpointEngine
from dpif.code.parser import analyze_source
from dpif.models import CheckpointStatus, Severity
from dpif.rules.engine import load_rules


@pytest.fixture(scope="module")
def rules():
    """Load all rules indexed by rule_id."""
    return {r.rule_id: r for r in load_rules()}


# ====================================================================
# CODE-SQL-001: SELECT * Risk
# ====================================================================
class TestSql001SelectStar:
    def test_positive_select_star(self, rules):
        rule = rules["CODE-SQL-001"]
        sql = "SELECT * FROM bronze_events"
        code = analyze_source(sql, "query.sql")
        finding = rule.evaluate({"sql_analysis": code.sql_analysis, "source_file": "query.sql"}, {})
        assert finding is not None
        assert finding.status == CheckpointStatus.WARN
        assert "SELECT *" in finding.recommendation or "wildcard" in finding.recommendation.lower()

    def test_positive_table_star(self, rules):
        rule = rules["CODE-SQL-001"]
        sql = "SELECT t1.*, t2.id FROM t1 JOIN t2 ON t1.id = t2.id"
        code = analyze_source(sql, "query.sql")
        finding = rule.evaluate({"sql_analysis": code.sql_analysis, "source_file": "query.sql"}, {})
        assert finding is not None
        assert "t1" in finding.evidence.observed.get("operations", [{}])[0].get(
            "wildcard_tables", []
        )

    def test_negative_explicit_columns(self, rules):
        rule = rules["CODE-SQL-001"]
        sql = "SELECT id, user_name, created_at FROM bronze_events"
        code = analyze_source(sql, "query.sql")
        finding = rule.evaluate({"sql_analysis": code.sql_analysis, "source_file": "query.sql"}, {})
        assert finding is None

    def test_edge_count_star_not_flagged_as_projection(self, rules):
        rule = rules["CODE-SQL-001"]
        sql = "SELECT count(*) FROM bronze_events"
        code = analyze_source(sql, "query.sql")
        finding = rule.evaluate({"sql_analysis": code.sql_analysis, "source_file": "query.sql"}, {})
        assert finding is None

    def test_false_positive_comment_containing_star(self, rules):
        rule = rules["CODE-SQL-001"]
        sql = "-- SELECT * FROM old_table\nSELECT id, name FROM new_table"
        code = analyze_source(sql, "query.sql")
        finding = rule.evaluate({"sql_analysis": code.sql_analysis, "source_file": "query.sql"}, {})
        assert finding is None


# ====================================================================
# CODE-SQL-002: Cross Join Risk
# ====================================================================
class TestSql002CrossJoin:
    def test_positive_cross_join(self, rules):
        rule = rules["CODE-SQL-002"]
        sql = "SELECT a.id, b.val FROM tbl_a a CROSS JOIN tbl_b b"
        code = analyze_source(sql, "cross.sql")
        finding = rule.evaluate({"sql_analysis": code.sql_analysis, "source_file": "cross.sql"}, {})
        assert finding is not None
        assert finding.status == CheckpointStatus.FAIL
        assert finding.severity == Severity.HIGH
        assert finding.blocking is True
        assert "CROSS JOIN" in finding.recommendation

    def test_positive_comma_cross_join(self, rules):
        rule = rules["CODE-SQL-002"]
        sql = "SELECT a.id, b.val FROM tbl_a a, tbl_b b"
        code = analyze_source(sql, "cross_comma.sql")
        finding = rule.evaluate(
            {"sql_analysis": code.sql_analysis, "source_file": "cross_comma.sql"}, {}
        )
        assert finding is not None
        assert finding.status == CheckpointStatus.FAIL

    def test_negative_keyed_inner_join(self, rules):
        rule = rules["CODE-SQL-002"]
        sql = "SELECT a.id, b.val FROM tbl_a a INNER JOIN tbl_b b ON a.id = b.id"
        code = analyze_source(sql, "join.sql")
        finding = rule.evaluate({"sql_analysis": code.sql_analysis, "source_file": "join.sql"}, {})
        assert finding is None

    def test_negative_left_join(self, rules):
        rule = rules["CODE-SQL-002"]
        sql = "SELECT a.id, b.val FROM tbl_a a LEFT JOIN tbl_b b ON a.id = b.id"
        code = analyze_source(sql, "left.sql")
        finding = rule.evaluate({"sql_analysis": code.sql_analysis, "source_file": "left.sql"}, {})
        assert finding is None

    def test_false_positive_column_or_table_named_cross(self, rules):
        rule = rules["CODE-SQL-002"]
        sql = "SELECT cross_street, cross_road FROM intersections WHERE cross_street = 'Main'"
        code = analyze_source(sql, "cross_names.sql")
        finding = rule.evaluate(
            {"sql_analysis": code.sql_analysis, "source_file": "cross_names.sql"}, {}
        )
        assert finding is None


# ====================================================================
# CODE-SQL-003: Potential Large Join
# ====================================================================
class TestSql003LargeJoin:
    def test_positive_large_join(self, rules):
        rule = rules["CODE-SQL-003"]
        sql = "SELECT a.id, b.v FROM tbl_a a JOIN tbl_b b ON a.id = b.id"
        code = analyze_source(sql, "join.sql")
        finding = rule.evaluate(
            {"sql_analysis": code.sql_analysis, "source_file": "join.sql"},
            {"data_size_gb": 500.0},
        )
        assert finding is not None
        assert finding.status == CheckpointStatus.FAIL or finding.severity in (
            Severity.MEDIUM,
            Severity.HIGH,
        )
        assert "shuffle" in finding.recommendation.lower()

    def test_positive_high_volume_escalates_to_high(self, rules):
        rule = rules["CODE-SQL-003"]
        sql = "SELECT a.id, b.v FROM tbl_a a JOIN tbl_b b ON a.id = b.id"
        code = analyze_source(sql, "join.sql")
        finding = rule.evaluate(
            {"sql_analysis": code.sql_analysis, "source_file": "join.sql"},
            {"data_size_gb": 2000.0},
        )
        assert finding is not None
        assert finding.severity == Severity.HIGH

    def test_negative_small_volume(self, rules):
        rule = rules["CODE-SQL-003"]
        sql = "SELECT a.id, b.v FROM tbl_a a JOIN tbl_b b ON a.id = b.id"
        code = analyze_source(sql, "join.sql")
        finding = rule.evaluate(
            {"sql_analysis": code.sql_analysis, "source_file": "join.sql"},
            {"data_size_gb": 0.05},  # 50 MB
        )
        assert finding is None

    def test_negative_no_joins_present(self, rules):
        rule = rules["CODE-SQL-003"]
        sql = "SELECT id, v FROM tbl_a WHERE id > 10"
        code = analyze_source(sql, "no_join.sql")
        finding = rule.evaluate(
            {"sql_analysis": code.sql_analysis, "source_file": "no_join.sql"},
            {"data_size_gb": 500.0},
        )
        assert finding is None

    def test_unknown_context_downgrades_to_qualified_warning(self, rules):
        rule = rules["CODE-SQL-003"]
        sql = "SELECT a.id, b.v FROM tbl_a a JOIN tbl_b b ON a.id = b.id"
        code = analyze_source(sql, "join.sql")
        finding = rule.evaluate(
            {"sql_analysis": code.sql_analysis, "source_file": "join.sql"},
            {},
        )
        assert finding is not None
        assert finding.status == CheckpointStatus.WARN
        assert "context-unavailable" in finding.assumptions


# ====================================================================
# CODE-SQL-004: Potential Large Aggregation
# ====================================================================
class TestSql004LargeAggregation:
    def test_positive_large_group_by(self, rules):
        rule = rules["CODE-SQL-004"]
        sql = "SELECT category, sum(amount) FROM transactions GROUP BY category"
        code = analyze_source(sql, "agg.sql")
        finding = rule.evaluate(
            {"sql_analysis": code.sql_analysis, "source_file": "agg.sql"},
            {"data_size_gb": 300.0},
        )
        assert finding is not None
        assert "aggregation" in finding.recommendation.lower()

    def test_positive_global_agg_large_volume(self, rules):
        rule = rules["CODE-SQL-004"]
        sql = "SELECT avg(amount), max(latency) FROM metrics"
        code = analyze_source(sql, "agg2.sql")
        finding = rule.evaluate(
            {"sql_analysis": code.sql_analysis, "source_file": "agg2.sql"},
            {"data_size_gb": 1200.0},
        )
        assert finding is not None
        assert finding.severity == Severity.HIGH

    def test_negative_small_volume(self, rules):
        rule = rules["CODE-SQL-004"]
        sql = "SELECT category, count(*) FROM lookup GROUP BY category"
        code = analyze_source(sql, "agg_small.sql")
        finding = rule.evaluate(
            {"sql_analysis": code.sql_analysis, "source_file": "agg_small.sql"},
            {"data_size_gb": 0.01},  # 10 MB
        )
        assert finding is None

    def test_negative_no_aggregations(self, rules):
        rule = rules["CODE-SQL-004"]
        sql = "SELECT id, category FROM transactions WHERE id > 10"
        code = analyze_source(sql, "plain.sql")
        finding = rule.evaluate(
            {"sql_analysis": code.sql_analysis, "source_file": "plain.sql"},
            {"data_size_gb": 500.0},
        )
        assert finding is None

    def test_unknown_context_qualified_warning(self, rules):
        rule = rules["CODE-SQL-004"]
        sql = "SELECT category, sum(amount) FROM transactions GROUP BY category"
        code = analyze_source(sql, "agg.sql")
        finding = rule.evaluate(
            {"sql_analysis": code.sql_analysis, "source_file": "agg.sql"},
            {},
        )
        assert finding is not None
        assert finding.status == CheckpointStatus.WARN
        assert "context-unavailable" in finding.assumptions


# ====================================================================
# CODE-SQL-005: Global ORDER BY Risk
# ====================================================================
class TestSql005GlobalSort:
    def test_positive_large_global_sort(self, rules):
        rule = rules["CODE-SQL-005"]
        sql = "SELECT id, score FROM results ORDER BY score DESC"
        code = analyze_source(sql, "sort.sql")
        finding = rule.evaluate(
            {"sql_analysis": code.sql_analysis, "source_file": "sort.sql"},
            {"data_size_gb": 250.0},
        )
        assert finding is not None
        assert "sort" in finding.recommendation.lower()

    def test_positive_very_large_sort_escalation(self, rules):
        rule = rules["CODE-SQL-005"]
        sql = "SELECT * FROM big_table ORDER BY timestamp"
        code = analyze_source(sql, "sort_huge.sql")
        finding = rule.evaluate(
            {"sql_analysis": code.sql_analysis, "source_file": "sort_huge.sql"},
            {"data_size_gb": 1500.0},
        )
        assert finding is not None
        assert finding.severity == Severity.HIGH

    def test_negative_small_volume_sort(self, rules):
        rule = rules["CODE-SQL-005"]
        sql = "SELECT id, score FROM results ORDER BY score DESC"
        code = analyze_source(sql, "sort_small.sql")
        finding = rule.evaluate(
            {"sql_analysis": code.sql_analysis, "source_file": "sort_small.sql"},
            {"data_size_gb": 1.0},
        )
        assert finding is None

    def test_negative_no_order_by(self, rules):
        rule = rules["CODE-SQL-005"]
        sql = "SELECT id, score FROM results WHERE score > 50"
        code = analyze_source(sql, "nosort.sql")
        finding = rule.evaluate(
            {"sql_analysis": code.sql_analysis, "source_file": "nosort.sql"},
            {"data_size_gb": 500.0},
        )
        assert finding is None

    def test_unknown_context_yields_warning(self, rules):
        rule = rules["CODE-SQL-005"]
        sql = "SELECT id, score FROM results ORDER BY score DESC"
        code = analyze_source(sql, "sort.sql")
        finding = rule.evaluate(
            {"sql_analysis": code.sql_analysis, "source_file": "sort.sql"},
            {},
        )
        assert finding is not None
        assert finding.status == CheckpointStatus.WARN


# ====================================================================
# CODE-SQL-006: Expensive DISTINCT
# ====================================================================
class TestSql006ExpensiveDistinct:
    def test_positive_select_distinct_large(self, rules):
        rule = rules["CODE-SQL-006"]
        sql = "SELECT DISTINCT user_id, device_id FROM activity_logs"
        code = analyze_source(sql, "distinct.sql")
        finding = rule.evaluate(
            {"sql_analysis": code.sql_analysis, "source_file": "distinct.sql"},
            {"data_size_gb": 200.0},
        )
        assert finding is not None
        assert "deduplication" in finding.recommendation.lower()

    def test_positive_count_distinct_large(self, rules):
        rule = rules["CODE-SQL-006"]
        sql = "SELECT count(DISTINCT user_id) FROM web_events"
        code = analyze_source(sql, "count_distinct.sql")
        finding = rule.evaluate(
            {"sql_analysis": code.sql_analysis, "source_file": "count_distinct.sql"},
            {"data_size_gb": 350.0},
        )
        assert finding is not None

    def test_negative_small_volume_distinct(self, rules):
        rule = rules["CODE-SQL-006"]
        sql = "SELECT DISTINCT status FROM order_status_lookup"
        code = analyze_source(sql, "small_distinct.sql")
        finding = rule.evaluate(
            {"sql_analysis": code.sql_analysis, "source_file": "small_distinct.sql"},
            {"data_size_gb": 0.05},
        )
        assert finding is None

    def test_negative_no_distinct(self, rules):
        rule = rules["CODE-SQL-006"]
        sql = "SELECT user_id, count(order_id) FROM orders GROUP BY user_id"
        code = analyze_source(sql, "no_distinct.sql")
        finding = rule.evaluate(
            {"sql_analysis": code.sql_analysis, "source_file": "no_distinct.sql"},
            {"data_size_gb": 500.0},
        )
        assert finding is None

    def test_unknown_context_warns(self, rules):
        rule = rules["CODE-SQL-006"]
        sql = "SELECT DISTINCT user_id FROM activity_logs"
        code = analyze_source(sql, "distinct.sql")
        finding = rule.evaluate(
            {"sql_analysis": code.sql_analysis, "source_file": "distinct.sql"},
            {},
        )
        assert finding is not None
        assert finding.status == CheckpointStatus.WARN


# ====================================================================
# CODE-SQL-007: Window Operation Risk
# ====================================================================
class TestSql007WindowRisk:
    def test_positive_unbounded_window(self, rules):
        rule = rules["CODE-SQL-007"]
        sql = "SELECT id, avg(val) OVER (PARTITION BY dept) FROM t"
        code = analyze_source(sql, "window.sql")
        finding = rule.evaluate(
            {"sql_analysis": code.sql_analysis, "source_file": "window.sql"}, {}
        )
        assert finding is not None
        assert finding.status == CheckpointStatus.WARN
        assert "window" in finding.recommendation.lower()

    def test_negative_explicit_rows_between_frame(self, rules):
        rule = rules["CODE-SQL-007"]
        sql = """
        SELECT id, avg(val) OVER (
            PARTITION BY dept ORDER BY dt
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) FROM t
        """
        code = analyze_source(sql, "bounded_window.sql")
        finding = rule.evaluate(
            {"sql_analysis": code.sql_analysis, "source_file": "bounded_window.sql"}, {}
        )
        assert finding is None

    def test_negative_range_between_frame(self, rules):
        rule = rules["CODE-SQL-007"]
        sql = """
        SELECT id, sum(val) OVER (
            PARTITION BY dept ORDER BY dt
            RANGE BETWEEN INTERVAL 7 DAYS PRECEDING AND CURRENT ROW
        ) FROM t
        """
        code = analyze_source(sql, "bounded_range.sql")
        finding = rule.evaluate(
            {"sql_analysis": code.sql_analysis, "source_file": "bounded_range.sql"}, {}
        )
        assert finding is None

    def test_negative_no_windows_at_all(self, rules):
        rule = rules["CODE-SQL-007"]
        sql = "SELECT id, sum(val) FROM t GROUP BY id"
        code = analyze_source(sql, "nowindow.sql")
        finding = rule.evaluate(
            {"sql_analysis": code.sql_analysis, "source_file": "nowindow.sql"}, {}
        )
        assert finding is None

    def test_multiple_windows_catches_unbounded(self, rules):
        rule = rules["CODE-SQL-007"]
        sql = """
        SELECT
            id,
            sum(val) OVER (
                PARTITION BY dept ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
            ) AS bounded_val,
            avg(val) OVER (PARTITION BY dept) AS unbounded_val
        FROM t
        """
        code = analyze_source(sql, "mixed_windows.sql")
        finding = rule.evaluate(
            {"sql_analysis": code.sql_analysis, "source_file": "mixed_windows.sql"}, {}
        )
        assert finding is not None
        assert finding.evidence.observed["operations"][0]["bounded"] is False


# ====================================================================
# CODE-SQL-008: Function on Filter Column
# ====================================================================
class TestSql008FunctionOnFilter:
    def test_positive_upper_on_column(self, rules):
        rule = rules["CODE-SQL-008"]
        sql = "SELECT * FROM customers WHERE UPPER(city) = 'NEW YORK'"
        code = analyze_source(sql, "func_filter.sql")
        finding = rule.evaluate(
            {"sql_analysis": code.sql_analysis, "source_file": "func_filter.sql"}, {}
        )
        assert finding is not None
        assert "pruning" in finding.recommendation.lower()

    def test_positive_date_trunc_on_timestamp(self, rules):
        rule = rules["CODE-SQL-008"]
        sql = "SELECT * FROM events WHERE DATE_TRUNC('month', event_time) = '2024-01-01'"
        code = analyze_source(sql, "date_filter.sql")
        finding = rule.evaluate(
            {"sql_analysis": code.sql_analysis, "source_file": "date_filter.sql"}, {}
        )
        assert finding is not None

    def test_negative_raw_column_comparison(self, rules):
        rule = rules["CODE-SQL-008"]
        sql = "SELECT * FROM customers WHERE city = 'New York' AND age >= 21"
        code = analyze_source(sql, "clean_filter.sql")
        finding = rule.evaluate(
            {"sql_analysis": code.sql_analysis, "source_file": "clean_filter.sql"}, {}
        )
        assert finding is None

    def test_negative_function_in_select_list_only(self, rules):
        rule = rules["CODE-SQL-008"]
        sql = (
            "SELECT UPPER(city), count(*) FROM customers WHERE country = 'USA' GROUP BY UPPER(city)"
        )
        code = analyze_source(sql, "func_select.sql")
        finding = rule.evaluate(
            {"sql_analysis": code.sql_analysis, "source_file": "func_select.sql"}, {}
        )
        assert finding is None

    def test_edge_cast_on_filter_column(self, rules):
        rule = rules["CODE-SQL-008"]
        sql = "SELECT * FROM logs WHERE CAST(id AS STRING) = '12345'"
        code = analyze_source(sql, "cast_filter.sql")
        finding = rule.evaluate(
            {"sql_analysis": code.sql_analysis, "source_file": "cast_filter.sql"}, {}
        )
        assert finding is not None


# ====================================================================
# CODE-SQL-009: Excessive Query Complexity
# ====================================================================
class TestSql009Complexity:
    def test_positive_too_many_joins(self, rules):
        rule = rules["CODE-SQL-009"]
        sql = """
        SELECT *
        FROM t0
        JOIN t1 ON t0.id = t1.id
        JOIN t2 ON t1.id = t2.id
        JOIN t3 ON t2.id = t3.id
        JOIN t4 ON t3.id = t4.id
        JOIN t5 ON t4.id = t5.id
        JOIN t6 ON t5.id = t6.id
        JOIN t7 ON t6.id = t7.id
        JOIN t8 ON t7.id = t8.id
        """
        code = analyze_source(sql, "huge_join.sql")
        finding = rule.evaluate(
            {"sql_analysis": code.sql_analysis, "source_file": "huge_join.sql"}, {}
        )
        assert finding is not None
        assert "join" in finding.evidence.evidence[0].lower()

    def test_positive_too_many_ctes(self, rules):
        rule = rules["CODE-SQL-009"]
        cte_defs = ", ".join(f"c{i} AS (SELECT {i} AS n)" for i in range(12))
        sql = f"WITH {cte_defs} SELECT * FROM c0"
        code = analyze_source(sql, "many_ctes.sql")
        finding = rule.evaluate(
            {"sql_analysis": code.sql_analysis, "source_file": "many_ctes.sql"}, {}
        )
        assert finding is not None
        assert "cte" in finding.evidence.evidence[0].lower()

    def test_negative_simple_query(self, rules):
        rule = rules["CODE-SQL-009"]
        sql = "SELECT a.id, b.name FROM a JOIN b ON a.id = b.id WHERE a.status = 'ACTIVE'"
        code = analyze_source(sql, "simple.sql")
        finding = rule.evaluate(
            {"sql_analysis": code.sql_analysis, "source_file": "simple.sql"}, {}
        )
        assert finding is None

    def test_negative_moderate_ctes(self, rules):
        rule = rules["CODE-SQL-009"]
        sql = (
            "WITH c1 AS (SELECT 1 AS a), c2 AS (SELECT 2 AS b) "
            "SELECT * FROM c1 JOIN c2 ON c1.a = c2.b"
        )
        code = analyze_source(sql, "moderate.sql")
        finding = rule.evaluate(
            {"sql_analysis": code.sql_analysis, "source_file": "moderate.sql"}, {}
        )
        assert finding is None

    def test_custom_threshold_override(self, rules):
        rule = rules["CODE-SQL-009"]
        sql = "SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id"
        code = analyze_source(sql, "custom.sql")
        # Default max_joins=7: should not trigger
        f_default = rule.evaluate(
            {"sql_analysis": code.sql_analysis, "source_file": "custom.sql"}, {}
        )
        assert f_default is None

        # Custom override: max_joins=1 should trigger
        rule_copy = rule.model_copy(update={"params": {"max_joins": 1}})
        f_custom = rule_copy.evaluate(
            {"sql_analysis": code.sql_analysis, "source_file": "custom.sql"}, {}
        )
        assert f_custom is not None


# ====================================================================
# CODE-SQL-010: SQL Parse Error
# ====================================================================
class TestSql010ParseError:
    def test_positive_unparseable_sql_is_blocking_critical(self, rules):
        rule = rules["CODE-SQL-010"]
        sql = "SELECT ,,, FROM FROM WHERE"
        code = analyze_source(sql, "malformed.sql")
        finding = rule.evaluate(
            {"sql_analysis": code.sql_analysis, "source_file": "malformed.sql"}, {}
        )
        assert finding is not None
        assert finding.status == CheckpointStatus.FAIL
        assert finding.severity == Severity.CRITICAL
        assert finding.blocking is True
        assert "syntax error" in finding.recommendation.lower()

    def test_negative_valid_complex_sql(self, rules):
        rule = rules["CODE-SQL-010"]
        sql = """
        WITH cte AS (
            SELECT dept, avg(salary) as avg_sal
            FROM employees
            GROUP BY dept
        )
        SELECT e.name, e.salary, c.avg_sal
        FROM employees e
        JOIN cte c ON e.dept = c.dept
        WHERE e.salary > c.avg_sal
        """
        code = analyze_source(sql, "valid.sql")
        finding = rule.evaluate({"sql_analysis": code.sql_analysis, "source_file": "valid.sql"}, {})
        assert finding is None

    def test_edge_empty_query(self, rules):
        rule = rules["CODE-SQL-010"]
        sql = ""
        code = analyze_source(sql, "empty.sql")
        finding = rule.evaluate({"sql_analysis": code.sql_analysis, "source_file": "empty.sql"}, {})
        assert finding is None

    def test_edge_comment_only_query(self, rules):
        rule = rules["CODE-SQL-010"]
        sql = "-- Just a SQL comment\n/* Multi-line\ncomment */"
        code = analyze_source(sql, "comment.sql")
        finding = rule.evaluate(
            {"sql_analysis": code.sql_analysis, "source_file": "comment.sql"}, {}
        )
        assert finding is None

    def test_parse_error_carries_exact_line_location(self, rules):
        rule = rules["CODE-SQL-010"]
        sql = "SELECT id FROM valid_tbl;\nSELECT * FROM ("
        code = analyze_source(sql, "multi.sql")
        finding = rule.evaluate({"sql_analysis": code.sql_analysis, "source_file": "multi.sql"}, {})
        assert finding is not None
        assert finding.blocking is True
        assert any("Line" in e for e in finding.evidence.evidence)


# ====================================================================
# Integration: Python Embedded SQL, Variables & Checkpoint CP-004
# ====================================================================
class TestSqlIntegration:
    def test_embedded_spark_sql_literal(self, rules):
        py_code = (
            "from pyspark.sql import SparkSession\n"
            "spark = SparkSession.builder.getOrCreate()\n"
            'df = spark.sql("SELECT a.id, b.name FROM tbl_a a CROSS JOIN tbl_b b")\n'
            'df.write.parquet("dbfs:/output")\n'
        )
        code = analyze_source(py_code, "pipeline.py")
        assert code.sql_analysis is not None

        finding = rules["CODE-SQL-002"].evaluate(
            {"sql_analysis": code.sql_analysis, "source_file": "pipeline.py"},
            {},
        )
        assert finding is not None
        assert finding.blocking is True

    def test_sql_in_python_variable(self, rules):
        py_code = 'query = """SELECT * FROM delta_table WHERE id > 100"""\ndf = spark.sql(query)\n'
        code = analyze_source(py_code, "etl.py")
        assert code.sql_analysis is not None

        finding = rules["CODE-SQL-001"].evaluate(
            {"sql_analysis": code.sql_analysis, "source_file": "etl.py"},
            {},
        )
        assert finding is not None
        assert finding.status == CheckpointStatus.WARN

    def test_multiple_spark_sql_calls_in_one_file(self, rules):
        py_code = (
            'df1 = spark.sql("SELECT * FROM table1")\n'
            'df2 = spark.sql("SELECT a.id, b.val FROM table1 a CROSS JOIN table2 b")\n'
        )
        code = analyze_source(py_code, "multi_sql.py")
        assert code.sql_analysis is not None
        assert len(code.sql_analysis.queries) == 2

        f1 = rules["CODE-SQL-001"].evaluate(
            {"sql_analysis": code.sql_analysis, "source_file": "multi_sql.py"},
            {},
        )
        assert f1 is not None

        f2 = rules["CODE-SQL-002"].evaluate(
            {"sql_analysis": code.sql_analysis, "source_file": "multi_sql.py"},
            {},
        )
        assert f2 is not None

    def test_checkpoint_cp004_with_blocking_sql_fails_checkpoint(self):
        engine = CheckpointEngine()
        code = "SELECT a.id, b.val FROM a CROSS JOIN b"
        context = {
            "code_snippet": code,
            "code_filename": "cross.sql",
            "pipeline_name": "test_pipeline",
        }
        cp = cp004_code(code)
        cp004 = engine.execute_checkpoint(cp, context)
        assert cp004.status == CheckpointStatus.FAIL

        findings = engine.findings_by_checkpoint.get("CP-004", [])
        assert any(f.rule_id == "CODE-SQL-002" and f.blocking for f in findings)

    def test_checkpoint_cp004_clean_sql_passes_checkpoint(self):
        engine = CheckpointEngine()
        code = "SELECT a.id, b.name FROM a JOIN b ON a.id = b.id WHERE a.id > 10"
        context = {
            "code_snippet": code,
            "code_filename": "clean.sql",
            "pipeline_name": "clean_pipeline",
            "rule_context": {"data_size_gb": 1.0},
        }
        cp = cp004_code(code)
        cp004 = engine.execute_checkpoint(cp, context)
        assert cp004.status == CheckpointStatus.PASS
        findings = engine.findings_by_checkpoint.get("CP-004", [])
        assert len(findings) == 0

    def test_empirical_volume_matrix_across_scales(self, rules):
        """Verify empirical context matrix across 10 MB, 1 GB, 50 GB, 500 GB, 2 TB."""
        rule_join = rules["CODE-SQL-003"]
        sql = "SELECT a.id, b.name FROM a JOIN b ON a.id = b.id"
        code = analyze_source(sql, "scale.sql")

        # 10 MB (0.01 GB) -> PASS (no finding)
        assert (
            rule_join.evaluate({"sql_analysis": code.sql_analysis}, {"data_size_gb": 0.01}) is None
        )
        # 1 GB -> PASS
        assert (
            rule_join.evaluate({"sql_analysis": code.sql_analysis}, {"data_size_gb": 1.0}) is None
        )
        # 50 GB -> PASS (threshold is 100 GB)
        assert (
            rule_join.evaluate({"sql_analysis": code.sql_analysis}, {"data_size_gb": 50.0}) is None
        )
        # 500 GB -> Triggers MEDIUM
        f_500 = rule_join.evaluate({"sql_analysis": code.sql_analysis}, {"data_size_gb": 500.0})
        assert f_500 is not None
        assert f_500.severity == Severity.MEDIUM
        # 2 TB (2000 GB) -> Escalates to HIGH
        f_2000 = rule_join.evaluate({"sql_analysis": code.sql_analysis}, {"data_size_gb": 2000.0})
        assert f_2000 is not None
        assert f_2000.severity == Severity.HIGH
