"""Comprehensive Phase 5 Audit Verification Script."""
import sys
from dpif.code.parser import analyze_source
from dpif.sql.parser import SQLParser
from dpif.rules.engine import load_rules, evaluate_rule
from dpif.checkpoints.engine import CheckpointEngine
from dpif.models import PipelineContract, CheckpointStatus, Severity

def audit_standalone_sql():
    print("=== 1. STANDALONE SQL AUDIT ===")
    p = SQLParser()
    r1 = p.parse("SELECT a, b FROM tbl WHERE a > 10")
    print("  Single stmt: queries=", len(r1.analysis.queries), "errors=", len(r1.parse_errors))
    r2 = p.parse("SELECT 1; SELECT 2; SELECT 3 FROM my_table")
    print("  Multi stmt: queries=", len(r2.analysis.queries), "errors=", len(r2.parse_errors))
    r3 = p.parse("")
    print("  Empty: queries=", len(r3.analysis.queries), "errors=", len(r3.parse_errors))
    r4 = p.parse("-- Comment\n/* Block */")
    print("  Comments only: queries=", len(r4.analysis.queries), "errors=", len(r4.parse_errors))
    r5 = p.parse("SELECT FROM WHERE")
    print("  Malformed: queries=", len(r5.analysis.queries), "errors=", len(r5.parse_errors))

def audit_embedded_sql():
    print("\n=== 2. EMBEDDED SQL AUDIT ===")
    c1 = analyze_source('spark.sql("SELECT * FROM tbl")', 's1.py')
    print("  Direct literal:", len(c1.sql_analysis.queries) if c1.sql_analysis else 0)

    s2 = 'spark.sql("""\nSELECT id, sum(val)\nFROM sales\nGROUP BY id\n""")'
    c2 = analyze_source(s2, 's2.py')
    print("  Multiline literal:", len(c2.sql_analysis.queries) if c2.sql_analysis else 0)

    s3 = 'q = "SELECT * FROM users"\nspark.sql(q)'
    c3 = analyze_source(s3, 's3.py')
    print("  String variable:", len(c3.sql_analysis.queries) if c3.sql_analysis else 0)

    s4 = 't = "orders"\nq = f"SELECT * FROM {t}"\nspark.sql(q)'
    c4 = analyze_source(s4, 's4.py')
    print("  F-string variable:", len(c4.sql_analysis.queries) if c4.sql_analysis else 0)

    s5 = 'q = build_query("orders")\nspark.sql(q)'
    c5 = analyze_source(s5, 's5.py')
    print("  Dynamic function call:", len(c5.sql_analysis.queries) if c5.sql_analysis else 0)

def audit_pyspark_sql_coexistence():
    print("\n=== 3. PYTHON + SQL INTEGRATION IN CP-004 ===")
    mixed_code = """
import pyspark.sql.functions as F

df = spark.read.table("sales")
spark.sql("SELECT * FROM invalid_table CROSS JOIN small_table")
df_large = df.crossJoin(spark.read.table("other"))
df_large.collect()
"""
    ca = analyze_source(mixed_code, "mixed.py")
    print("  Code operations (PySpark):", len(ca.operations))
    print("  SQL queries parsed:", len(ca.sql_analysis.queries) if ca.sql_analysis else 0)
    print("  SQL joins detected:", len(ca.sql_analysis.joins) if ca.sql_analysis else 0)

def audit_empirical_matrix():
    print("\n=== 4. EMPIRICAL DATA-CONTEXT MATRIX ===")
    rules = {r.rule_id: r for r in load_rules()}
    rule_join = rules["CODE-SQL-003"]
    rule_agg = rules["CODE-SQL-004"]
    rule_sort = rules["CODE-SQL-005"]
    rule_dedup = rules["CODE-SQL-006"]

    sql = "SELECT a.id, count(DISTINCT a.val) FROM a JOIN b ON a.id = b.id GROUP BY a.id ORDER BY a.id"
    ca = analyze_source(sql, "scale.sql")

    volumes = [0.01, 0.1, 10.0, 100.0, 500.0, 2000.0]
    labels = ["10 MB", "100 MB", "10 GB", "100 GB", "500 GB", "2 TB"]

    print("Volume | Large Join (003) | Large Agg (004) | Global Sort (005) | Distinct (006)")
    print("-" * 75)
    for gb, lbl in zip(volumes, labels):
        ctx = {"data_size_gb": gb}
        f_j = rule_join.evaluate({"sql_analysis": ca.sql_analysis}, ctx)
        f_a = rule_agg.evaluate({"sql_analysis": ca.sql_analysis}, ctx)
        f_s = rule_sort.evaluate({"sql_analysis": ca.sql_analysis}, ctx)
        f_d = rule_dedup.evaluate({"sql_analysis": ca.sql_analysis}, ctx)

        s_j = f"{f_j.status.value}:{f_j.severity.value}" if f_j else "PASS"
        s_a = f"{f_a.status.value}:{f_a.severity.value}" if f_a else "PASS"
        s_s = f"{f_s.status.value}:{f_s.severity.value}" if f_s else "PASS"
        s_d = f"{f_d.status.value}:{f_d.severity.value}" if f_d else "PASS"
        print(f"{lbl:<8} | {s_j:<16} | {s_a:<15} | {s_s:<17} | {s_d:<14}")

def audit_unknown_semantics():
    print("\n=== 5. UNKNOWN SEMANTICS AUDIT ===")
    rules = {r.rule_id: r for r in load_rules()}
    sql = "SELECT a.id, count(DISTINCT a.val) FROM a JOIN b ON a.id = b.id GROUP BY a.id ORDER BY a.id"
    ca = analyze_source(sql, "unknown.sql")
    # Empty context
    for rid in ["CODE-SQL-003", "CODE-SQL-004", "CODE-SQL-005", "CODE-SQL-006"]:
        r = rules[rid]
        f = r.evaluate({"sql_analysis": ca.sql_analysis}, {})
        print(f"  {rid} with empty context -> status: {f.status if f else None}, sev: {f.severity if f else None}, conf: {f.confidence if f else None}, assumptions: {f.assumptions if f else None}")

def audit_threshold_override():
    print("\n=== 6. THRESHOLD OVERRIDE EMPIRICAL AUDIT ===")
    rules = {r.rule_id: r for r in load_rules()}
    r3 = rules["CODE-SQL-003"]
    sql = "SELECT a.id, b.v FROM a JOIN b ON a.id = b.id"
    ca = analyze_source(sql, "override.sql")

    # default large_join_gb is 100.0. Test at 200 GB
    f_default = r3.evaluate({"sql_analysis": ca.sql_analysis}, {"data_size_gb": 200.0})
    print("  Default (100 GB threshold) at 200 GB ->", f_default.severity.value if f_default else "PASS")

    # override threshold to 500.0. Test at 200 GB
    r3_custom = r3.model_copy(update={"params": {"large_join_gb": 500.0, "high_gb": 2000.0}})
    f_custom = r3_custom.evaluate({"sql_analysis": ca.sql_analysis}, {"data_size_gb": 200.0})
    print("  Custom (500 GB threshold) at 200 GB ->", f_custom.severity.value if f_custom else "PASS")

if __name__ == "__main__":
    audit_standalone_sql()
    audit_embedded_sql()
    audit_pyspark_sql_coexistence()
    audit_empirical_matrix()
    audit_unknown_semantics()
    audit_threshold_override()
