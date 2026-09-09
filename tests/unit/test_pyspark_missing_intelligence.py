"""Phase 4 missing PySpark intelligence tests.

Tests the 4 newly wired contextual evaluators and YAML rules:
- CODE-PYSPARK-016: Broadcast Join Risk (analyze_broadcast)
- CODE-PYSPARK-017: UDF Overhead Risk (analyze_udfs)
- CODE-PYSPARK-018: Inefficient Count for Existence Check (analyze_count_for_existence)
- CODE-PYSPARK-019: Cartesian Cross Join Risk (analyze_cross_join)

Each rule includes 5 tests: positive, negative, edge, false-positive, and data-context.
"""

from __future__ import annotations

from dpif.models import CheckpointStatus
from dpif.rules.engine import evaluate_rule


def _rule(rules_by_id, rule_id):
    assert rule_id in rules_by_id, f"{rule_id} not loaded"
    rule = rules_by_id[rule_id]
    assert rule.evaluator, f"{rule_id} must declare an evaluator"
    return rule


def _data(code_dir, name, **kw):
    base = {
        "text": (code_dir / name).read_text(encoding="utf-8"),
        "source_file": name,
        "pipeline_name": "p4_test",
        "assumptions": {},
    }
    base.update(kw)
    return base


def _snippet(code_text, filename="snippet.py", **kw):
    base = {
        "text": code_text,
        "source_file": filename,
        "pipeline_name": "p4_test",
        "assumptions": {},
    }
    base.update(kw)
    return base


def _ctx(gb=None, **kw):
    ctx = {}
    if gb is not None:
        ctx["data_size_gb"] = gb
    ctx.update(kw)
    return ctx


# ------------------------------------------------------------- 016 Broadcast Risk
class TestBroadcastRisk:
    RULE = "CODE-PYSPARK-016"

    def test_positive_large_broadcast(self, rules_by_id, code_dir):
        r = _rule(rules_by_id, self.RULE)
        f = evaluate_rule(r, _data(code_dir, "broadcast_unknown.py"), _ctx(200.0))
        assert f is not None
        assert f.status == CheckpointStatus.FAIL
        assert f.severity.value == "HIGH"

    def test_negative_no_broadcast(self, rules_by_id, code_dir):
        r = _rule(rules_by_id, self.RULE)
        f = evaluate_rule(r, _data(code_dir, "optimized_pipeline.py"), _ctx(50.0))
        assert f is None

    def test_edge_broadcast_unknown_volume(self, rules_by_id, code_dir):
        r = _rule(rules_by_id, self.RULE)
        f = evaluate_rule(r, _data(code_dir, "broadcast_unknown.py"), _ctx())
        assert f is not None
        assert f.status == CheckpointStatus.WARN
        assert f.confidence < 0.6

    def test_false_positive_comment_or_string(self, rules_by_id):
        r = _rule(rules_by_id, self.RULE)
        code = (
            "# broadcast(df) is recommended for small lookup tables\n"
            'msg = "broadcast mode enabled"\n'
        )
        f = evaluate_rule(r, _snippet(code), _ctx(500.0))
        assert f is None

    def test_context_small_vs_huge(self, rules_by_id, code_dir):
        r = _rule(rules_by_id, self.RULE)
        small = evaluate_rule(r, _data(code_dir, "broadcast_small.py"), _ctx(5.0))
        huge = evaluate_rule(r, _data(code_dir, "broadcast_small.py"), _ctx(500.0))
        # 5 GB is under broadcast_max_gb (10.0), so it does not trigger a warning
        assert small is None
        # 500 GB triggers FAIL
        assert huge is not None and huge.status == CheckpointStatus.FAIL


# ------------------------------------------------------------- 017 UDF Risk
class TestUdfRisk:
    RULE = "CODE-PYSPARK-017"

    def test_positive_python_udf_large_volume(self, rules_by_id):
        r = _rule(rules_by_id, self.RULE)
        code = (
            "from pyspark.sql.functions import udf\n"
            "from pyspark.sql.types import StringType\n"
            "@udf(returnType=StringType())\n"
            "def normalize(val):\n"
            "    return val.strip().lower() if val else None\n"
            "df = spark.read.table('raw_events')\n"
            "out = df.withColumn('clean', normalize(df.text))\n"
        )
        f = evaluate_rule(r, _snippet(code), _ctx(200.0))
        assert f is not None
        assert f.status == CheckpointStatus.FAIL
        assert f.severity.value == "HIGH"
        assert "row-wise serialization" in f.recommendation

    def test_negative_native_expressions(self, rules_by_id, code_dir):
        r = _rule(rules_by_id, self.RULE)
        f = evaluate_rule(r, _data(code_dir, "optimized_pipeline.py"), _ctx(100.0))
        assert f is None

    def test_edge_pandas_udf_vectorized(self, rules_by_id):
        r = _rule(rules_by_id, self.RULE)
        code = (
            "from pyspark.sql.functions import pandas_udf\n"
            "import pandas as pd\n"
            "@pandas_udf('double')\n"
            "def add_one(s: pd.Series) -> pd.Series:\n"
            "    return s + 1\n"
            "df = spark.read.table('metrics')\n"
            "out = df.withColumn('inc', add_one(df.val))\n"
        )
        f = evaluate_rule(r, _snippet(code), _ctx(500.0))
        assert f is not None
        assert f.status == CheckpointStatus.WARN
        assert "Pandas UDF detected" in f.recommendation

    def test_false_positive_name_without_call(self, rules_by_id):
        r = _rule(rules_by_id, self.RULE)
        code = "udf_enabled = False\ndef process_data(df):\n    return df.filter(df.id > 0)\n"
        f = evaluate_rule(r, _snippet(code), _ctx(100.0))
        assert f is None

    def test_context_udf_small_vs_large(self, rules_by_id):
        r = _rule(rules_by_id, self.RULE)
        code = (
            "from pyspark.sql.functions import udf\n"
            "@udf\n"
            "def fn(x):\n"
            "    return x\n"
            "df = spark.read.table('t')\n"
            "df2 = df.withColumn('x', fn(df.x))\n"
        )
        small = evaluate_rule(r, _snippet(code), _ctx(5.0))
        large = evaluate_rule(r, _snippet(code), _ctx(250.0))
        assert small is not None and small.status == CheckpointStatus.WARN
        assert large is not None and large.status == CheckpointStatus.FAIL


# ------------------------------------------------------------- 018 Count For Existence
class TestCountForExistence:
    RULE = "CODE-PYSPARK-018"

    def test_positive_count_comparison(self, rules_by_id, code_dir):
        r = _rule(rules_by_id, self.RULE)
        f = evaluate_rule(r, _data(code_dir, "count_for_existence.py"), _ctx(50.0))
        assert f is not None
        assert f.status == CheckpointStatus.WARN
        assert "isEmpty()" in f.recommendation

    def test_negative_is_empty_or_plain_count(self, rules_by_id):
        r = _rule(rules_by_id, self.RULE)
        # Plain metric counting (not an existence check)
        code = (
            "df = spark.read.table('events')\n"
            "total_rows = df.count()\n"
            "metrics.save(total_rows)\n"
            "if not df.isEmpty():\n"
            "    df.write.save('out')\n"
        )
        f = evaluate_rule(r, _snippet(code), _ctx(100.0))
        assert f is None

    def test_edge_if_count_boolean_condition(self, rules_by_id):
        r = _rule(rules_by_id, self.RULE)
        code = "df = spark.read.table('events')\nif df.count():\n    df.write.save('out')\n"
        f = evaluate_rule(r, _snippet(code), _ctx(50.0))
        assert f is not None
        assert f.status == CheckpointStatus.WARN

    def test_false_positive_variable_named_count(self, rules_by_id):
        r = _rule(rules_by_id, self.RULE)
        code = "count = 10\nif count > 0:\n    print('positive count')\n"
        f = evaluate_rule(r, _snippet(code), _ctx(50.0))
        assert f is None

    def test_context_large_volume_escalates(self, rules_by_id, code_dir):
        r = _rule(rules_by_id, self.RULE)
        small = evaluate_rule(r, _data(code_dir, "count_for_existence.py"), _ctx(5.0))
        large = evaluate_rule(r, _data(code_dir, "count_for_existence.py"), _ctx(250.0))
        assert small is not None and small.status == CheckpointStatus.WARN
        assert large is not None and large.status == CheckpointStatus.FAIL


# ------------------------------------------------------------- 019 Cross Join Risk
class TestCrossJoinRisk:
    RULE = "CODE-PYSPARK-019"

    def test_positive_cross_join_large(self, rules_by_id):
        r = _rule(rules_by_id, self.RULE)
        code = (
            "df1 = spark.read.table('t1')\n"
            "df2 = spark.read.table('t2')\n"
            "out = df1.crossJoin(df2)\n"
            "out.write.save('out')\n"
        )
        f = evaluate_rule(r, _snippet(code), _ctx(50.0))
        assert f is not None
        assert f.status == CheckpointStatus.FAIL
        assert f.severity.value == "HIGH"
        assert "Cartesian product" in f.recommendation

    def test_negative_keyed_join(self, rules_by_id, code_dir):
        r = _rule(rules_by_id, self.RULE)
        f = evaluate_rule(r, _data(code_dir, "expensive_join.py"), _ctx(5.0))
        # expensive_join uses inner join with on="session_id", not crossJoin
        assert f is None

    def test_edge_join_how_cross(self, rules_by_id):
        r = _rule(rules_by_id, self.RULE)
        code = (
            "df1 = spark.read.table('t1')\n"
            "df2 = spark.read.table('t2')\n"
            "out = df1.join(df2, how='cross')\n"
        )
        f = evaluate_rule(r, _snippet(code), _ctx())
        assert f is not None
        assert f.status == CheckpointStatus.WARN
        assert f.confidence <= 0.65

    def test_false_positive_comment_or_variable(self, rules_by_id):
        r = _rule(rules_by_id, self.RULE)
        code = (
            "# Avoid crossJoin in production\n"
            "cross_join_allowed = False\n"
            "df = spark.read.table('t')\n"
        )
        f = evaluate_rule(r, _snippet(code), _ctx(100.0))
        assert f is None

    def test_context_small_vs_large_cross_join(self, rules_by_id):
        r = _rule(rules_by_id, self.RULE)
        code = (
            "df1 = spark.read.table('t1')\ndf2 = spark.read.table('t2')\nout = df1.crossJoin(df2)\n"
        )
        small = evaluate_rule(r, _snippet(code), _ctx(2.0))
        large = evaluate_rule(r, _snippet(code), _ctx(50.0))
        assert small is not None and small.status == CheckpointStatus.WARN
        assert large is not None and large.status == CheckpointStatus.FAIL
