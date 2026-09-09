"""Rule engine tests — 5 meaningful tests per rule (positive, negative,
edge, false-positive, context)."""

from __future__ import annotations

from dpif.models import CheckpointStatus
from dpif.rules.engine import evaluate_rule, evaluate_rules


def _rule(rules_by_id, rule_id):
    assert rule_id in rules_by_id, f"{rule_id} not loaded"
    return rules_by_id[rule_id]


def _data(text, **kw):
    base = {"text": text, "pipeline_name": "test_pipe", "assumptions": {}}
    base.update(kw)
    return base


# ---------------------------------------------------------------- CODE-PYSPARK-001
class TestCollect:
    RULE = "CODE-PYSPARK-001"

    def test_positive_large_data_fails_blocking(self, rules_by_id):
        r = _rule(rules_by_id, self.RULE)
        f = evaluate_rule(r, _data("rows = df.collect()"), {"data_size_gb": 500})
        assert f is not None
        assert f.status == CheckpointStatus.FAIL
        assert f.blocking is True
        assert f.severity.value == "CRITICAL"
        assert f.confidence >= 0.8

    def test_negative_count_not_flagged(self, rules_by_id):
        r = _rule(rules_by_id, self.RULE)
        assert evaluate_rule(r, _data("n = df.count()"), {"data_size_gb": 500}) is None

    def test_edge_whitespace_before_paren(self, rules_by_id):
        r = _rule(rules_by_id, self.RULE)
        f = evaluate_rule(r, _data("rows = df.collect ()"), {"data_size_gb": 500})
        assert f is not None

    def test_false_positive_comment_word_collects(self, rules_by_id):
        # "(no driver collects)" must NOT match r"\.collect\s*\("
        r = _rule(rules_by_id, self.RULE)
        text = _data('"""No driver collects here."""')
        assert evaluate_rule(r, text, {"data_size_gb": 500}) is None

    def test_small_data_context_downgrades_to_warning(self, rules_by_id):
        r = _rule(rules_by_id, self.RULE)
        f = evaluate_rule(r, _data("small = lookup.limit(50).collect()"), {"data_size_gb": 0.05})
        assert f is not None
        assert f.status == CheckpointStatus.WARN
        assert "small-data" in f.assumptions


# ---------------------------------------------------------------- CODE-PYSPARK-002
class TestUdf:
    RULE = "CODE-PYSPARK-002"

    def test_positive_udf_call(self, rules_by_id):
        r = _rule(rules_by_id, self.RULE)
        f = evaluate_rule(r, _data("shout = udf(lambda s: s.upper())"), {"data_size_gb": 500})
        assert f is not None
        assert f.rule_id == self.RULE

    def test_negative_native_spark_function(self, rules_by_id):
        r = _rule(rules_by_id, self.RULE)
        text = _data("df.withColumn('x', F.upper('y'))")
        assert evaluate_rule(r, text, {"data_size_gb": 500}) is None

    def test_edge_space_before_paren(self, rules_by_id):
        r = _rule(rules_by_id, self.RULE)
        assert evaluate_rule(r, _data("f = udf (lambda x: x)"), {"data_size_gb": 10}) is not None

    def test_false_positive_plural_identifier(self, rules_by_id):
        r = _rule(rules_by_id, self.RULE)
        text = _data("udfs = [a, b]\nprint(len(udfs))")
        assert evaluate_rule(r, text, {"data_size_gb": 10}) is None

    def test_no_context_yields_qualified_warning(self, rules_by_id):
        r = _rule(rules_by_id, self.RULE)
        f = evaluate_rule(r, _data("f = udf(lambda x: x)"))
        assert f is not None
        assert f.status == CheckpointStatus.WARN
        assert "context-unavailable" in f.assumptions
        assert f.confidence < 0.9


# ---------------------------------------------------------------- CODE-PYSPARK-003
class TestShowTake:
    RULE = "CODE-PYSPARK-003"

    def test_positive_show(self, rules_by_id):
        r = _rule(rules_by_id, self.RULE)
        assert evaluate_rule(r, _data("df.show()"), {"data_size_gb": 100}) is not None

    def test_positive_take(self, rules_by_id):
        r = _rule(rules_by_id, self.RULE)
        assert evaluate_rule(r, _data("first = df.take(5)"), {"data_size_gb": 100}) is not None

    def test_negative_write_path(self, rules_by_id):
        r = _rule(rules_by_id, self.RULE)
        text = _data('df.write.format("delta").save(path)')
        assert evaluate_rule(r, text, {"data_size_gb": 100}) is None

    def test_false_positive_takeover_identifier(self, rules_by_id):
        r = _rule(rules_by_id, self.RULE)
        assert evaluate_rule(r, _data("takeover = True"), {"data_size_gb": 100}) is None

    def test_edge_take_with_space(self, rules_by_id):
        r = _rule(rules_by_id, self.RULE)
        assert evaluate_rule(r, _data("x = df.take (10)"), {"data_size_gb": 100}) is not None


# ---------------------------------------------------------------- CODE-PYSPARK-004
class TestCrossJoin:
    RULE = "CODE-PYSPARK-004"

    def test_positive_crossjoin(self, rules_by_id):
        r = _rule(rules_by_id, self.RULE)
        f = evaluate_rule(r, _data("pairs = df.crossJoin(other)"), {"data_size_gb": 500})
        assert f is not None
        assert f.status == CheckpointStatus.FAIL

    def test_negative_keyed_join(self, rules_by_id):
        r = _rule(rules_by_id, self.RULE)
        assert evaluate_rule(r, _data("df.join(other, 'id')"), {"data_size_gb": 500}) is None

    def test_edge_method_chain(self, rules_by_id):
        r = _rule(rules_by_id, self.RULE)
        text = _data("df.crossJoin(other).filter('x > 1')")
        assert evaluate_rule(r, text, {"data_size_gb": 500}) is not None

    def test_false_positive_plural(self, rules_by_id):
        r = _rule(rules_by_id, self.RULE)
        assert evaluate_rule(r, _data("crossjoins = 5"), {"data_size_gb": 500}) is None

    def test_requires_context_without_it_warns(self, rules_by_id):
        r = _rule(rules_by_id, self.RULE)
        assert "data_size_gb" in r.requires_context
        f = evaluate_rule(r, _data("pairs = df.crossJoin(other)"))
        assert f is not None
        assert f.status == CheckpointStatus.WARN
        assert "context-unavailable" in f.assumptions


# ---------------------------------------------------------------- CODE-PYSPARK-005
class TestHardcodedPath:
    RULE = "CODE-PYSPARK-005"

    def test_positive_tmp_path(self, rules_by_id):
        r = _rule(rules_by_id, self.RULE)
        f = evaluate_rule(r, _data('df.write.parquet("/tmp/out")'), {"data_size_gb": 10})
        assert f is not None
        assert "/tmp/out" in str(f.evidence.evidence)

    def test_positive_windows_path(self, rules_by_id):
        r = _rule(rules_by_id, self.RULE)
        text = _data(r'df.read.csv("C:\data\in.csv")')
        assert evaluate_rule(r, text, {"data_size_gb": 10}) is not None

    def test_negative_abfss_config_path(self, rules_by_id):
        r = _rule(rules_by_id, self.RULE)
        assert (
            evaluate_rule(
                r,
                _data('spark.read.load("abfss://c@a.dfs.core.windows.net/d/")'),
                {"data_size_gb": 10},
            )
            is None
        )

    def test_false_positive_tmpdir_prefix(self, rules_by_id):
        r = _rule(rules_by_id, self.RULE)
        assert evaluate_rule(r, _data('p = "/tmpdir/file"'), {"data_size_gb": 10}) is None

    def test_evidence_and_recommendation_present(self, rules_by_id):
        r = _rule(rules_by_id, self.RULE)
        f = evaluate_rule(r, _data('open("/tmp/x.txt")'), {"data_size_gb": 10})
        assert f is not None
        assert f.recommendation
        assert f.evidence.evidence


def test_all_five_rules_load(all_rules):
    ids = {r.rule_id for r in all_rules}
    for rid in (
        "CODE-PYSPARK-001",
        "CODE-PYSPARK-002",
        "CODE-PYSPARK-003",
        "CODE-PYSPARK-004",
        "CODE-PYSPARK-005",
    ):
        assert rid in ids


def test_evaluate_rules_batch_returns_only_matches(rules_by_id):
    rules = [rules_by_id["CODE-PYSPARK-001"], rules_by_id["CODE-PYSPARK-003"]]
    out = evaluate_rules(rules, "df.show()", pipeline_name="p", context={"data_size_gb": 5})
    assert [f.rule_id for f in out] == ["CODE-PYSPARK-003"]
