"""Phase 4 rule tests — 5 meaningful tests each (positive, negative, edge,
false-positive, context) through the loaded YAML rules + AST evaluators."""

from __future__ import annotations

from dpif.models import CheckpointStatus
from dpif.rules.engine import evaluate_rule


def _rule(rules_by_id, rule_id):
    assert rule_id in rules_by_id, f"{rule_id} not loaded"
    rule = rules_by_id[rule_id]
    assert rule.evaluator, f"{rule_id} must declare an evaluator"
    assert isinstance(rule.params, dict), f"{rule_id} params must be a dict"
    return rule


def _data(code_dir, name, **kw):
    base = {
        "text": (code_dir / name).read_text(encoding="utf-8"),
        "source_file": name,
        "pipeline_name": "p4",
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


# ------------------------------------------------------------- 006 collect
class TestLargeCollect:
    RULE = "CODE-PYSPARK-006"

    def test_positive_unrestricted_large(self, rules_by_id, code_dir):
        f = evaluate_rule(
            _rule(rules_by_id, self.RULE), _data(code_dir, "large_collect.py"), _ctx(2048.0)
        )
        assert f is not None and f.status == CheckpointStatus.FAIL
        assert f.severity.value == "CRITICAL" and f.blocking is True

    def test_negative_no_collect(self, rules_by_id, code_dir):
        assert (
            evaluate_rule(
                _rule(rules_by_id, self.RULE), _data(code_dir, "optimized_pipeline.py"), _ctx(10.0)
            )
            is None
        )

    def test_edge_limited_collect_warns(self, rules_by_id, code_dir):
        f = evaluate_rule(
            _rule(rules_by_id, self.RULE), _data(code_dir, "small_collect.py"), _ctx(500.0)
        )
        assert f is not None and f.status == CheckpointStatus.WARN

    def test_false_positive_plain_name(self, rules_by_id):
        data = {
            "text": "collected = True\nprint(collected)",
            "source_file": "x.py",
            "pipeline_name": "p",
            "assumptions": {},
        }
        assert evaluate_rule(_rule(rules_by_id, self.RULE), data, _ctx(500.0)) is None

    def test_context_small_vs_huge(self, rules_by_id, code_dir):
        r = _rule(rules_by_id, self.RULE)
        small = evaluate_rule(r, _data(code_dir, "large_collect.py"), _ctx(0.05))
        huge = evaluate_rule(r, _data(code_dir, "large_collect.py"), _ctx(2048.0))
        assert small is not None and small.status == CheckpointStatus.WARN
        assert huge is not None and huge.status == CheckpointStatus.FAIL


# ------------------------------------------------------------- 007 topandas
class TestTopandas:
    RULE = "CODE-PYSPARK-007"

    def test_positive_unbounded_large(self, rules_by_id, code_dir):
        data = {
            "text": (code_dir / "bad_pipeline.py").read_text(encoding="utf-8"),
            "source_file": "bad_pipeline.py",
            "pipeline_name": "p",
            "assumptions": {},
        }
        f = evaluate_rule(_rule(rules_by_id, self.RULE), data, _ctx(2048.0))
        assert f is not None and f.status == CheckpointStatus.FAIL

    def test_negative_no_topandas(self, rules_by_id, code_dir):
        assert (
            evaluate_rule(
                _rule(rules_by_id, self.RULE), _data(code_dir, "optimized_pipeline.py"), _ctx(500.0)
            )
            is None
        )

    def test_edge_bounded_warns(self, rules_by_id, code_dir):
        f = evaluate_rule(
            _rule(rules_by_id, self.RULE), _data(code_dir, "limited_to_pandas.py"), _ctx(500.0)
        )
        assert f is not None and f.status == CheckpointStatus.WARN

    def test_false_positive_string_assignment(self, rules_by_id):
        data = {
            "text": 'to_pandas = "done"\nprint(to_pandas)',
            "source_file": "x.py",
            "pipeline_name": "p",
            "assumptions": {},
        }
        assert evaluate_rule(_rule(rules_by_id, self.RULE), data, _ctx(500.0)) is None

    def test_context_bounded_vs_unbounded(self, rules_by_id, code_dir):
        r = _rule(rules_by_id, self.RULE)
        bounded = evaluate_rule(r, _data(code_dir, "limited_to_pandas.py"), _ctx(2048.0))
        assert bounded is not None and bounded.status == CheckpointStatus.WARN


# ------------------------------------------------------------- 008 shuffle
class TestShuffle:
    RULE = "CODE-PYSPARK-008"

    def test_positive_large_join(self, rules_by_id, code_dir):
        f = evaluate_rule(
            _rule(rules_by_id, self.RULE), _data(code_dir, "expensive_join.py"), _ctx(500.0)
        )
        assert f is not None and f.status == CheckpointStatus.WARN
        assert "Potential shuffle risk" in f.recommendation

    def test_negative_small_volume(self, rules_by_id, code_dir):
        assert (
            evaluate_rule(
                _rule(rules_by_id, self.RULE), _data(code_dir, "expensive_join.py"), _ctx(10.0)
            )
            is None
        )

    def test_edge_exact_threshold(self, rules_by_id, code_dir):
        f = evaluate_rule(
            _rule(rules_by_id, self.RULE), _data(code_dir, "expensive_join.py"), _ctx(100.0)
        )
        assert f is not None

    def test_false_positive_plain_transforms(self, rules_by_id):
        data = {
            "text": (
                "df = spark.read.parquet('/a')\nout = df.filter('x > 1')\nout.write.save('/o')\n"
            ),
            "source_file": "x.py",
            "pipeline_name": "p",
            "assumptions": {},
        }
        assert evaluate_rule(_rule(rules_by_id, self.RULE), data, _ctx(500.0)) is None

    def test_context_never_fails_without_runtime(self, rules_by_id, code_dir):
        f = evaluate_rule(
            _rule(rules_by_id, self.RULE), _data(code_dir, "expensive_join.py"), _ctx(5000.0)
        )
        assert f is not None and f.status == CheckpointStatus.WARN


# ------------------------------------------------------------- 009 repartition
class TestRepartition:
    RULE = "CODE-PYSPARK-009"

    def test_positive_repeated(self, rules_by_id, code_dir):
        f = evaluate_rule(
            _rule(rules_by_id, self.RULE), _data(code_dir, "repartition_pipeline.py"), _ctx(500.0)
        )
        assert f is not None and f.status == CheckpointStatus.WARN

    def test_negative_single_sane(self, rules_by_id, code_dir):
        assert (
            evaluate_rule(
                _rule(rules_by_id, self.RULE), _data(code_dir, "optimized_pipeline.py"), _ctx(500.0)
            )
            is None
        )

    def test_edge_oversized_literal(self, rules_by_id):
        data = {
            "text": ("df = spark.read.parquet('/a')\ndf.repartition(5000).write.save('/o')\n"),
            "source_file": "x.py",
            "pipeline_name": "p",
            "assumptions": {},
        }
        f = evaluate_rule(_rule(rules_by_id, self.RULE), data, _ctx(500.0))
        assert f is not None

    def test_false_positive_partitionby_word(self, rules_by_id):
        data = {
            "text": "# repartition the data manually\nx = 1\n",
            "source_file": "x.py",
            "pipeline_name": "p",
            "assumptions": {},
        }
        assert evaluate_rule(_rule(rules_by_id, self.RULE), data, _ctx(500.0)) is None

    def test_context_flow_across_renames(self, rules_by_id, code_dir):
        # huge = df.repartition(..); huge.repartition(..) groups via flow root
        f = evaluate_rule(
            _rule(rules_by_id, self.RULE), _data(code_dir, "bad_pipeline.py"), _ctx(2048.0)
        )
        assert f is not None
        frames = [o.get("dataframe") for o in f.evidence.observed["operations"]]
        assert "huge" in frames


# ------------------------------------------------------------- 010 coalesce
class TestCoalesce:
    RULE = "CODE-PYSPARK-010"

    def test_positive_single_partition_large(self, rules_by_id, code_dir):
        f = evaluate_rule(
            _rule(rules_by_id, self.RULE), _data(code_dir, "coalesce_one.py"), _ctx(500.0)
        )
        assert f is not None and f.status == CheckpointStatus.WARN
        assert "parallelism" in f.recommendation.lower()

    def test_negative_no_coalesce(self, rules_by_id, code_dir):
        assert (
            evaluate_rule(
                _rule(rules_by_id, self.RULE), _data(code_dir, "optimized_pipeline.py"), _ctx(500.0)
            )
            is None
        )

    def test_edge_wide_coalesce_ok(self, rules_by_id):
        data = {
            "text": ("df = spark.read.parquet('/a')\ndf.coalesce(8).write.save('/o')\n"),
            "source_file": "x.py",
            "pipeline_name": "p",
            "assumptions": {},
        }
        assert evaluate_rule(_rule(rules_by_id, self.RULE), data, _ctx(500.0)) is None

    def test_false_positive_comment(self, rules_by_id):
        data = {
            "text": "# coalesce the outputs later\nx = 1\n",
            "source_file": "x.py",
            "pipeline_name": "p",
            "assumptions": {},
        }
        assert evaluate_rule(_rule(rules_by_id, self.RULE), data, _ctx(500.0)) is None

    def test_context_small_volume_ok(self, rules_by_id, code_dir):
        assert (
            evaluate_rule(
                _rule(rules_by_id, self.RULE), _data(code_dir, "coalesce_one.py"), _ctx(10.0)
            )
            is None
        )


# ------------------------------------------------------------- 011 cache
class TestCache:
    RULE = "CODE-PYSPARK-011"

    def test_positive_no_reuse(self, rules_by_id, code_dir):
        f = evaluate_rule(
            _rule(rules_by_id, self.RULE), _data(code_dir, "repeated_cache.py"), _ctx(500.0)
        )
        assert f is not None and f.status == CheckpointStatus.WARN

    def test_negative_reused(self, rules_by_id, code_dir):
        assert (
            evaluate_rule(
                _rule(rules_by_id, self.RULE), _data(code_dir, "justified_cache.py"), _ctx(500.0)
            )
            is None
        )

    def test_edge_persist_variant(self, rules_by_id):
        data = {
            "text": ("df = spark.read.parquet('/a')\ndf.persist()\ndf.write.save('/o')\n"),
            "source_file": "x.py",
            "pipeline_name": "p",
            "assumptions": {},
        }
        assert evaluate_rule(_rule(rules_by_id, self.RULE), data, _ctx(500.0)) is not None

    def test_false_positive_cached_name(self, rules_by_id):
        data = {
            "text": ("cached_df = df.filter('x > 1')\ncached_df.write.save('/o')\n"),
            "source_file": "x.py",
            "pipeline_name": "p",
            "assumptions": {},
        }
        assert evaluate_rule(_rule(rules_by_id, self.RULE), data, _ctx(500.0)) is None

    def test_context_volume_independent(self, rules_by_id, code_dir):
        r = _rule(rules_by_id, self.RULE)
        assert evaluate_rule(r, _data(code_dir, "repeated_cache.py"), _ctx(10.0)) is not None
        assert evaluate_rule(r, _data(code_dir, "justified_cache.py"), _ctx(10.0)) is None


# ------------------------------------------------------------- 012 actions
class TestActions:
    RULE = "CODE-PYSPARK-012"

    def test_positive_count_plus_write(self, rules_by_id, code_dir):
        f = evaluate_rule(
            _rule(rules_by_id, self.RULE), _data(code_dir, "repeated_actions.py"), _ctx(500.0)
        )
        assert f is not None and f.status == CheckpointStatus.WARN

    def test_negative_single_write(self, rules_by_id, code_dir):
        assert (
            evaluate_rule(
                _rule(rules_by_id, self.RULE), _data(code_dir, "optimized_pipeline.py"), _ctx(500.0)
            )
            is None
        )

    def test_edge_exactly_two(self, rules_by_id, code_dir):
        f = evaluate_rule(
            _rule(rules_by_id, self.RULE), _data(code_dir, "repeated_actions.py"), _ctx(10.0)
        )
        assert f is not None

    def test_false_positive_transforms_only(self, rules_by_id):
        data = {
            "text": ("df = spark.read.parquet('/a')\nout = df.filter('x > 1')\n"),
            "source_file": "x.py",
            "pipeline_name": "p",
            "assumptions": {},
        }
        assert evaluate_rule(_rule(rules_by_id, self.RULE), data, _ctx(500.0)) is None

    def test_context_lists_both_actions(self, rules_by_id, code_dir):
        f = evaluate_rule(
            _rule(rules_by_id, self.RULE), _data(code_dir, "repeated_actions.py"), _ctx(500.0)
        )
        assert f is not None
        assert "COUNT" in str(f.evidence.observed.get("operations"))


# ------------------------------------------------------------- 013 sort
class TestSort:
    RULE = "CODE-PYSPARK-013"

    def test_positive_global_sort_large(self, rules_by_id, code_dir):
        f = evaluate_rule(
            _rule(rules_by_id, self.RULE), _data(code_dir, "global_sort.py"), _ctx(500.0)
        )
        assert f is not None and f.status == CheckpointStatus.WARN

    def test_negative_no_sort(self, rules_by_id, code_dir):
        assert (
            evaluate_rule(
                _rule(rules_by_id, self.RULE), _data(code_dir, "optimized_pipeline.py"), _ctx(500.0)
            )
            is None
        )

    def test_edge_exact_threshold(self, rules_by_id, code_dir):
        assert (
            evaluate_rule(
                _rule(rules_by_id, self.RULE), _data(code_dir, "global_sort.py"), _ctx(100.0)
            )
            is not None
        )

    def test_false_positive_word_in_string(self, rules_by_id):
        data = {
            "text": 'msg = "please sort the files"\nprint(msg)\n',
            "source_file": "x.py",
            "pipeline_name": "p",
            "assumptions": {},
        }
        assert evaluate_rule(_rule(rules_by_id, self.RULE), data, _ctx(500.0)) is None

    def test_context_small_ok(self, rules_by_id, code_dir):
        assert (
            evaluate_rule(
                _rule(rules_by_id, self.RULE), _data(code_dir, "global_sort.py"), _ctx(10.0)
            )
            is None
        )


# ------------------------------------------------------------- 014 window
class TestWindow:
    RULE = "CODE-PYSPARK-014"

    def test_positive_unbounded(self, rules_by_id, code_dir):
        f = evaluate_rule(
            _rule(rules_by_id, self.RULE), _data(code_dir, "expensive_window.py"), _ctx(500.0)
        )
        assert f is not None and f.status == CheckpointStatus.WARN

    def test_negative_no_window(self, rules_by_id, code_dir):
        assert (
            evaluate_rule(
                _rule(rules_by_id, self.RULE), _data(code_dir, "optimized_pipeline.py"), _ctx(500.0)
            )
            is None
        )

    def test_edge_bounded_ok(self, rules_by_id):
        data = {
            "text": (
                "from pyspark.sql import Window\n"
                "spec = Window.partitionBy('a').orderBy('b').rowsBetween(-5, 0)\n"
            ),
            "source_file": "x.py",
            "pipeline_name": "p",
            "assumptions": {},
        }
        assert evaluate_rule(_rule(rules_by_id, self.RULE), data, _ctx(500.0)) is None

    def test_false_positive_plain_name(self, rules_by_id):
        data = {
            "text": "windows = ['a', 'b']\nprint(windows)\n",
            "source_file": "x.py",
            "pipeline_name": "p",
            "assumptions": {},
        }
        assert evaluate_rule(_rule(rules_by_id, self.RULE), data, _ctx(500.0)) is None

    def test_context_over_form_detected(self, rules_by_id, code_dir):
        # finding references the over() application site, not just the spec
        f = evaluate_rule(
            _rule(rules_by_id, self.RULE), _data(code_dir, "expensive_window.py"), _ctx(500.0)
        )
        assert f is not None
        assert any("over(" in str(ev) for ev in f.evidence.evidence)


# ------------------------------------------------------------- 015 dedup
class TestDedup:
    RULE = "CODE-PYSPARK-015"

    def test_positive_large_dedup(self, rules_by_id, code_dir):
        f = evaluate_rule(
            _rule(rules_by_id, self.RULE), _data(code_dir, "good_pipeline.py"), _ctx(500.0)
        )
        assert f is not None and f.status == CheckpointStatus.WARN

    def test_negative_small_volume(self, rules_by_id, code_dir):
        assert (
            evaluate_rule(
                _rule(rules_by_id, self.RULE), _data(code_dir, "good_pipeline.py"), _ctx(10.0)
            )
            is None
        )

    def test_edge_exact_threshold(self, rules_by_id, code_dir):
        assert (
            evaluate_rule(
                _rule(rules_by_id, self.RULE), _data(code_dir, "good_pipeline.py"), _ctx(100.0)
            )
            is not None
        )

    def test_false_positive_comment(self, rules_by_id):
        data = {
            "text": "# deduplicate downstream\nx = 1\n",
            "source_file": "x.py",
            "pipeline_name": "p",
            "assumptions": {},
        }
        assert evaluate_rule(_rule(rules_by_id, self.RULE), data, _ctx(500.0)) is None

    def test_context_distinct_variant(self, rules_by_id):
        data = {
            "text": ("df = spark.read.parquet('/a')\nu = df.distinct()\nu.write.save('/o')\n"),
            "source_file": "x.py",
            "pipeline_name": "p",
            "assumptions": {},
        }
        f = evaluate_rule(_rule(rules_by_id, self.RULE), data, _ctx(500.0))
        assert f is not None


def test_all_ten_rules_load_with_evaluators_and_params(rules_by_id):
    for n in range(6, 16):
        rid = f"CODE-PYSPARK-{n:03d}"
        rule = rules_by_id[rid]
        assert rule.evaluator, rid
        assert isinstance(rule.params, dict), rid
    # Rules with numeric policies must expose them as tunable params
    # (analyzers must not hard-code thresholds).
    assert rules_by_id["CODE-PYSPARK-006"].params["critical_gb"] == 500.0
    assert rules_by_id["CODE-PYSPARK-008"].params["shuffle_gb"] == 100.0
    assert rules_by_id["CODE-PYSPARK-009"].params["max_partitions"] == 2000
    assert rules_by_id["CODE-PYSPARK-013"].params["sort_gb"] == 100.0
    assert rules_by_id["CODE-PYSPARK-015"].params["dedup_gb"] == 100.0
