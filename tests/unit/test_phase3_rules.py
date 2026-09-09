"""Phase 3 rule tests — 5 meaningful tests each (positive, negative, edge,
false-positive, context), executed through the loaded YAML rules."""

from __future__ import annotations

from dpif.models import CheckpointStatus
from dpif.rules.engine import evaluate_rule


def _rule(rules_by_id, rule_id):
    assert rule_id in rules_by_id, f"{rule_id} not loaded"
    rule = rules_by_id[rule_id]
    assert rule.evaluator, f"{rule_id} must declare an evaluator"
    return rule


def _data(observed=None, **kw):
    base = {"text": "", "pipeline_name": "p3", "assumptions": {}, "observed": observed or {}}
    base.update(kw)
    return base


def _ctx(**kw):
    base = {"evidence_source": "fixture metadata", "collection_method": "fixture"}
    base.update(kw)
    return base


def _small_files_obs(**kw):
    obs = {
        "average_file_size_kb": 284.0,
        "median_file_size_kb": 210.0,
        "p95_file_size_kb": 900.0,
        "p99_file_size_kb": 1500.0,
        "file_count": 1800000,
        "total_gb": 500.0,
        "format": "parquet",
        "collection_method": "fixture",
        "evidence_source": "fixture metadata",
    }
    obs.update(kw)
    return obs


# ------------------------------------------------------------- DATA-001
class TestData001:
    RULE = "DATA-001"

    def test_positive_small_files_warn(self, rules_by_id):
        f = evaluate_rule(_rule(rules_by_id, self.RULE), _data(_small_files_obs()), _ctx())
        assert f is not None and f.rule_id == self.RULE
        assert f.status == CheckpointStatus.WARN
        assert f.evidence.observed["configured_threshold_kb"] == 1000.0

    def test_negative_healthy_sizes(self, rules_by_id):
        obs = _small_files_obs(
            average_file_size_kb=262144.0,
            median_file_size_kb=250000.0,
            p95_file_size_kb=350000.0,
            file_count=2000,
        )
        assert evaluate_rule(_rule(rules_by_id, self.RULE), _data(obs), _ctx()) is None

    def test_edge_few_files_not_flagged(self, rules_by_id):
        obs = _small_files_obs(
            average_file_size_kb=100.0,
            median_file_size_kb=90.0,
            p95_file_size_kb=150.0,
            file_count=10,
        )
        assert evaluate_rule(_rule(rules_by_id, self.RULE), _data(obs), _ctx()) is None

    def test_false_positive_no_file_stats(self, rules_by_id):
        assert evaluate_rule(_rule(rules_by_id, self.RULE), _data({}), _ctx()) is None

    def test_context_evidence_source_recorded(self, rules_by_id):
        f = evaluate_rule(_rule(rules_by_id, self.RULE), _data(_small_files_obs()), _ctx())
        assert f is not None
        assert f.evidence.observed["evidence_source"] == "fixture metadata"
        assert f.evidence.observed["collection_method"] == "fixture"
        assert f.confidence >= 0.75


# ------------------------------------------------------------- DATA-002
class TestData002:
    RULE = "DATA-002"
    SIZES = {"country=US": 60.0, "country=DE": 5.0, "country=FR": 5.0}

    def test_positive_hot_partition(self, rules_by_id):
        obs = {
            "partition_sizes_gb": dict(self.SIZES),
            "total_gb": 100.0,
            "collection_method": "fixture",
            "evidence_source": "fixture metadata",
        }
        f = evaluate_rule(_rule(rules_by_id, self.RULE), _data(obs), _ctx())
        assert f is not None and f.status == CheckpointStatus.WARN
        assert "Potential partition imbalance" in f.recommendation

    def test_negative_balanced(self, rules_by_id):
        obs = {"partition_sizes_gb": {"a": 16.0, "b": 15.5}, "total_gb": 100.0}
        assert evaluate_rule(_rule(rules_by_id, self.RULE), _data(obs), _ctx()) is None

    def test_edge_exact_ratio_boundary(self, rules_by_id):
        obs = {"partition_sizes_gb": {"a": 50.0, "b": 10.0}, "total_gb": 100.0}
        assert evaluate_rule(_rule(rules_by_id, self.RULE), _data(obs), _ctx()) is not None

    def test_false_positive_single_partition(self, rules_by_id):
        obs = {"partition_sizes_gb": {"only": 50.0}, "total_gb": 50.0}
        assert evaluate_rule(_rule(rules_by_id, self.RULE), _data(obs), _ctx()) is None

    def test_context_tiny_volumes_ignored(self, rules_by_id):
        obs = {"partition_sizes_gb": {"a": 0.5, "b": 0.1}, "total_gb": 0.6}
        assert evaluate_rule(_rule(rules_by_id, self.RULE), _data(obs), _ctx()) is None


# ------------------------------------------------------------- DATA-003
class TestData003:
    RULE = "DATA-003"
    EXPECTED = {
        "columns": [
            {"name": "order_id", "data_type": "long", "nullable": False},
            {"name": "customer_id", "data_type": "long", "nullable": False},
            {"name": "order_total", "data_type": "decimal(12,2)", "nullable": True},
            {"name": "loyalty_tier", "data_type": "string", "nullable": True},
        ]
    }
    DRIFTED = [
        {"name": "order_id", "data_type": "long", "nullable": False},
        {"name": "customer_id", "data_type": "long", "nullable": False},
        {"name": "order_total", "data_type": "double", "nullable": True},
        {"name": "promo_code", "data_type": "string", "nullable": True},
    ]

    def test_positive_drift_fails(self, rules_by_id):
        obs = {"schema_columns": list(self.DRIFTED)}
        ctx = _ctx(expected_schema=dict(self.EXPECTED))
        f = evaluate_rule(_rule(rules_by_id, self.RULE), _data(obs), ctx)
        assert f is not None and f.status == CheckpointStatus.FAIL
        assert "loyalty_tier" in f.evidence.observed["missing"]

    def test_negative_identical_schemas(self, rules_by_id):
        obs = {"schema_columns": [dict(c) for c in self.EXPECTED["columns"]]}
        ctx = _ctx(expected_schema=dict(self.EXPECTED))
        assert evaluate_rule(_rule(rules_by_id, self.RULE), _data(obs), ctx) is None

    def test_edge_unexpected_only_warns(self, rules_by_id):
        obs_cols = [dict(c) for c in self.EXPECTED["columns"]] + [
            {"name": "promo_code", "data_type": "string", "nullable": True}
        ]
        ctx = _ctx(expected_schema=dict(self.EXPECTED))
        f = evaluate_rule(_rule(rules_by_id, self.RULE), _data({"schema_columns": obs_cols}), ctx)
        assert f is not None and f.status == CheckpointStatus.WARN

    def test_false_positive_no_expected_schema(self, rules_by_id):
        obs = {"schema_columns": list(self.DRIFTED)}
        assert evaluate_rule(_rule(rules_by_id, self.RULE), _data(obs), _ctx()) is None

    def test_context_custom_policy(self, rules_by_id):
        obs = {"schema_columns": list(self.DRIFTED)}
        ctx = _ctx(
            expected_schema=dict(self.EXPECTED),
            schema_policy={
                "missing": "WARN",
                "type_change": "WARN",
                "unexpected": "WARN",
                "nullable_change": "WARN",
            },
        )
        f = evaluate_rule(_rule(rules_by_id, self.RULE), _data(obs), ctx)
        assert f is not None and f.status == CheckpointStatus.WARN


# ------------------------------------------------------------- DATA-004
class TestData004:
    RULE = "DATA-004"

    def test_positive_missing_volume(self, rules_by_id):
        f = evaluate_rule(
            _rule(rules_by_id, self.RULE),
            _data({}),
            _ctx(expected_volume_gb=None, peak_volume_gb=None),
        )
        assert f is not None and f.status == CheckpointStatus.WARN

    def test_negative_declared_volume(self, rules_by_id):
        ctx = _ctx(expected_volume_gb=500.0, peak_volume_gb=3000.0)
        assert evaluate_rule(_rule(rules_by_id, self.RULE), _data({}), ctx) is None

    def test_edge_zero_counts_as_missing(self, rules_by_id):
        ctx = _ctx(expected_volume_gb=0.0, peak_volume_gb=0.0)
        assert evaluate_rule(_rule(rules_by_id, self.RULE), _data({}), ctx) is not None

    def test_false_positive_peak_optional(self, rules_by_id):
        ctx = _ctx(expected_volume_gb=500.0, peak_volume_gb=None)
        assert evaluate_rule(_rule(rules_by_id, self.RULE), _data({}), ctx) is None

    def test_context_recommendation_names_contract(self, rules_by_id):
        f = evaluate_rule(
            _rule(rules_by_id, self.RULE),
            _data({}),
            _ctx(expected_volume_gb=None, peak_volume_gb=None),
        )
        assert f is not None and "contract" in f.recommendation.lower()


# ------------------------------------------------------------- DATA-005
class TestData005:
    RULE = "DATA-005"

    def test_positive_huge_count(self, rules_by_id):
        f = evaluate_rule(_rule(rules_by_id, self.RULE), _data(_small_files_obs()), _ctx())
        assert f is not None and f.status == CheckpointStatus.WARN
        assert f.evidence.observed["file_count"] == 1800000

    def test_negative_normal_count(self, rules_by_id):
        obs = _small_files_obs(file_count=2000)
        assert evaluate_rule(_rule(rules_by_id, self.RULE), _data(obs), _ctx()) is None

    def test_edge_exact_limit(self, rules_by_id):
        ctx = _ctx(thresholds={"excessive_file_count": 1000000})
        obs = _small_files_obs(file_count=1000000)
        assert evaluate_rule(_rule(rules_by_id, self.RULE), _data(obs), ctx) is not None

    def test_false_positive_just_below(self, rules_by_id):
        assert (
            evaluate_rule(
                _rule(rules_by_id, self.RULE), _data(_small_files_obs(file_count=999999)), _ctx()
            )
            is None
        )

    def test_context_custom_limit(self, rules_by_id):
        ctx = _ctx(thresholds={"excessive_file_count": 100000})
        obs = _small_files_obs(file_count=500000)
        assert evaluate_rule(_rule(rules_by_id, self.RULE), _data(obs), ctx) is not None


# ------------------------------------------------------------- SOURCE-001
class TestSource001:
    RULE = "SOURCE-001"

    def _obs(self, fmt, vol=500.0, stype="adls"):
        return {"format": fmt, "expected_volume_gb": vol, "source_type": stype}

    def test_positive_unknown_format(self, rules_by_id):
        f = evaluate_rule(
            _rule(rules_by_id, self.RULE), _data(self._obs("xml")), _ctx(expected_volume_gb=500.0)
        )
        assert f is not None and f.status == CheckpointStatus.FAIL

    def test_positive_csv_at_scale_warns(self, rules_by_id):
        f = evaluate_rule(
            _rule(rules_by_id, self.RULE),
            _data(self._obs("csv", 600.0)),
            _ctx(expected_volume_gb=600.0),
        )
        assert f is not None and f.status == CheckpointStatus.WARN
        assert "columnar" in f.recommendation.lower()

    def test_negative_parquet(self, rules_by_id):
        assert (
            evaluate_rule(
                _rule(rules_by_id, self.RULE),
                _data(self._obs("parquet")),
                _ctx(expected_volume_gb=500.0),
            )
            is None
        )

    def test_false_positive_jdbc_row_protocol(self, rules_by_id):
        ctx = _ctx(expected_volume_gb=800.0, source_type="jdbc")
        assert (
            evaluate_rule(
                _rule(rules_by_id, self.RULE), _data(self._obs("unknown", 800.0, "jdbc")), ctx
            )
            is None
        )

    def test_context_small_csv_acceptable(self, rules_by_id):
        ctx = _ctx(expected_volume_gb=10.0)
        assert (
            evaluate_rule(_rule(rules_by_id, self.RULE), _data(self._obs("csv", 10.0)), ctx) is None
        )


# ------------------------------------------------------------- SOURCE-002
class TestSource002:
    RULE = "SOURCE-002"

    def test_positive_full_load_large(self, rules_by_id):
        ctx = _ctx(expected_volume_gb=2048.0, ingestion_mode="full_load")
        f = evaluate_rule(_rule(rules_by_id, self.RULE), _data({}), ctx)
        assert f is not None and f.status == CheckpointStatus.FAIL

    def test_negative_incremental(self, rules_by_id):
        ctx = _ctx(expected_volume_gb=2048.0, ingestion_mode="incremental")
        assert evaluate_rule(_rule(rules_by_id, self.RULE), _data({}), ctx) is None

    def test_edge_exact_large_threshold(self, rules_by_id):
        ctx = _ctx(
            expected_volume_gb=500.0,
            ingestion_mode="full_load",
            thresholds={"large_volume_gb": 500.0},
        )
        assert evaluate_rule(_rule(rules_by_id, self.RULE), _data({}), ctx) is not None

    def test_false_positive_partitioning_evidence(self, rules_by_id):
        ctx = _ctx(expected_volume_gb=2048.0, ingestion_mode="batch", partitioning=["event_date"])
        assert evaluate_rule(_rule(rules_by_id, self.RULE), _data({}), ctx) is None

    def test_context_no_volume_defers_to_data004(self, rules_by_id):
        ctx = _ctx(expected_volume_gb=None, ingestion_mode="full_load")
        assert evaluate_rule(_rule(rules_by_id, self.RULE), _data({}), ctx) is None


# ------------------------------------------------------------- SOURCE-003
class TestSource003:
    RULE = "SOURCE-003"

    def _jdbc(self, **kw):
        base = {"partition_column": None, "num_partitions": None, "incremental_column": None}
        base.update(kw)
        return base

    def test_positive_unsharded_large(self, rules_by_id):
        ctx = _ctx(expected_volume_gb=800.0, source_type="jdbc", jdbc=self._jdbc())
        f = evaluate_rule(_rule(rules_by_id, self.RULE), _data({}), ctx)
        assert f is not None

    def test_negative_sharded(self, rules_by_id):
        ctx = _ctx(
            expected_volume_gb=800.0,
            source_type="jdbc",
            jdbc=self._jdbc(partition_column="order_id", num_partitions=32),
        )
        assert evaluate_rule(_rule(rules_by_id, self.RULE), _data({}), ctx) is None

    def test_edge_exact_minimum(self, rules_by_id):
        ctx = _ctx(
            expected_volume_gb=100.0,
            source_type="jdbc",
            jdbc=self._jdbc(),
            thresholds={"jdbc_parallel_min_gb": 100.0},
        )
        assert evaluate_rule(_rule(rules_by_id, self.RULE), _data({}), ctx) is not None

    def test_false_positive_non_jdbc(self, rules_by_id):
        ctx = _ctx(expected_volume_gb=800.0, source_type="parquet")
        assert evaluate_rule(_rule(rules_by_id, self.RULE), _data({}), ctx) is None

    def test_context_raised_bar(self, rules_by_id):
        ctx = _ctx(
            expected_volume_gb=800.0,
            source_type="jdbc",
            jdbc=self._jdbc(),
            thresholds={"jdbc_parallel_min_gb": 1000.0},
        )
        assert evaluate_rule(_rule(rules_by_id, self.RULE), _data({}), ctx) is None


def test_all_phase3_rules_load_with_evaluators(rules_by_id):
    for rid in (
        "DATA-001",
        "DATA-002",
        "DATA-003",
        "DATA-004",
        "DATA-005",
        "SOURCE-001",
        "SOURCE-002",
        "SOURCE-003",
    ):
        assert rules_by_id[rid].evaluator, rid
