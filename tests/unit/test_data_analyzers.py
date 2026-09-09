"""Analyzer unit tests: volume, files, distribution, partitions, schema,
growth, formats, JDBC, streaming, coverage."""

from __future__ import annotations

from dpif.analyzers.data import (
    coverage as coverage_mod,
)
from dpif.analyzers.data import (
    distribution,
    formats,
    growth,
    jdbc,
    partitions,
    schema,
    small_files,
    streaming,
    volume,
)
from dpif.models import Checkpoint, CheckpointStatus


def _prof(**kw):
    base = {
        "total_bytes": 100 * 1024**3,
        "total_gb": 100.0,
        "file_count": 800,
        "average_file_size_kb": 131072.0,
        "median_file_size_kb": 128000.0,
        "p95_file_size_kb": 175000.0,
        "p99_file_size_kb": 210000.0,
        "min_file_size_kb": 60000.0,
        "max_file_size_kb": 280000.0,
        "record_count": 100000000,
        "partition_count": 16,
        "collection_method": "fixture",
        "evidence_source": "fixture metadata",
    }
    base.update(kw)
    return base


# ---- volume -----------------------------------------------------------
def test_volume_present_ok():
    assert volume.analyze_volume_presence(500.0, 3000.0)["triggered"] is False


def test_volume_missing_triggers():
    r = volume.analyze_volume_presence(None, None)
    assert r["triggered"] is True
    assert "expected_daily_volume_gb" in r["recommendation"] or "expected" in str(r)


def test_volume_zero_counts_as_missing():
    assert volume.analyze_volume_presence(0.0, 0.0)["triggered"] is True


# ---- small files ------------------------------------------------------
def test_small_files_healthy_no_trigger():
    assert small_files.analyze_small_files(_prof())["triggered"] is False


def test_small_files_triggers_with_distribution():
    p = _prof(
        average_file_size_kb=284.0,
        median_file_size_kb=210.0,
        p95_file_size_kb=900.0,
        file_count=1800000,
        total_gb=500.0,
    )
    r = small_files.analyze_small_files(p, "parquet")
    assert r["triggered"] is True
    assert r["observed"]["configured_threshold_kb"] == 1000.0
    assert r["confidence"] >= 0.75


def test_small_files_csv_threshold_scaled():
    p = _prof(
        average_file_size_kb=200.0,
        median_file_size_kb=180.0,
        p95_file_size_kb=240.0,
        file_count=5000,
    )
    # csv factor 0.25 -> threshold 250 KB: 200 avg still below -> triggers
    assert small_files.analyze_small_files(p, "csv")["triggered"] is True
    # parquet factor 1.0 at same sizes also triggers; at 2000 KB it must not
    p2 = _prof(
        average_file_size_kb=2000.0,
        median_file_size_kb=1900.0,
        p95_file_size_kb=2500.0,
        file_count=5000,
    )
    assert small_files.analyze_small_files(p2, "parquet")["triggered"] is False


def test_small_files_few_files_no_trigger():
    p = _prof(
        average_file_size_kb=100.0, median_file_size_kb=90.0, p95_file_size_kb=150.0, file_count=10
    )
    assert small_files.analyze_small_files(p, "parquet")["triggered"] is False


def test_excessive_count_boundary():
    assert small_files.analyze_file_count({"file_count": 1000000}, 1000000)["triggered"] is True
    assert small_files.analyze_file_count({"file_count": 999999}, 1000000)["triggered"] is False


# ---- distribution -----------------------------------------------------
def test_distribution_healthy_ratio():
    d = distribution.describe_distribution(_prof())
    assert d["right_skewed_shape"] is False
    assert d["median_to_average_ratio"] > 0.9


def test_distribution_skewed_shape_flagged():
    d = distribution.describe_distribution(
        _prof(average_file_size_kb=131072.0, median_file_size_kb=20000.0, file_count=5000)
    )
    assert d["right_skewed_shape"] is True


# ---- partitions -------------------------------------------------------
def test_partitions_balanced_no_trigger():
    r = partitions.analyze_partitions({"a": 16.0, "b": 15.5, "c": 16.2}, total_gb=100.0)
    assert r["triggered"] is False


def test_partitions_hot_key_triggers_potential_wording():
    r = partitions.analyze_partitions(
        {"country=US": 60.0, "country=DE": 5.0, "country=FR": 5.0}, total_gb=100.0
    )
    assert r["triggered"] is True
    assert "Potential partition imbalance" in r["recommendation"]
    assert "runtime skew" not in r["recommendation"].lower() or "before" in r["recommendation"]
    assert r["observed"]["max_min_ratio"] == 12.0


def test_partitions_single_insufficient_not_finding():
    r = partitions.analyze_partitions({"only": 50.0}, total_gb=50.0)
    assert r["triggered"] is False


def test_partitions_tiny_volumes_no_trigger():
    r = partitions.analyze_partitions({"a": 0.5, "b": 0.1}, total_gb=0.6)
    assert r["triggered"] is False


# ---- schema -----------------------------------------------------------
EXPECTED = {
    "columns": [
        {"name": "order_id", "data_type": "long", "nullable": False},
        {"name": "customer_id", "data_type": "long", "nullable": False},
        {"name": "order_total", "data_type": "decimal(12,2)", "nullable": True},
        {"name": "loyalty_tier", "data_type": "string", "nullable": True},
    ]
}
OBSERVED_DRIFTED = {
    "columns": [
        {"name": "order_id", "data_type": "long", "nullable": False},
        {"name": "customer_id", "data_type": "long", "nullable": False},
        {"name": "order_total", "data_type": "double", "nullable": True},
        {"name": "promo_code", "data_type": "string", "nullable": True},
    ]
}


def test_schema_identical_pass():
    r = schema.compare_schemas(EXPECTED, EXPECTED)
    assert r["status"] == "PASS"


def test_schema_drift_detects_all_classes():
    r = schema.compare_schemas(EXPECTED, OBSERVED_DRIFTED)
    assert r["status"] == "FAIL"
    assert r["missing"] == ["loyalty_tier"]
    assert r["unexpected"] == ["promo_code"]
    assert r["type_changes"] == ["order_total"]


def test_schema_missing_side_unknown():
    assert schema.compare_schemas(None, OBSERVED_DRIFTED)["status"] == "UNKNOWN"
    assert schema.compare_schemas(EXPECTED, None)["status"] == "UNKNOWN"


def test_schema_policy_configurable():
    r = schema.compare_schemas(
        EXPECTED,
        OBSERVED_DRIFTED,
        {"missing": "WARN", "type_change": "WARN", "unexpected": "WARN", "nullable_change": "WARN"},
    )
    assert r["status"] == "WARN"


# ---- growth -----------------------------------------------------------
def test_growth_projects_with_rate():
    r = growth.project_growth(500.0, 8.0, 365)
    assert r["status"] == "OK"
    assert r["projected_gb"] == 540.0  # 8% annual prorated over 365 days


def test_growth_annual_prorating_is_sane():
    r = growth.project_growth(500.0, 8.0, 365)
    assert r["projected_gb"] < 1000.0  # never trillions from an annual rate


def test_growth_unknown_without_rate():
    r = growth.project_growth(500.0, None, 365)
    assert r["status"] == "UNKNOWN"
    assert r["projected_gb"] is None


def test_growth_unknown_without_current():
    assert growth.project_growth(None, 8.0, 365)["status"] == "UNKNOWN"


# ---- formats ----------------------------------------------------------
def test_format_unknown_triggers():
    assert formats.analyze_format("xml", 500.0)["triggered"] is True


def test_format_csv_large_warns_not_bans():
    r = formats.analyze_format("csv", 600.0)
    assert r["triggered"] is True
    assert r["level"] == "WARN"


def test_format_csv_small_ok():
    assert formats.analyze_format("csv", 10.0)["triggered"] is False


def test_format_parquet_ok_with_guidance():
    r = formats.analyze_format("parquet", 500.0)
    assert r["triggered"] is False
    assert "guidance" in r["observed"]


def test_format_jdbc_na_not_trigger():
    r = formats.analyze_format("unknown", 800.0, source_type="jdbc")
    assert r["triggered"] is False


# ---- jdbc -------------------------------------------------------------
def test_jdbc_large_without_parallel_triggers():
    r = jdbc.analyze_jdbc({"fetch_size": 10000}, 800.0)
    assert r["triggered"] is True


def test_jdbc_documented_parallel_ok():
    assert (
        jdbc.analyze_jdbc({"partition_column": "order_id", "num_partitions": 32}, 800.0)[
            "triggered"
        ]
        is False
    )


def test_jdbc_small_volume_no_finding():
    assert jdbc.analyze_jdbc({}, 10.0)["triggered"] is False


# ---- streaming --------------------------------------------------------
def test_streaming_missing_config_triggers():
    r = streaming.analyze_streaming({"trigger": "x"})
    assert r["triggered"] is True
    assert "checkpoint_location" in r["recommendation"]


def test_streaming_complete_no_trigger_but_unknowns_listed():
    r = streaming.analyze_streaming(
        {"checkpoint_location": "abfss://c", "trigger": "30s", "watermark": "10m"}
    )
    assert r["triggered"] is False
    assert "consumer_lag" in r["unknowns"]


# ---- coverage ---------------------------------------------------------
def test_coverage_counts_unknown_not_fail():
    cps = [
        Checkpoint(checkpoint_id="A", name="a", category="x", status=CheckpointStatus.PASS),
        Checkpoint(checkpoint_id="B", name="b", category="y", status=CheckpointStatus.UNKNOWN),
    ]
    cov = coverage_mod.compute_coverage(cps)
    assert (cov.total_checks, cov.evaluated_checks, cov.unknown_checks) == (2, 1, 1)
    assert cov.coverage_percentage == 50.0
