"""Cross-data-profile tests (mandatory): identical code must yield
different findings against different data evidence. Proves context-awareness.
"""

from __future__ import annotations

from dpif.code import pyspark as detectors
from dpif.code.parser import analyze_source
from dpif.models import CheckpointStatus
from dpif.rules.engine import evaluate_rule


def _data(code_dir, name, **kw):
    base = {
        "text": (code_dir / name).read_text(encoding="utf-8"),
        "source_file": name,
        "pipeline_name": "xdata",
        "assumptions": {},
    }
    base.update(kw)
    return base


def test_collect_10mb_vs_2tb(rules_by_id, code_dir):
    rule = rules_by_id["CODE-PYSPARK-006"]
    small = evaluate_rule(rule, _data(code_dir, "large_collect.py"), {"data_size_gb": 0.01})
    huge = evaluate_rule(rule, _data(code_dir, "large_collect.py"), {"data_size_gb": 2048.0})
    assert small is not None and small.status == CheckpointStatus.WARN
    assert huge is not None and huge.status == CheckpointStatus.FAIL
    assert huge.severity.value == "CRITICAL"
    assert huge.confidence > small.confidence


def test_broadcast_small_vs_large_vs_unknown(code_dir):
    small_ctx = {"data_size_gb": 0.05}
    large_ctx = {"data_size_gb": 20.0}
    small = detectors.analyze_broadcast(
        analyze_source((code_dir / "broadcast_small.py").read_text(), "b.py"), small_ctx
    )
    large = detectors.analyze_broadcast(
        analyze_source((code_dir / "broadcast_small.py").read_text(), "b.py"), large_ctx
    )
    unknown = detectors.analyze_broadcast(
        analyze_source((code_dir / "broadcast_unknown.py").read_text(), "b.py"), {}
    )
    assert small == [], "small broadcast is potentially appropriate: no finding"
    assert large and large[0]["level"] == "WARN"
    assert unknown and unknown[0]["level"] == "WARN"
    assert unknown[0]["confidence"] < large[0]["confidence"]
    assert "cannot be established" in unknown[0]["recommendation"]


def test_topandas_bounded_vs_unbounded_same_volume(rules_by_id, code_dir):
    rule = rules_by_id["CODE-PYSPARK-007"]
    bounded = evaluate_rule(rule, _data(code_dir, "limited_to_pandas.py"), {"data_size_gb": 500.0})
    assert bounded is not None and bounded.status == CheckpointStatus.WARN
    raw = {
        "text": "df = spark.read.parquet('/a')\npdf = df.toPandas()",
        "source_file": "x.py",
        "pipeline_name": "x",
        "assumptions": {},
    }
    unbounded = evaluate_rule(rule, raw, {"data_size_gb": 500.0})
    assert unbounded is not None and unbounded.status == CheckpointStatus.FAIL


def test_join_same_code_small_vs_large(rules_by_id, code_dir):
    rule = rules_by_id["CODE-PYSPARK-008"]
    small = evaluate_rule(rule, _data(code_dir, "expensive_join.py"), {"data_size_gb": 10.0})
    large = evaluate_rule(rule, _data(code_dir, "expensive_join.py"), {"data_size_gb": 500.0})
    assert small is None
    assert large is not None and large.status == CheckpointStatus.WARN


def test_sort_same_code_small_vs_large(rules_by_id, code_dir):
    rule = rules_by_id["CODE-PYSPARK-013"]
    small = evaluate_rule(rule, _data(code_dir, "global_sort.py"), {"data_size_gb": 10.0})
    large = evaluate_rule(rule, _data(code_dir, "global_sort.py"), {"data_size_gb": 500.0})
    assert small is None
    assert large is not None and large.status == CheckpointStatus.WARN


def test_unknown_size_never_invents_gb(rules_by_id, code_dir):
    rule = rules_by_id["CODE-PYSPARK-006"]
    f = evaluate_rule(rule, _data(code_dir, "large_collect.py"), {})
    assert f is not None
    assert "UNKNOWN" in str(f.evidence.evidence)
    assert "GB" not in str(f.evidence.observed.get("input_gb"))
