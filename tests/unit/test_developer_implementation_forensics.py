"""Unit tests for M5E Developer Implementation Forensics.

Tests scenarios A-T covering transformation flow, pre-shuffle optimization,
partitioning, join strategies, cache/checkpoint lifecycle, resource lifecycle,
exception safety, and implementation completeness.
"""

from __future__ import annotations

import pytest

from dpif.analyzers.implementation import DeveloperImplementationAnalyzer
from dpif.code.parser import analyze_source
from dpif.models import CheckpointStatus, Severity
from dpif.models.implementation import EvidenceProvenanceKind, ImplementationDimension


def _analyze(code_text: str, context: dict | None = None):
    code_analysis = analyze_source(code_text, filename="pipeline.py")
    code_analysis._raw_source = code_text
    analyzer = DeveloperImplementationAnalyzer(code_analysis, context=context or {})
    return analyzer.analyze()


# -----------------------------------------------------------------------------
# Scenario A: Python UDF on large dataset
# -----------------------------------------------------------------------------
def test_scenario_a_python_udf_large_data():
    code = """
from pyspark.sql.functions import udf
@udf("string")
def custom_func(x):
    return x.upper() if x else None

df = spark.read.table("large_table")
res = df.withColumn("upper_x", custom_func("x"))
"""
    res = _analyze(code, {"data_size_gb": 250.0, "collection_method": "runtime"})
    dim = res.dimensions[ImplementationDimension.TRANSFORMATION_QUALITY.value]
    assert dim.status == CheckpointStatus.WARN
    assert any(f.rule_id == "IMP-TRANS-001" and f.severity == Severity.HIGH for f in dim.findings)
    assert dim.findings[0].provenance == EvidenceProvenanceKind.RUNTIME


# -----------------------------------------------------------------------------
# Scenario B: Vectorized Pandas UDF
# -----------------------------------------------------------------------------
def test_scenario_b_pandas_udf():
    code = """
import pandas as pd
from pyspark.sql.functions import pandas_udf

@pandas_udf("double")
def add_one(s: pd.Series) -> pd.Series:
    return s + 1.0

df = spark.read.table("t")
res = df.withColumn("v2", add_one("v"))
"""
    res = _analyze(code, {"data_size_gb": 50.0})
    dim = res.dimensions[ImplementationDimension.TRANSFORMATION_QUALITY.value]
    assert dim.status == CheckpointStatus.PASS
    assert any(f.rule_id == "IMP-TRANS-002" and f.status == CheckpointStatus.PASS for f in dim.findings)


# -----------------------------------------------------------------------------
# Scenario C: Pre-join filter optimization
# -----------------------------------------------------------------------------
def test_scenario_c_pre_join_filter():
    code = """
df1 = spark.read.table("t1").filter("status == 'ACTIVE'")
df2 = spark.read.table("t2")
res = df1.join(df2, "id")
"""
    res = _analyze(code, {"data_size_gb": 100.0})
    dim = res.dimensions[ImplementationDimension.SHUFFLE_OPTIMIZATION.value]
    assert dim.status == CheckpointStatus.PASS
    assert any(f.rule_id == "IMP-SHUFFLE-001" for f in dim.findings)


# -----------------------------------------------------------------------------
# Scenario D: Post-join filtering
# -----------------------------------------------------------------------------
def test_scenario_d_post_join_filter():
    code = """
df1 = spark.read.table("t1")
df2 = spark.read.table("t2")
res = df1.join(df2, "id").filter("status == 'ACTIVE'")
"""
    res = _analyze(code, {"data_size_gb": 100.0})
    dim = res.dimensions[ImplementationDimension.SHUFFLE_OPTIMIZATION.value]
    assert dim.status == CheckpointStatus.WARN
    assert any(f.rule_id == "IMP-SHUFFLE-002" for f in dim.findings)


# -----------------------------------------------------------------------------
# Scenario E: Repartition directly before join
# -----------------------------------------------------------------------------
def test_scenario_e_repartition_before_join():
    code = """
df1 = spark.read.table("t1").repartition(200)
df2 = spark.read.table("t2")
res = df1.join(df2, "id")
"""
    res = _analyze(code)
    dim = res.dimensions[ImplementationDimension.SHUFFLE_OPTIMIZATION.value]
    assert any(f.rule_id == "IMP-SHUFFLE-003" for f in dim.findings)


# -----------------------------------------------------------------------------
# Scenario F: Repeated repartition on same frame flow
# -----------------------------------------------------------------------------
def test_scenario_f_repeated_repartition():
    code = """
df = spark.read.table("t1").repartition(100)
df2 = df.repartition(200)
"""
    res = _analyze(code)
    dim = res.dimensions[ImplementationDimension.PARTITIONING_QUALITY.value]
    assert dim.status == CheckpointStatus.WARN
    assert any(f.rule_id == "IMP-PART-001" for f in dim.findings)


# -----------------------------------------------------------------------------
# Scenario G: coalesce(1) before aggregation (FAIL)
# -----------------------------------------------------------------------------
def test_scenario_g_coalesce_before_agg():
    code = """
df = spark.read.table("t1").coalesce(1)
res = df.groupBy("key").count()
"""
    res = _analyze(code)
    dim = res.dimensions[ImplementationDimension.PARTITIONING_QUALITY.value]
    assert dim.status == CheckpointStatus.FAIL
    assert any(f.rule_id == "IMP-PART-002" and f.severity == Severity.HIGH for f in dim.findings)


# -----------------------------------------------------------------------------
# Scenario H: coalesce(1) before write
# -----------------------------------------------------------------------------
def test_scenario_h_coalesce_before_write():
    code = """
df = spark.read.table("t1").groupBy("key").count()
df.coalesce(1).write.parquet("/tmp/out")
"""
    res = _analyze(code, {"data_size_gb": 0.2})
    dim = res.dimensions[ImplementationDimension.PARTITIONING_QUALITY.value]
    assert any(f.rule_id == "IMP-PART-003" and f.status == CheckpointStatus.PASS for f in dim.findings)


# -----------------------------------------------------------------------------
# Scenario I: Broadcast join volume UNKNOWN
# -----------------------------------------------------------------------------
def test_scenario_i_broadcast_unknown_volume():
    code = """
from pyspark.sql.functions import broadcast
df1 = spark.read.table("t1")
df2 = spark.read.table("t2")
res = df1.join(broadcast(df2), "id")
"""
    res = _analyze(code, {})  # no data_size_gb
    dim = res.dimensions[ImplementationDimension.JOIN_STRATEGY.value]
    assert any(f.rule_id == "IMP-JOIN-001" and f.status == CheckpointStatus.UNKNOWN for f in dim.findings)


# -----------------------------------------------------------------------------
# Scenario J: Broadcast join supported by runtime evidence
# -----------------------------------------------------------------------------
def test_scenario_j_broadcast_runtime_evidence():
    code = """
from pyspark.sql.functions import broadcast
df1 = spark.read.table("t1")
df2 = spark.read.table("t2")
res = df1.join(broadcast(df2), "id")
"""
    res = _analyze(code, {"data_size_gb": 5.0, "evidence_source": "runtime metrics"})
    dim = res.dimensions[ImplementationDimension.JOIN_STRATEGY.value]
    assert any(f.rule_id == "IMP-JOIN-002" and f.status == CheckpointStatus.PASS for f in dim.findings)


# -----------------------------------------------------------------------------
# Scenario K: Cross join on large dataset
# -----------------------------------------------------------------------------
def test_scenario_k_cross_join_large_data():
    code = """
df1 = spark.read.table("t1")
df2 = spark.read.table("t2")
res = df1.crossJoin(df2)
"""
    res = _analyze(code, {"data_size_gb": 150.0})
    dim = res.dimensions[ImplementationDimension.JOIN_STRATEGY.value]
    assert dim.status == CheckpointStatus.FAIL
    assert any(f.rule_id == "IMP-JOIN-004" and f.severity == Severity.HIGH for f in dim.findings)


# -----------------------------------------------------------------------------
# Scenario L: Optimal cache lifecycle (cache + multi-use + unpersist)
# -----------------------------------------------------------------------------
def test_scenario_l_optimal_cache_lifecycle():
    code = """
df = spark.read.table("t1").cache()
c1 = df.count()
c2 = df.collect()
df.unpersist()
"""
    res = _analyze(code)
    dim = res.dimensions[ImplementationDimension.CACHE_LIFECYCLE.value]
    assert dim.status == CheckpointStatus.PASS
    assert any(f.rule_id == "IMP-CACHE-001" for f in dim.findings)


# -----------------------------------------------------------------------------
# Scenario M: Single-use cache
# -----------------------------------------------------------------------------
def test_scenario_m_single_use_cache():
    code = """
df = spark.read.table("t1").cache()
c1 = df.count()
"""
    res = _analyze(code)
    dim = res.dimensions[ImplementationDimension.CACHE_LIFECYCLE.value]
    assert any(f.rule_id == "IMP-CACHE-002" and f.status == CheckpointStatus.WARN for f in dim.findings)


# -----------------------------------------------------------------------------
# Scenario N: Cache without unpersist
# -----------------------------------------------------------------------------
def test_scenario_n_cache_without_unpersist():
    code = """
df = spark.read.table("t1").cache()
c1 = df.count()
c2 = df.collect()
"""
    res = _analyze(code)
    dim = res.dimensions[ImplementationDimension.CACHE_LIFECYCLE.value]
    assert any(f.rule_id == "IMP-CACHE-003" and f.status == CheckpointStatus.WARN for f in dim.findings)


# -----------------------------------------------------------------------------
# Scenario O: Checkpoint materialized with downstream usage
# -----------------------------------------------------------------------------
def test_scenario_o_checkpoint_materialized():
    code = """
df = spark.read.table("t1").localCheckpoint()
c1 = df.count()
"""
    res = _analyze(code)
    dim = res.dimensions[ImplementationDimension.CHECKPOINT_LIFECYCLE.value]
    assert dim.status == CheckpointStatus.PASS
    assert any(f.rule_id == "IMP-CKPT-001" for f in dim.findings)


# -----------------------------------------------------------------------------
# Scenario P: Resource context manager (with)
# -----------------------------------------------------------------------------
def test_scenario_p_resource_with_context_manager():
    code = """
with open("/tmp/file.txt", "w") as f:
    f.write("hello")
"""
    res = _analyze(code)
    dim = res.dimensions[ImplementationDimension.RESOURCE_LIFECYCLE.value]
    assert dim.status == CheckpointStatus.PASS
    assert any(f.rule_id == "IMP-RES-001" for f in dim.findings)


# -----------------------------------------------------------------------------
# Scenario Q: Resource closed inside try/finally block
# -----------------------------------------------------------------------------
def test_scenario_q_resource_try_finally():
    code = """
f = open("/tmp/file.txt", "w")
try:
    f.write("data")
finally:
    f.close()
"""
    res = _analyze(code)
    dim = res.dimensions[ImplementationDimension.RESOURCE_LIFECYCLE.value]
    assert dim.status == CheckpointStatus.PASS
    assert any(f.rule_id == "IMP-RES-002" for f in dim.findings)


# -----------------------------------------------------------------------------
# Scenario R: Swallowed / bare exception handler (FAIL)
# -----------------------------------------------------------------------------
def test_scenario_r_swallowed_exception():
    code = """
try:
    spark.read.table("t1")
except Exception:
    pass
"""
    res = _analyze(code)
    dim = res.dimensions[ImplementationDimension.EXCEPTION_SAFETY.value]
    assert dim.status == CheckpointStatus.FAIL
    assert any(f.rule_id == "IMP-EXC-001" and f.severity == Severity.HIGH for f in dim.findings)


# -----------------------------------------------------------------------------
# Scenario S: Specific exception handler with logging
# -----------------------------------------------------------------------------
def test_scenario_s_specific_exception_handling():
    code = """
import logging
try:
    spark.read.table("t1")
except ValueError as e:
    logging.error("Failed to read: %s", e)
    raise
"""
    res = _analyze(code)
    dim = res.dimensions[ImplementationDimension.EXCEPTION_SAFETY.value]
    assert dim.status == CheckpointStatus.PASS
    assert any(f.rule_id == "IMP-EXC-003" for f in dim.findings)


# -----------------------------------------------------------------------------
# Scenario T: NotImplementedError & TODO markers
# -----------------------------------------------------------------------------
def test_scenario_t_completeness_markers():
    code = """
# TODO: implement retry logic
def process():
    raise NotImplementedError("Pending feature")
"""
    res = _analyze(code)
    dim = res.dimensions[ImplementationDimension.IMPLEMENTATION_COMPLETENESS.value]
    assert dim.status == CheckpointStatus.WARN
    assert any(f.rule_id == "IMP-COMP-001" for f in dim.findings)
    assert any(f.rule_id == "IMP-COMP-002" for f in dim.findings)
