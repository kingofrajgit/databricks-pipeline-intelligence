"""AST analyzer tests: structure, locations, arbitrary df names, flow,
loops, secrets/paths, complexity, windows, joins, error tolerance."""

from __future__ import annotations

from dpif.code import flow as flow_mod
from dpif.code.models import AnalysisContext, OperationType
from dpif.code.parser import analyze_source


def test_functions_imports_detected_with_lines():
    code = (
        "import logging\n"
        "from pyspark.sql import functions as F\n"
        "\n"
        "def clean(df):\n"
        "    if df is None:\n"
        "        return df\n"
        "    return df.filter(F.col('x') > 1)\n"
    )
    a = analyze_source(code, "sample.py")
    assert a.parse_error is None
    assert [f.name for f in a.functions] == ["clean"]
    assert a.functions[0].line == 4
    assert set(a.imports) >= {"logging", "pyspark"}
    assert a.functions[0].branch_count >= 1


def test_operations_carry_line_numbers(code_dir):
    a = analyze_source((code_dir / "bad_pipeline.py").read_text(), "bad_pipeline.py")
    by_type = {}
    for op in a.operations:
        by_type.setdefault(op.operation_type, []).append(op.line)
    assert by_type[OperationType.COLLECT] == [9]
    assert by_type[OperationType.TO_PANDAS] == [12]
    assert by_type[OperationType.READ] == [6]
    assert all(op.column >= 0 for op in a.operations)


def test_arbitrary_dataframe_names_tracked():
    code = (
        "transactions = spark.read.parquet('/a')\n"
        "customer_df = spark.read.parquet('/b')\n"
        "result = transactions.join(customer_df, on='id', how='inner')\n"
        "result.write.save('/out')\n"
    )
    a = analyze_source(code, "names.py")
    assert set(a.dataframe_variables) >= {"transactions", "customer_df", "result"}
    joins = a.of_type(OperationType.JOIN)
    assert len(joins) == 1 and joins[0].dataframe == "transactions"


def test_flow_chains_ordered(code_dir):
    a = analyze_source((code_dir / "good_pipeline.py").read_text(), "good_pipeline.py")
    flow = flow_mod.build_flow(a)
    # Operations record their receiver: FILTER acts on df, WRITE on deduped.
    assert flow["df"] == ["FILTER"]
    assert flow["deduped"][-1] == "WRITE"
    assert flow["watermarked"] == ["DROP_DUPLICATES"]


def test_driver_loop_after_collect_detected():
    code = "df = spark.read.parquet('/a')\nrows = df.collect()\nfor row in rows:\n    print(row)\n"
    a = analyze_source(code, "loop.py")
    assert a.driver_loops and a.driver_loops[0]["variable"] == "rows"
    assert a.driver_loops[0]["collected_via"] == "collect"


def test_secrets_masked_paths_located():
    code = 'password = "hunter2-hunter2-hunter2"\ndf.write.parquet("/tmp/out")\n'
    a = analyze_source(code, "sec.py")
    assert [(s.line, s.kind) for s in a.secrets] == [(1, "credential-variable")]
    assert all("hunter2" not in s.model_dump_json() for s in a.secrets)
    assert [(p.line, p.value) for p in a.hardcoded_paths] == [(2, "/tmp/out")]


def test_complexity_metrics(code_dir):
    a = analyze_source((code_dir / "good_pipeline.py").read_text(), "good_pipeline.py")
    assert a.lines_of_code > 0
    assert a.function_count == 0
    assert a.has_exception_handling is True
    assert a.max_nesting_depth >= 1


def test_syntax_error_never_raises():
    a = analyze_source("def broken(:\n  pass", "broken.py")
    assert a.parse_error is not None
    assert a.operations == []


def test_window_bounded_flag():
    bounded = (
        "from pyspark.sql import Window\n"
        "spec = Window.partitionBy('a').orderBy('b').rowsBetween(-5, 0)\n"
    )
    unbounded = "from pyspark.sql import Window\nspec = Window.partitionBy('a').orderBy('b')\n"
    b_ops = analyze_source(bounded, "b.py").of_type(OperationType.WINDOW)
    u_ops = analyze_source(unbounded, "u.py").of_type(OperationType.WINDOW)
    assert b_ops and all(o.arguments.get("bounded") is True for o in b_ops)
    assert u_ops and all(o.arguments.get("bounded") is not True for o in u_ops)


def test_join_type_and_keys_recorded(code_dir):
    a = analyze_source((code_dir / "bad_pipeline.py").read_text(), "bad_pipeline.py")
    joins = a.of_type(OperationType.JOIN)
    assert len(joins) == 1
    assert joins[0].arguments["how"] == "cross"
    assert "limit(100)" in str(joins[0].arguments["other"])


def test_broadcast_and_udf_classification(code_dir):
    a = analyze_source((code_dir / "broadcast_small.py").read_text(), "b.py")
    bcast = a.of_type(OperationType.BROADCAST)
    assert len(bcast) == 1 and bcast[0].dataframe == "customer_df"
    bad = analyze_source((code_dir / "bad_pipeline.py").read_text(), "bad.py")
    assert bad.of_type(OperationType.UDF) and not bad.of_type(OperationType.PANDAS_UDF)
    deco = analyze_source("@pandas_udf('double')\ndef f(x):\n return x\n", "d.py")
    assert deco.of_type(OperationType.PANDAS_UDF)


def test_analysis_context_flattens_for_rules():
    ctx = AnalysisContext(
        pipeline_name="p",
        source_type="adls",
        expected_volume_gb=500.0,
        processing_type="batch",
        evidence_source="fixture metadata",
        collection_method="fixture",
    )
    flat = ctx.to_rule_context()
    assert flat["data_size_gb"] == 500.0
    assert flat["source_type"] == "adls"
    assert "peak_volume_gb" not in flat  # Nones are dropped, never invented
