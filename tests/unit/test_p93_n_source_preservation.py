"""P9-3 focused tests: N-source preservation (no first-READ collapse).

The authoritative representation is the N-source collection from
``extract_sources_from_code``. The legacy singular ``_extract_source_from_code``
remains a compatibility projection only and must never feed per-operation
volume attribution. Preserved sources carry no volume (UNKNOWN by
construction); no pipeline baseline is copied into them.
"""

from __future__ import annotations

from dpif.code.parser import analyze_source
from dpif.discovery.synthesis import (
    _extract_source_from_code,
    extract_sources_from_code,
    synthesize_discovered_contract,
)
from dpif.flow.builder import build_pipeline_flow_graph

SRC_A = "abfss://lake@acct.dfs.core.windows.net/bronze/a"
SRC_B = "abfss://lake@acct.dfs.core.windows.net/bronze/b"
SRC_C = "abfss://lake@acct.dfs.core.windows.net/bronze/c"

THREE_SOURCE_CODE = (
    f"df_a = spark.read.parquet('{SRC_A}')\n"
    f"df_b = spark.read.parquet('{SRC_B}')\n"
    f"df_c = spark.read.parquet('{SRC_C}')\n"
    "joined = df_a.join(df_b, on='id').join(df_c, on='id')\n"
)


def _analysis(code: str):
    return analyze_source(code, filename="multi.py")


# 1. Single-source input produces one authoritative source.
def test_single_source_yields_one_entry():
    code = f"df = spark.read.parquet('{SRC_A}')"
    sources = extract_sources_from_code(_analysis(code), code)
    assert len(sources) == 1
    assert sources[0].path == SRC_A


# 2/3. Three-source and N-source fixtures preserve every source.
def test_three_sources_preserved():
    sources = extract_sources_from_code(_analysis(THREE_SOURCE_CODE), THREE_SOURCE_CODE)
    assert [s.path for s in sources] == [SRC_A, SRC_B, SRC_C]


def test_n_sources_preserved_in_order():
    paths = [f"abfss://lake@acct.dfs.core.windows.net/r{i}" for i in range(5)]
    code = "".join(f"df{i} = spark.read.parquet('{p}')\n" for i, p in enumerate(paths))
    sources = extract_sources_from_code(_analysis(code), code)
    assert [s.path for s in sources] == paths


# 4. Non-first sources are never dropped.
def test_non_first_sources_present():
    sources = extract_sources_from_code(_analysis(THREE_SOURCE_CODE), THREE_SOURCE_CODE)
    paths = [s.path for s in sources]
    assert SRC_B in paths and SRC_C in paths


# 10. Identical repeated reads are not collapsed (no identity rule exists).
def test_identical_reads_not_collapsed():
    code = f"df1 = spark.read.parquet('{SRC_A}')\ndf2 = spark.read.parquet('{SRC_A}')\n"
    sources = extract_sources_from_code(_analysis(code), code)
    assert len(sources) == 2
    assert all(s.path == SRC_A for s in sources)


# 5/6. Legacy singular projection: available, first-READ, and NOT authoritative.
def test_legacy_singular_is_first_read_projection_only():
    analysis = _analysis(THREE_SOURCE_CODE)
    single = _extract_source_from_code(analysis, THREE_SOURCE_CODE)
    assert single is not None
    assert single.path == SRC_A
    assert single.source_id == "discovered_source"
    plural = extract_sources_from_code(analysis, THREE_SOURCE_CODE)
    assert len(plural) == 3
    # The projection carries no collection: it cannot see B or C.
    assert single.path != SRC_B and single.path != SRC_C
    # Authoritative entries are distinctly identified.
    assert [s.source_id for s in plural] == [
        "discovered_source_1",
        "discovered_source_2",
        "discovered_source_3",
    ]


# 7. The collection (not the projection) is what attribution receives.
def test_collection_shape_suits_attribution():
    sources = extract_sources_from_code(_analysis(THREE_SOURCE_CODE), THREE_SOURCE_CODE)
    assert isinstance(sources, list)
    assert all(hasattr(s, "path") and hasattr(s, "type") for s in sources)


# 8/9. Preserved sources default to UNKNOWN volume; no baseline copied.
def test_preserved_sources_carry_no_volume():
    sources = extract_sources_from_code(_analysis(THREE_SOURCE_CODE), THREE_SOURCE_CODE)
    for source in sources:
        assert source.expected_volume_gb is None
        assert source.peak_volume_gb is None
        assert source.growth_rate_percent is None


# 11. Existing contract consumers remain semantically compatible.
def test_contract_synthesis_still_uses_first_source():
    analysis = _analysis(THREE_SOURCE_CODE)
    contract = synthesize_discovered_contract(
        code_analysis=analysis,
        raw_code=THREE_SOURCE_CODE,
        pipeline_name="multi",
    )
    assert contract is not None
    assert contract.source.path == SRC_A


# 12. P9-2 tagging and combined analysis unchanged by P9-3.
def test_tagging_helpers_unaffected():
    from dpif.code.parser import parse_task_boundaries, tag_operations_with_tasks

    blob = "# --- TASK: t1 (/a) ---\n" + THREE_SOURCE_CODE
    analysis = analyze_source(blob, filename="combined.py")
    assert tag_operations_with_tasks(analysis, parse_task_boundaries(blob)) > 0
    assert {op.task_key for op in analysis.operations} == {"t1"}


# 13/14. End-to-end: synthesis collection → flow builder contains A, B, C
# with unchanged topology, and no duplicate SOURCE nodes are introduced.
def test_end_to_end_multi_source_flow_graph():
    analysis = _analysis(THREE_SOURCE_CODE)
    sources = extract_sources_from_code(analysis, THREE_SOURCE_CODE)
    assert len(sources) == 3
    graph = build_pipeline_flow_graph(
        code_analysis=analysis,
        contract=None,
        pipeline_name="multi",
        raw_code=THREE_SOURCE_CODE,
    )
    source_nodes = [n for n in graph.nodes if n.kind.value == "SOURCE"]
    names = {n.dataset.name for n in source_nodes if n.dataset is not None}
    assert SRC_A in names and SRC_B in names and SRC_C in names
    # One SOURCE node per READ (no duplicates fabricated, none dropped).
    assert len(source_nodes) == 3
    # Volumes untouched by P9-3: all None (UNKNOWN), never zero-filled.
    assert all(n.volume is None for n in source_nodes)
