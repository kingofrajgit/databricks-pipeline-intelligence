"""Unit tests for Phase 2 per-operation data volume intelligence (GAP-002).

Covers (per approved scope A-N):
- VolumeObservation model: known / derived / unknown semantics
- Zero preservation (genuine 0 stays 0) vs UNKNOWN (missing stays None)
- SOURCE / TARGET pipeline baselines via extract_baseline_volume
- Offline + online flow graph volume via the SAME common builder
- Offline/online parity for identical evidence
- writeStream detection (parser prerequisite)
- Zero-collapse regression cases (synthesis placeholders, M5H checks)
- Phase 1 behavior unchanged (intermediate nodes keep volume=None)
"""

from __future__ import annotations

from dpif.analyzers.sufficiency import EvidenceSufficiencyAnalyzer
from dpif.code.models import OperationType
from dpif.code.parser import analyze_source
from dpif.discovery.synthesis import synthesize_discovered_data_profile
from dpif.flow import (
    EvidenceState,
    FlowNode,
    FlowNodeKind,
    VolumeObservation,
    build_pipeline_flow_graph,
)
from dpif.flow.models import FlowProvenance
from dpif.models import CollectionMethod, DataProfile, PipelineContract, Source, SourceType, Target
from dpif.models.implementation import EvidenceProvenanceKind
from dpif.reporting.validation_reports import _append_flow_graph_section, _format_volume
from dpif.runtime.models import RuntimeRun, RuntimeStage

_GB = 1024.0**3

_SIMPLE_CODE = (
    "df = spark.read.table('bronze.orders')\n"
    "f = df.filter(\"amount > 0\")\n"
    "f.write.saveAsTable('silver.orders')\n"
)


def _contract(expected_gb: float | None = None) -> PipelineContract:
    kwargs: dict[str, object] = {
        "pipeline_name": "vol",
        "source": Source(source_id="s", type=SourceType.OTHER, path="bronze.orders"),
        "target": Target(target_id="t", path="silver.orders"),
    }
    if expected_gb is not None:
        kwargs["expected_daily_volume_gb"] = expected_gb
    return PipelineContract.model_construct(**kwargs)


def _fixture_profile() -> DataProfile:
    return DataProfile(
        total_bytes=int(100 * _GB),
        total_gb=100.0,
        file_count=10,
        average_file_size_kb=10.0 * 1024 * 1024,
        record_count=1_000_000,
        partition_count=4,
        collection_method=CollectionMethod.FIXTURE,
        evidence_source="fixture metadata",
    )


def _runtime_run(
    in_bytes: int = 0, out_bytes: int = 0, with_stages: bool = True
) -> RuntimeRun:
    stages = (
        [RuntimeStage(stage_id=1, input_bytes=in_bytes, output_bytes=out_bytes)]
        if with_stages
        else []
    )
    return RuntimeRun(run_id="r1", stages=stages)


# ==============================================================================
# A. VolumeObservation model
# ==============================================================================


def test_volume_observation_defaults_unknown():
    v = VolumeObservation()
    assert v.input_bytes is None
    assert v.output_bytes is None
    assert v.input_rows is None
    assert v.output_rows is None
    assert v.file_count is None
    assert v.status == EvidenceState.UNKNOWN
    assert v.is_known is False


def test_volume_observation_no_new_provenance_vocab():
    v = VolumeObservation(
        input_bytes=10,
        provenance=FlowProvenance(
            kind=EvidenceProvenanceKind.RUNTIME, state=EvidenceState.KNOWN
        ),
    )
    assert isinstance(v.provenance.kind, EvidenceProvenanceKind)
    assert isinstance(v.status, EvidenceState)


# ==============================================================================
# B/C. Known / derived volume
# ==============================================================================


def test_volume_observation_known_runtime():
    v = VolumeObservation(
        input_bytes=int(12 * _GB),
        provenance=FlowProvenance(
            kind=EvidenceProvenanceKind.RUNTIME, state=EvidenceState.KNOWN
        ),
    )
    assert v.status == EvidenceState.KNOWN
    assert v.is_known is True
    assert v.model_dump(mode="json")["input_bytes"] == int(12 * _GB)


def test_volume_observation_derived_fixture():
    v = VolumeObservation(
        input_bytes=int(100 * _GB),
        input_rows=1_000_000,
        file_count=10,
        provenance=FlowProvenance(
            kind=EvidenceProvenanceKind.FIXTURE, state=EvidenceState.DERIVED
        ),
    )
    assert v.status == EvidenceState.DERIVED
    assert v.is_known is True


# ==============================================================================
# D/F. Unknown volume (None, never fabricated)
# ==============================================================================


def test_flow_node_volume_defaults_none():
    node = FlowNode(node_id="s1", kind=FlowNodeKind.SOURCE)
    assert node.volume is None


def test_builder_without_volume_evidence_leaves_volume_none():
    g = build_pipeline_flow_graph(
        code_analysis=analyze_source(_SIMPLE_CODE, filename="p.py"),
        pipeline_name="p",
        raw_code=_SIMPLE_CODE,
    )
    assert g.source_ids and g.target_ids
    for node in g.nodes:
        assert node.volume is None


def test_no_per_operation_volume_estimated():
    """Intermediate nodes never carry invented byte/row deltas."""
    g = build_pipeline_flow_graph(
        code_analysis=analyze_source(_SIMPLE_CODE, filename="p.py"),
        contract=_contract(expected_gb=500.0),
        pipeline_name="p",
        raw_code=_SIMPLE_CODE,
    )
    for node in g.nodes:
        if node.kind in (FlowNodeKind.OPERATION, FlowNodeKind.DATASET, FlowNodeKind.SHUFFLE):
            assert node.volume is None


# ==============================================================================
# E. Zero preserved when genuinely evidenced; missing stays UNKNOWN
# ==============================================================================


def test_volume_zero_is_not_unknown_at_model_level():
    v = VolumeObservation(
        output_bytes=0,
        provenance=FlowProvenance(
            kind=EvidenceProvenanceKind.RUNTIME, state=EvidenceState.KNOWN
        ),
    )
    assert v.output_bytes == 0
    assert v.output_bytes is not None
    assert v.is_known is True


def test_measured_zero_output_preserved_on_target():
    """Stages observed with 0 output bytes = genuinely empty, not UNKNOWN."""
    g = build_pipeline_flow_graph(
        code_analysis=analyze_source(_SIMPLE_CODE, filename="p.py"),
        pipeline_name="p",
        raw_code=_SIMPLE_CODE,
        runtime_run=_runtime_run(in_bytes=1000, out_bytes=0, with_stages=True),
    )
    targets = g.nodes_of_kind(FlowNodeKind.TARGET)
    assert len(targets) == 1
    assert targets[0].volume is not None
    assert targets[0].volume.output_bytes == 0


def test_missing_stages_leave_target_volume_unknown():
    g = build_pipeline_flow_graph(
        code_analysis=analyze_source(_SIMPLE_CODE, filename="p.py"),
        pipeline_name="p",
        raw_code=_SIMPLE_CODE,
        runtime_run=_runtime_run(with_stages=False),
    )
    for node in g.nodes_of_kind(FlowNodeKind.TARGET):
        assert node.volume is None


# ==============================================================================
# G. SOURCE volume (contract baseline)
# ==============================================================================


def test_source_volume_from_contract_baseline():
    g = build_pipeline_flow_graph(
        code_analysis=analyze_source(_SIMPLE_CODE, filename="p.py"),
        contract=_contract(expected_gb=500.0),
        pipeline_name="p",
        raw_code=_SIMPLE_CODE,
    )
    sources = g.nodes_of_kind(FlowNodeKind.SOURCE)
    assert len(sources) == 1
    vol = sources[0].volume
    assert vol is not None
    assert vol.input_bytes == int(500.0 * _GB)
    assert vol.provenance.kind == EvidenceProvenanceKind.CONTRACT
    assert vol.provenance.state == EvidenceState.KNOWN
    assert vol.status == EvidenceState.KNOWN


def test_source_volume_runtime_takes_precedence_over_contract():
    g = build_pipeline_flow_graph(
        code_analysis=analyze_source(_SIMPLE_CODE, filename="p.py"),
        contract=_contract(expected_gb=500.0),
        pipeline_name="p",
        raw_code=_SIMPLE_CODE,
        runtime_run=_runtime_run(in_bytes=int(200 * _GB), out_bytes=10),
    )
    vol = g.nodes_of_kind(FlowNodeKind.SOURCE)[0].volume
    assert vol is not None
    assert vol.input_bytes == int(200 * _GB)
    assert vol.provenance.kind == EvidenceProvenanceKind.RUNTIME


# ==============================================================================
# H. TARGET volume (measured runtime output)
# ==============================================================================


def test_target_volume_from_measured_runtime_output():
    out = int(12 * _GB)
    g = build_pipeline_flow_graph(
        code_analysis=analyze_source(_SIMPLE_CODE, filename="p.py"),
        pipeline_name="p",
        raw_code=_SIMPLE_CODE,
        runtime_run=_runtime_run(in_bytes=int(200 * _GB), out_bytes=out),
    )
    vol = g.nodes_of_kind(FlowNodeKind.TARGET)[0].volume
    assert vol is not None
    assert vol.output_bytes == out
    assert vol.provenance.kind == EvidenceProvenanceKind.RUNTIME
    assert vol.status == EvidenceState.KNOWN


# ==============================================================================
# I/J. Offline (fixture profile) / online (runtime) graph volume
# ==============================================================================


def test_offline_fixture_profile_volume_derived():
    g = build_pipeline_flow_graph(
        code_analysis=analyze_source(_SIMPLE_CODE, filename="p.py"),
        contract=_contract(),
        pipeline_name="p",
        raw_code=_SIMPLE_CODE,
        data_profile=_fixture_profile(),
    )
    vol = g.nodes_of_kind(FlowNodeKind.SOURCE)[0].volume
    assert vol is not None
    assert vol.input_bytes == int(100.0 * _GB)
    assert vol.input_rows == 1_000_000
    assert vol.file_count == 10
    assert vol.provenance.kind == EvidenceProvenanceKind.FIXTURE
    assert vol.status == EvidenceState.DERIVED


def test_online_runtime_profile_volume():
    profile = DataProfile(
        total_bytes=int(200 * _GB),
        total_gb=200.0,
        record_count=0,
        collection_method=CollectionMethod.RUNTIME,
        evidence_source="DATABRICKS_API",
    )
    g = build_pipeline_flow_graph(
        code_analysis=analyze_source(_SIMPLE_CODE, filename="p.py"),
        contract=_contract(),
        pipeline_name="job-1",
        raw_code=_SIMPLE_CODE,
        data_profile=profile,
    )
    vol = g.nodes_of_kind(FlowNodeKind.SOURCE)[0].volume
    assert vol is not None
    assert vol.input_bytes == int(200.0 * _GB)
    # No rows evidenced, no fixture file distribution: stay UNKNOWN.
    assert vol.input_rows is None
    assert vol.file_count is None


# ==============================================================================
# K. Offline/online parity for identical evidence
# ==============================================================================


def test_offline_online_volume_parity():
    kwargs: dict[str, object] = {
        "code_analysis": analyze_source(_SIMPLE_CODE, filename="p.py"),
        "contract": _contract(expected_gb=500.0),
        "pipeline_name": "parity",
        "raw_code": _SIMPLE_CODE,
        "data_profile": _fixture_profile(),
        "runtime_run": _runtime_run(in_bytes=int(200 * _GB), out_bytes=42),
    }
    offline_graph = build_pipeline_flow_graph(**kwargs)  # type: ignore[arg-type]
    online_graph = build_pipeline_flow_graph(**kwargs)  # type: ignore[arg-type]
    for kind in (FlowNodeKind.SOURCE, FlowNodeKind.TARGET):
        off = offline_graph.nodes_of_kind(kind)
        on = online_graph.nodes_of_kind(kind)
        assert len(off) == len(on)
        for o_node, n_node in zip(off, on, strict=True):
            assert (o_node.volume is None) == (n_node.volume is None)
            if o_node.volume is not None and n_node.volume is not None:
                assert o_node.volume.model_dump(mode="json") == n_node.volume.model_dump(
                    mode="json"
                )


# ==============================================================================
# L. writeStream detection (parser prerequisite)
# ==============================================================================


def test_writestream_start_detected_as_write():
    code = (
        'df2 = df.writeStream.format("delta")'
        '.option("checkpointLocation", "/chk").start("/out/path")\n'
    )
    analysis = analyze_source(code, filename="w.py")
    writes = [o for o in analysis.operations if o.operation_type == OperationType.WRITE]
    assert len(writes) == 1
    assert writes[0].arguments.get("via") == "start"


def test_writestream_graph_target_identity():
    code = 'df.writeStream.format("delta").option("checkpointLocation", "/chk").start("/out/path")\n'
    g = build_pipeline_flow_graph(
        code_analysis=analyze_source(code, filename="w.py"),
        pipeline_name="w",
        raw_code=code,
    )
    targets = g.nodes_of_kind(FlowNodeKind.TARGET)
    assert len(targets) == 1
    assert targets[0].dataset is not None
    assert targets[0].dataset.name == "/out/path"


def test_writestream_totable_target_identity():
    code = 'df.writeStream.format("delta").toTable("live.events")\n'
    g = build_pipeline_flow_graph(
        code_analysis=analyze_source(code, filename="w.py"),
        pipeline_name="w",
        raw_code=code,
    )
    targets = g.nodes_of_kind(FlowNodeKind.TARGET)
    assert len(targets) == 1
    assert targets[0].dataset is not None
    assert targets[0].dataset.name == "live.events"


def test_writestream_without_path_stays_unknown():
    code = 'q = df.writeStream.format("memory").start()\n'
    g = build_pipeline_flow_graph(
        code_analysis=analyze_source(code, filename="w.py"),
        pipeline_name="w",
        raw_code=code,
    )
    targets = g.nodes_of_kind(FlowNodeKind.TARGET)
    assert len(targets) == 1
    assert targets[0].dataset is not None
    assert targets[0].dataset.name == "UNKNOWN"


def test_batch_write_detection_unchanged():
    """Phase 1 batch write behavior is untouched by the writeStream fix."""
    code = "df.write.mode('overwrite').saveAsTable('silver.t')\n"
    analysis = analyze_source(code, filename="b.py")
    writes = [o for o in analysis.operations if o.operation_type == OperationType.WRITE]
    assert len(writes) == 1
    assert writes[0].arguments.get("via") == "saveAsTable"


# ==============================================================================
# M. Zero-collapse regression cases
# ==============================================================================


def test_synthesis_does_not_use_task_count_as_rows():
    run = RuntimeRun(run_id="r1", stages=[RuntimeStage(stage_id=1, task_count=8)])
    profile = synthesize_discovered_data_profile(runtime_run=run, table_profile=None)
    assert profile is None or profile.record_count == 0


def test_synthesis_does_not_invent_file_counts():
    run = _runtime_run(in_bytes=int(50 * _GB), out_bytes=0)
    profile = synthesize_discovered_data_profile(runtime_run=run, table_profile=None)
    assert profile is not None
    assert profile.file_count == 0
    assert profile.average_file_size_kb == 0.0


def test_m5h_empty_runtime_has_no_phantom_task_credit():
    analyzer = EvidenceSufficiencyAnalyzer()
    coverage = analyzer._evaluate_runtime_domain(RuntimeRun(run_id="empty"))
    assert "task_count" in coverage.evidence_unavailable
    assert "bytes_read" in coverage.evidence_unavailable
    assert "bytes_written" in coverage.evidence_unavailable
    assert "spill_metrics" in coverage.evidence_unavailable
    assert coverage.decision_sufficient is False


def test_m5h_measured_runtime_bytes_credited():
    analyzer = EvidenceSufficiencyAnalyzer()
    run = _runtime_run(in_bytes=1000, out_bytes=500)
    # RuntimeRun has no duration evidence here; bytes must still be credited.
    coverage = analyzer._evaluate_runtime_domain(run)
    assert "bytes_read" in coverage.evidence_available
    assert "bytes_written" in coverage.evidence_available


# ==============================================================================
# Reporting: JSON + markdown volume display
# ==============================================================================


def test_volume_serializes_to_json_payload():
    g = build_pipeline_flow_graph(
        code_analysis=analyze_source(_SIMPLE_CODE, filename="p.py"),
        contract=_contract(expected_gb=500.0),
        pipeline_name="p",
        raw_code=_SIMPLE_CODE,
    )
    payload = g.to_dict()
    assert isinstance(payload, dict)
    src_payload = next(n for n in payload["nodes"] if n["kind"] == "SOURCE")
    assert src_payload["volume"]["input_bytes"] == int(500.0 * _GB)
    assert src_payload["volume"]["provenance"]["kind"] == "CONTRACT"


def test_format_volume_unknown_renders_unknown_not_zero():
    assert _format_volume(None) == "UNKNOWN (no volume evidence)"
    assert "UNKNOWN" in _format_volume(VolumeObservation())
    assert " 0 B" not in _format_volume(VolumeObservation())


def test_format_volume_known_renders_quantities():
    text = _format_volume(
        VolumeObservation(
            input_bytes=int(12 * _GB),
            input_rows=100,
            provenance=FlowProvenance(
                kind=EvidenceProvenanceKind.RUNTIME, state=EvidenceState.KNOWN
            ),
        )
    )
    assert str(int(12 * _GB)) in text
    assert "input_rows=100" in text
    assert "RUNTIME" in text


def test_markdown_section_reports_source_target_volume():
    g = build_pipeline_flow_graph(
        code_analysis=analyze_source(_SIMPLE_CODE, filename="p.py"),
        contract=_contract(expected_gb=500.0),
        pipeline_name="p",
        raw_code=_SIMPLE_CODE,
    )
    lines: list[str] = []
    _append_flow_graph_section(lines, g)
    joined = "\n".join(lines)
    assert "Volume [SOURCE" in joined
    assert "Volume [TARGET" in joined
    assert "UNKNOWN (no volume evidence)" in joined  # target has no runtime
    assert str(int(500.0 * _GB)) in joined
