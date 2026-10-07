"""Phase 9 (P9-4): exact volume attribution for flow-graph SOURCE nodes.

Attribution rule (unconditional):

    exact attribution  -> attach the observed/declared/profile volume
    otherwise          -> volume stays None (UNKNOWN)

There is deliberately NO fallback: no pipeline-baseline fan-out, no copied
totals, no remainder fabrication, no cross-nature summation, and no
heuristic matching (proximity, ordering, timing, names, stages). Accepted
join keys are exact identities only:

- table/profile name equality (DATA_PROFILE, via ``DataProfile.source_name``),
- SQL ``query_id`` + KNOWN fingerprint correlation + table equality
  (OBSERVED_RUNTIME, from already-exposed history measurements),
- declared source path equality with an explicit per-source declared volume
  (CONTRACT_DECLARED; pipeline-level declared volume never qualifies).

Evidence natures are never mixed: each attached observation keeps the
provenance of the evidence it came from, and conservation is scoped per
nature (see :func:`attribution_coverage`). ``None`` (UNKNOWN) is excluded
from every numeric consideration.
"""

from __future__ import annotations

from typing import Any

from dpif.flow import FlowProvenance
from dpif.flow.builder import _GB_BYTES
from dpif.flow.models import (
    EvidenceState,
    FlowNode,
    FlowNodeKind,
    PipelineFlowGraph,
    VolumeObservation,
)
from dpif.models import CollectionMethod, DataProfile, Source
from dpif.models.implementation import EvidenceProvenanceKind
from dpif.runtime.models import QueryHistoryEntry


def _profile_provenance(profile: DataProfile) -> FlowProvenance:
    """Provenance for a per-source profile observation (mirrors baseline map)."""
    if profile.collection_method == CollectionMethod.RUNTIME:
        return FlowProvenance(kind=EvidenceProvenanceKind.RUNTIME, state=EvidenceState.KNOWN)
    if profile.collection_method == CollectionMethod.FIXTURE:
        return FlowProvenance(kind=EvidenceProvenanceKind.FIXTURE, state=EvidenceState.DERIVED)
    return FlowProvenance(kind=EvidenceProvenanceKind.METADATA, state=EvidenceState.DERIVED)


def _declared_observation(source: Source) -> VolumeObservation | None:
    """Declared per-source volume, or ``None`` when not explicitly declared.

    Only ``Source.expected_volume_gb`` (the per-source declaration) qualifies.
    Contract-level daily rates and peak capacity are never treated as source
    volumes. Representation mirrors the legacy baseline conversion (GB to
    bytes, CONTRACT nature) without copying pipeline totals.
    """
    expected = source.expected_volume_gb
    if not isinstance(expected, (int, float)) or expected <= 0:
        return None
    return VolumeObservation(
        input_bytes=int(expected * _GB_BYTES),
        provenance=FlowProvenance(
            kind=EvidenceProvenanceKind.CONTRACT, state=EvidenceState.KNOWN
        ),
    )


def _profile_observation(profile: DataProfile) -> VolumeObservation | None:
    """Profile observation for one source, or ``None`` when unmeasured."""
    total_bytes = profile.total_bytes if isinstance(profile.total_bytes, int) else 0
    record_count = profile.record_count if isinstance(profile.record_count, int) else 0
    if total_bytes <= 0 and record_count <= 0:
        return None
    return VolumeObservation(
        input_bytes=total_bytes if total_bytes > 0 else None,
        input_rows=record_count if record_count > 0 else None,
        provenance=_profile_provenance(profile),
    )


def _history_observation(entry: QueryHistoryEntry) -> VolumeObservation | None:
    """History observation, or ``None`` when the entry exposes no measurement.

    Only already-exposed ``read_bytes`` / ``written_bytes`` / ``rows`` fields
    are used. Duration, cluster size, task counts, totals, and estimates are
    never converted into volume.
    """
    read_bytes = entry.read_bytes if isinstance(entry.read_bytes, int) else None
    written_bytes = entry.written_bytes if isinstance(entry.written_bytes, int) else None
    rows = entry.rows if isinstance(entry.rows, int) else None
    if read_bytes is None and written_bytes is None and rows is None:
        return None
    return VolumeObservation(
        input_bytes=read_bytes,
        output_bytes=written_bytes,
        input_rows=rows,
        provenance=FlowProvenance(
            kind=EvidenceProvenanceKind.RUNTIME, state=EvidenceState.KNOWN
        ),
    )


def _source_node_names(node: FlowNode) -> str | None:
    """Exact dataset identity of a SOURCE node, or ``None`` when unknown."""
    if node.kind != FlowNodeKind.SOURCE:
        return None
    dataset = node.dataset
    if dataset is None:
        return None
    name = dataset.name
    if not name or name == "UNKNOWN":
        return None
    return name


def attribute_volumes(
    graph: PipelineFlowGraph,
    *,
    sources: list[Source] | None = None,
    contract_source: Source | None = None,
    data_profiles: list[DataProfile] | None = None,
    query_entries: list[QueryHistoryEntry] | None = None,
) -> dict[str, Any]:
    """Attach exact per-source volumes to SOURCE nodes; UNKNOWN otherwise.

    Precedence per node is first exact match wins across independent natures
    (DATA_PROFILE, then OBSERVED_RUNTIME, then CONTRACT_DECLARED); natures are
    never combined into one observation. Nodes with a pre-existing volume are
    never overwritten. Returns a coverage summary of node counts per nature
    (metadata about the graph, never invented volumes)::

        {"attributed": {"DATA_PROFILE": n, ...}, "unknown": m, "total_sources": t}
    """
    summary: dict[str, Any] = {"attributed": {}, "unknown": 0, "total_sources": 0}

    def _count(kind: str) -> None:
        bucket = summary["attributed"]
        bucket[kind] = bucket.get(kind, 0) + 1

    # Index KNOWN SQL correlations by authoritative query id.
    known_queries: set[str] = set()
    for record in graph.correlations:
        if record.state == EvidenceState.KNOWN and record.runtime_query_id:
            known_queries.add(record.runtime_query_id)
    entries_by_query: dict[str, QueryHistoryEntry] = {}
    for entry in query_entries or []:
        if isinstance(entry, QueryHistoryEntry) and entry.query_id:
            entries_by_query.setdefault(entry.query_id, entry)

    declared: list[Source] = [s for s in (sources or []) if isinstance(s, Source)]
    if isinstance(contract_source, Source):
        declared.append(contract_source)

    for node in graph.nodes:
        if node.kind != FlowNodeKind.SOURCE or node.volume is not None:
            continue
        name = _source_node_names(node)
        summary["total_sources"] += 1
        if name is None:
            summary["unknown"] += 1
            continue
        observation: VolumeObservation | None = None
        nature = ""
        # DATA_PROFILE: exact source_name match with measured bytes/rows.
        for profile in data_profiles or []:
            if (
                isinstance(profile, DataProfile)
                and profile.source_name == name
            ):
                observation = _profile_observation(profile)
                if observation is not None:
                    nature = "DATA_PROFILE"
                    break
        # OBSERVED_RUNTIME: exact KNOWN correlation + measured history entry
        # whose tables name this exact source.
        if observation is None:
            for query_id in known_queries:
                matched = entries_by_query.get(query_id)
                if matched is None:
                    continue
                tables = matched.tables or []
                if name not in tables:
                    continue
                observation = _history_observation(matched)
                if observation is not None:
                    nature = "OBSERVED_RUNTIME"
                    break
        # CONTRACT_DECLARED: exact path/name match with an explicit
        # per-source declared volume (never pipeline-level rates).
        if observation is None:
            for source in declared:
                paths = {source.path, source.name, source.location}
                if name in {p for p in paths if p}:
                    observation = _declared_observation(source)
                    if observation is not None:
                        nature = "CONTRACT_DECLARED"
                        break
        if observation is None:
            summary["unknown"] += 1
        else:
            node.volume = observation
            _count(nature)
    return summary
