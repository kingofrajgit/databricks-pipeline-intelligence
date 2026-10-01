"""Common pipeline data-flow graph (GAP-001).

Single graph model + builder shared by offline and online validation.
See docs/PIPELINE_FLOW_GRAPH.md.
"""

from dpif.flow.builder import build_pipeline_flow_graph
from dpif.flow.completeness import (
    CompletenessSignal,
    CompletenessSignalKind,
    StructuralCompleteness,
    TargetReachability,
    analyze_structural_completeness,
    target_reachability,
)
from dpif.flow.correlation import correlate_sql_operations, normalize_query_history
from dpif.flow.models import (
    CorrelationMethod,
    DatasetIdentity,
    EvidenceState,
    FlowEdge,
    FlowGraphIssue,
    FlowNode,
    FlowNodeKind,
    FlowProvenance,
    OperationRuntimeCorrelation,
    PipelineFlowGraph,
    SourceLocation,
    VolumeObservation,
)

__all__ = [
    "CompletenessSignal",
    "CompletenessSignalKind",
    "CorrelationMethod",
    "DatasetIdentity",
    "EvidenceState",
    "FlowEdge",
    "FlowGraphIssue",
    "FlowNode",
    "FlowNodeKind",
    "FlowProvenance",
    "OperationRuntimeCorrelation",
    "PipelineFlowGraph",
    "SourceLocation",
    "StructuralCompleteness",
    "TargetReachability",
    "VolumeObservation",
    "analyze_structural_completeness",
    "build_pipeline_flow_graph",
    "correlate_sql_operations",
    "normalize_query_history",
    "target_reachability",
]
