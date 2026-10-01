"""Common pipeline data-flow graph (GAP-001).

Single graph model + builder shared by offline and online validation.
See docs/PIPELINE_FLOW_GRAPH.md.
"""

from dpif.flow.builder import build_pipeline_flow_graph
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
    "VolumeObservation",
    "build_pipeline_flow_graph",
    "correlate_sql_operations",
    "normalize_query_history",
]
