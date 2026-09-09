"""Strongly-typed domain models for Phase 8 Scalability Intelligence."""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

from dpif.models import Finding


class EvidenceProvenance(StrEnum):
    """Explicit evidence classes.

    Crucial rule: PROJECTED is NOT RUNTIME. FIXTURE is NOT RUNTIME.
    STATIC is NOT RUNTIME. CONTRACT is NOT RUNTIME.
    """

    CONTRACT = "CONTRACT"
    STATIC = "STATIC"
    DATABRICKS_API = "DATABRICKS_API"
    DATABRICKS_METADATA = "DATABRICKS_METADATA"
    RUNTIME = "RUNTIME"
    EVENT_LOG = "EVENT_LOG"
    FIXTURE = "FIXTURE"
    PROJECTED = "PROJECTED"
    UNKNOWN = "UNKNOWN"


class ScenarioType(StrEnum):
    """Workload scenario categories."""

    BASELINE = "BASELINE"
    EXPECTED = "EXPECTED"
    PEAK = "PEAK"
    GROWTH_1 = "GROWTH_1"
    GROWTH_2 = "GROWTH_2"
    CUSTOM = "CUSTOM"


class ProjectionMethod(StrEnum):
    """Methods used to project future workload metrics."""

    LINEAR = "LINEAR"
    HISTORICAL_TREND = "HISTORICAL_TREND"
    HEURISTIC = "HEURISTIC"
    UNAVAILABLE = "UNAVAILABLE"


class ScalingBehavior(StrEnum):
    """Observed empirical scaling behavior across multiple runs."""

    SUB_LINEAR = "SUB_LINEAR"
    LINEAR = "LINEAR"
    SUPER_LINEAR = "SUPER_LINEAR"
    EXPONENTIAL = "EXPONENTIAL"
    INSUFFICIENT_DATA = "INSUFFICIENT_DATA"


class ScalabilityScenario(BaseModel):
    """Specific workload scenario defining target data and operational scale."""

    model_config = ConfigDict(populate_by_name=True)

    scenario_id: str
    name: str
    scenario_type: ScenarioType
    input_volume_gb: float
    file_count: int | None = None
    expected_runtime_minutes: float | None = None
    sla_minutes: float | None = None
    growth_factor: float = 1.0
    source: str = ""
    evidence_source: EvidenceProvenance = EvidenceProvenance.CONTRACT

    @property
    def input_volume_tb(self) -> float:
        return self.input_volume_gb / 1024.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "scenario_id": self.scenario_id,
            "name": self.name,
            "scenario_type": self.scenario_type.value,
            "input_volume_gb": self.input_volume_gb,
            "file_count": self.file_count,
            "expected_runtime_minutes": self.expected_runtime_minutes,
            "sla_minutes": self.sla_minutes,
            "growth_factor": self.growth_factor,
            "source": self.source,
            "evidence_source": self.evidence_source.value,
        }


class ScalabilityProjection(BaseModel):
    """Deterministic projection of a metric from baseline to target volume.

    Every projection explicitly records its method, assumptions, and confidence.
    """

    model_config = ConfigDict(populate_by_name=True)

    baseline_volume_gb: float
    target_volume_gb: float
    scaling_factor: float
    projection_method: ProjectionMethod
    projected_metric: str
    baseline_value: float | None = None
    projected_value: float
    confidence: float = Field(default=0.5, ge=0.0, le=1.0)
    assumptions: list[str] = Field(default_factory=list)
    evidence_source: EvidenceProvenance = EvidenceProvenance.PROJECTED

    def to_dict(self) -> dict[str, Any]:
        return {
            "baseline_volume_gb": self.baseline_volume_gb,
            "target_volume_gb": self.target_volume_gb,
            "scaling_factor": self.scaling_factor,
            "projection_method": self.projection_method.value,
            "projected_metric": self.projected_metric,
            "baseline_value": self.baseline_value,
            "projected_value": self.projected_value,
            "confidence": self.confidence,
            "assumptions": self.assumptions,
            "evidence_source": self.evidence_source.value,
        }


class ScalabilityObservation(BaseModel):
    """Single empirical observation point from actual run execution telemetry."""

    model_config = ConfigDict(populate_by_name=True)

    run_id: str | None = None
    volume_gb: float
    duration_minutes: float
    shuffle_bytes: int = 0
    spill_bytes: int = 0
    task_count: int = 0
    failed_tasks: int = 0
    worker_count: int | None = None
    evidence_source: EvidenceProvenance = EvidenceProvenance.RUNTIME

    @model_validator(mode="before")
    @classmethod
    def _normalize_fields(cls, data: Any) -> Any:
        if isinstance(data, dict):
            d = dict(data)
            if "volume_gb" not in d:
                for k in ("input_volume_gb", "data_volume_gb", "total_gb"):
                    if k in d:
                        d["volume_gb"] = float(d[k])
                        break
            if "duration_minutes" not in d:
                if "duration_seconds" in d:
                    d["duration_minutes"] = float(d["duration_seconds"]) / 60.0
                elif "total_duration_minutes" in d:
                    d["duration_minutes"] = float(d["total_duration_minutes"])
            return d
        return data

    @property
    def failure_rate(self) -> float:
        if self.task_count <= 0:
            return 0.0
        return self.failed_tasks / float(self.task_count)

    @property
    def shuffle_gb(self) -> float:
        return self.shuffle_bytes / (1024.0**3)

    @property
    def spill_gb(self) -> float:
        return self.spill_bytes / (1024.0**3)

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "volume_gb": self.volume_gb,
            "duration_minutes": self.duration_minutes,
            "shuffle_bytes": self.shuffle_bytes,
            "spill_bytes": self.spill_bytes,
            "task_count": self.task_count,
            "failed_tasks": self.failed_tasks,
            "worker_count": self.worker_count,
            "evidence_source": self.evidence_source.value,
        }


class ScalabilityTrend(BaseModel):
    """Empirical scaling relationship derived from 2+ observations."""

    model_config = ConfigDict(populate_by_name=True)

    metric_name: str
    observations_count: int
    scaling_behavior: ScalingBehavior
    scaling_factor: float | None = None
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    details: dict[str, Any] = Field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "metric_name": self.metric_name,
            "observations_count": self.observations_count,
            "scaling_behavior": self.scaling_behavior.value,
            "scaling_factor": self.scaling_factor,
            "confidence": self.confidence,
            "details": self.details,
        }


class ScalabilityAssessment(BaseModel):
    """Complete scalability intelligence summary across scenarios and findings."""

    model_config = ConfigDict(populate_by_name=True)

    baseline_scenario: ScalabilityScenario | None = None
    expected_scenario: ScalabilityScenario | None = None
    peak_scenario: ScalabilityScenario | None = None
    growth_scenarios: list[ScalabilityScenario] = Field(default_factory=list)
    observations: list[ScalabilityObservation] = Field(default_factory=list)
    projections: list[ScalabilityProjection] = Field(default_factory=list)
    trends: list[ScalabilityTrend] = Field(default_factory=list)
    findings: list[Finding] = Field(default_factory=list)

    @property
    def all_scenarios(self) -> list[ScalabilityScenario]:
        res: list[ScalabilityScenario] = []
        if self.baseline_scenario:
            res.append(self.baseline_scenario)
        if self.expected_scenario:
            res.append(self.expected_scenario)
        if self.peak_scenario:
            res.append(self.peak_scenario)
        res.extend(self.growth_scenarios)
        return res

    def to_dict(self) -> dict[str, Any]:
        return {
            "baseline_scenario": self.baseline_scenario.to_dict()
            if self.baseline_scenario
            else None,
            "expected_scenario": self.expected_scenario.to_dict()
            if self.expected_scenario
            else None,
            "peak_scenario": self.peak_scenario.to_dict() if self.peak_scenario else None,
            "growth_scenarios": [s.to_dict() for s in self.growth_scenarios],
            "observations": [o.to_dict() for o in self.observations],
            "projections": [p.to_dict() for p in self.projections],
            "trends": [t.to_dict() for t in self.trends],
            "findings_count": len(self.findings),
        }
