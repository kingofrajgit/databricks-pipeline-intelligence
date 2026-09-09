"""Static Risk to Runtime Evidence Correlation Engine (Phase 7).

Correlates static code and SQL findings against actual runtime execution observations
without modifying or deleting the underlying static findings.
"""

from __future__ import annotations

from dpif.models import CheckpointStatus, Finding
from dpif.runtime.models import CorrelationResult

# Mapping of static rule IDs to candidate runtime rule IDs that validate/corroborate them
_CORRELATION_MAP: dict[str, list[str]] = {
    # Shuffle & Join Risks
    "CODE-PYSPARK-008": ["RUNTIME-PERF-002", "RUNTIME-PERF-003"],
    "CODE-SQL-003": ["RUNTIME-PERF-002", "RUNTIME-PERF-003"],
    # Cross Join Risks
    "CODE-PYSPARK-002": ["RUNTIME-PERF-001", "RUNTIME-PERF-003"],
    "CODE-SQL-002": ["RUNTIME-PERF-001", "RUNTIME-PERF-003"],
    # Global Sort Risks
    "CODE-PYSPARK-007": ["RUNTIME-PERF-001", "RUNTIME-PERF-002", "RUNTIME-PERF-004"],
    "CODE-SQL-005": ["RUNTIME-PERF-001", "RUNTIME-PERF-002", "RUNTIME-PERF-004"],
    # Collect / Driver Bottlenecks
    "CODE-PYSPARK-001": ["RUNTIME-PERF-008", "RUNTIME-PERF-010"],
    "CODE-PYSPARK-006": ["RUNTIME-PERF-008", "RUNTIME-PERF-010"],
    # Window Functions
    "CODE-PYSPARK-012": ["RUNTIME-PERF-004", "RUNTIME-PERF-005"],
    "CODE-SQL-007": ["RUNTIME-PERF-004", "RUNTIME-PERF-005"],
}


def correlate_static_and_runtime(
    static_findings: list[Finding],
    runtime_findings: list[Finding],
    has_runtime_evidence: bool,
) -> list[CorrelationResult]:
    """Correlate static code/SQL findings against observed runtime performance findings.

    Returns a list of structured CorrelationResult objects tracking the explicit relationship.
    """
    results: list[CorrelationResult] = []

    # Index triggered runtime findings by rule_id
    triggered_runtime = {
        f.rule_id: f
        for f in runtime_findings
        if f.status in (CheckpointStatus.WARN, CheckpointStatus.FAIL)
    }

    for sf in static_findings:
        rule_id = sf.rule_id
        target_runtime_rules = _CORRELATION_MAP.get(rule_id)
        if not target_runtime_rules:
            continue

        if not has_runtime_evidence:
            results.append(
                CorrelationResult(
                    static_rule_id=rule_id,
                    runtime_rule_ids=target_runtime_rules,
                    status="RUNTIME_EVIDENCE_UNAVAILABLE",
                    description=(
                        f"Static risk '{rule_id}' detected, but no runtime execution evidence "
                        "was supplied to confirm or refute actual execution impact."
                    ),
                    confidence=0.5,
                    evidence=[sf.title],
                )
            )
            continue

        matching_runtime = [r_id for r_id in target_runtime_rules if r_id in triggered_runtime]

        if len(matching_runtime) >= 2 or (
            rule_id in ("CODE-PYSPARK-007", "CODE-SQL-005") and matching_runtime
        ):
            results.append(
                CorrelationResult(
                    static_rule_id=rule_id,
                    runtime_rule_ids=matching_runtime,
                    status="RUNTIME_SUPPORTS_STATIC_RISK",
                    description=(
                        f"Static risk '{rule_id}' is corroborated by multiple observed runtime "
                        f"performance indicators ({matching_runtime})."
                    ),
                    confidence=0.95,
                    evidence=[
                        f"Static: {sf.title}",
                        *(f"Runtime: {triggered_runtime[r].title}" for r in matching_runtime),
                    ],
                )
            )
        elif len(matching_runtime) == 1:
            results.append(
                CorrelationResult(
                    static_rule_id=rule_id,
                    runtime_rule_ids=matching_runtime,
                    status="STATIC_RISK_CONFIRMED",
                    description=(
                        f"Static risk '{rule_id}' was confirmed by runtime execution anomaly "
                        f"'{matching_runtime[0]}'."
                    ),
                    confidence=0.9,
                    evidence=[
                        f"Static: {sf.title}",
                        f"Runtime: {triggered_runtime[matching_runtime[0]].title}",
                    ],
                )
            )
        else:
            results.append(
                CorrelationResult(
                    static_rule_id=rule_id,
                    runtime_rule_ids=target_runtime_rules,
                    status="STATIC_RISK_NOT_OBSERVED_IN_SUPPLIED_RUN",
                    description=(
                        f"Static risk '{rule_id}' was present in code, but was not observed "
                        "during the analyzed execution run."
                    ),
                    confidence=0.75,
                    evidence=[
                        f"Static: {sf.title}",
                        "Supplied runtime run showed no matching performance anomalies",
                    ],
                )
            )

    return results
