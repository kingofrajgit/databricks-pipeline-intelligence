from __future__ import annotations

import logging
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

from dpif.models import (
    AnalysisMethod,
    CheckpointStatus,
    EvidenceRecord,
    Finding,
    Rule,
    Severity,
)

logger = logging.getLogger(__name__)

# Context keys the engine understands (never invented — only read from input).
KNOWN_CONTEXT_KEYS = (
    "data_size_gb",
    "row_count",
    "table_size_gb",
    "cluster_size",
    "cluster_workers",
    "workload_type",
    "runtime_minutes",
    "historical_runs",
    "source_type",
)

# Below this size a driver-side collect is a qualified warning, not a failure.
SMALL_DATA_GB = 1.0


def _resolve_rules_dir() -> Path:
    """Locate the project ``rules/`` directory.

    Resolution order:
    1. ``DPIF_RULES_DIR`` environment variable.
    2. Walk up from this file until a directory containing ``rules/`` is found.
    """
    import os

    env_dir = os.environ.get("DPIF_RULES_DIR")
    if env_dir:
        return Path(env_dir)
    try:
        from dpif.config import get_rules_dir

        candidate = get_rules_dir()
        if candidate.exists():
            return candidate
    except Exception:  # pragma: no cover - config import should not break rules
        logger.debug("get_rules_dir unavailable, falling back to path walk")
    here = Path(__file__).resolve()
    for parent in [here.parent, *here.parents]:
        if (parent / "pyproject.toml").exists():
            return parent / "rules"
    for parent in [here.parent, *here.parents]:
        candidate = parent / "rules"
        # Only accept directories that actually contain rule definitions,
        # so the ``dpif.rules`` package dir never shadows project rules/.
        if candidate.is_dir() and list(candidate.rglob("*.yaml")):
            return candidate
    return Path(__file__).resolve().parents[3] / "rules"


def load_rules(rule_names: list[str] | None = None) -> list[Rule]:
    """Load rules from YAML files in the rules directory."""
    rules_dir = _resolve_rules_dir()
    rules: list[Rule] = []

    if not rules_dir.exists():
        logger.warning("Rules directory not found: %s", rules_dir)
        return rules

    yaml_files = sorted(rules_dir.rglob("*.yaml")) + sorted(rules_dir.rglob("*.yml"))

    for yaml_file in yaml_files:
        try:
            with open(yaml_file, encoding="utf-8") as f:
                rule_data = yaml.safe_load(f)

            if not isinstance(rule_data, dict):
                continue

            severity_raw = str(rule_data.get("severity", "MEDIUM")).upper()
            try:
                severity = Severity(severity_raw)
            except ValueError:
                logger.warning("Unknown severity %r in %s, using MEDIUM", severity_raw, yaml_file)
                severity = Severity.MEDIUM

            rule = Rule(
                rule_id=str(rule_data.get("rule_id", yaml_file.stem)),
                name=str(rule_data.get("name", yaml_file.stem)),
                category=str(rule_data.get("category", "general")),
                description=str(rule_data.get("description", "")),
                severity=severity,
                condition=str(rule_data.get("condition", "")),
                evidence_required=list(rule_data.get("evidence_required", []) or []),
                recommendation=str(rule_data.get("recommendation", "")),
                blocking=bool(rule_data.get("blocking", False)),
                score=float(rule_data.get("score", 0.0)),
                version=str(rule_data.get("version", "1.0.0")),
                context_aware=bool(rule_data.get("context_aware", True)),
                requires_context=list(rule_data.get("requires_context", []) or []),
                evaluator=str(rule_data.get("evaluator", "") or ""),
                params=dict(rule_data.get("params", {}) or {}),
                cap_status=str(rule_data.get("cap_status", "") or ""),
            )
            rules.append(rule)
        except Exception as e:
            logger.error("Failed to load rule from %s: %s", yaml_file, e)

    if rule_names:
        wanted = set(rule_names)
        rules = [r for r in rules if r.rule_id in wanted or r.name in wanted]

    logger.info("Loaded %d rules from %s", len(rules), rules_dir)
    return rules


def _extract_context(data: dict[str, Any], context: dict[str, Any] | None = None) -> dict[str, Any]:
    """Merge context from ``data['context']`` and explicit ``context`` arg."""
    merged: dict[str, Any] = {}
    nested = data.get("context")
    if isinstance(nested, dict):
        merged.update(nested)
    if context:
        merged.update(context)
    # Also accept top-level known keys inside data itself.
    for key in KNOWN_CONTEXT_KEYS:
        if key in data and key not in merged:
            merged[key] = data[key]
    return merged


def _qualify_with_context(
    rule: Rule, ctx: dict[str, Any]
) -> tuple[CheckpointStatus, float, dict[str, Any]]:
    """Decide finding status/confidence given available context.

    Never invents metrics: when a context-aware rule has no relevant
    context, the finding is downgraded to WARN with low confidence and an
    explicit ``context-unavailable`` assumption.
    """
    assumptions: dict[str, Any] = {}
    if not rule.context_aware:
        return CheckpointStatus.FAIL, 0.9, assumptions

    relevant = [k for k in KNOWN_CONTEXT_KEYS if k in ctx]
    required = [k for k in (rule.requires_context or []) if k in ctx]
    if rule.requires_context and not required:
        assumptions["context-unavailable"] = (
            f"Rule {rule.rule_id} requires {rule.requires_context} "
            "but none was provided; treating as qualified warning."
        )
        return CheckpointStatus.WARN, 0.5, assumptions
    if not relevant:
        assumptions["context-unavailable"] = (
            "No data/cluster context provided; finding is syntax-level only."
        )
        # Blocking driver-collect without context stays visible but qualified.
        if rule.blocking:
            return CheckpointStatus.FAIL, 0.55, assumptions
        return CheckpointStatus.WARN, 0.5, assumptions

    size_gb = ctx.get("data_size_gb", ctx.get("table_size_gb"))
    if isinstance(size_gb, (int, float)) and size_gb < SMALL_DATA_GB:
        assumptions["small-data"] = (
            f"Observed data size {size_gb} GB < {SMALL_DATA_GB} GB; "
            "driver-side operation may be acceptable."
        )
        return CheckpointStatus.WARN, 0.6, assumptions
    return CheckpointStatus.FAIL, 0.9, assumptions


def evaluate_rule(
    rule: Rule,
    data: dict[str, Any],
    context: dict[str, Any] | None = None,
) -> Finding | None:
    """Evaluate one rule with optional context. Returns None on no-match.

    Rules carrying an ``evaluator`` name are routed to the registered data
    analyzer instead of executing ``condition`` as a regex.
    """
    if rule.evaluator:
        if rule.rule_id.startswith("CODE-SQL-"):
            from dpif.sql import evaluators as sql_evaluators

            fn = sql_evaluators.resolve(rule.evaluator)
        elif rule.rule_id.startswith("CONFIG-"):
            from dpif.discovery import evaluators as config_evaluators

            fn = config_evaluators.resolve(rule.evaluator)
        elif rule.rule_id.startswith("RUNTIME-"):
            from dpif.runtime import evaluators as runtime_evaluators

            fn = runtime_evaluators.resolve(rule.evaluator)
        else:
            from dpif.analyzers.data import evaluators as data_evaluators

            fn = data_evaluators.resolve(rule.evaluator)
            if fn is None:
                from dpif.sql import evaluators as sql_evaluators

                fn = sql_evaluators.resolve(rule.evaluator)
            if fn is None:
                from dpif.discovery import evaluators as config_evaluators

                fn = config_evaluators.resolve(rule.evaluator)
            if fn is None:
                from dpif.runtime import evaluators as runtime_evaluators

                fn = runtime_evaluators.resolve(rule.evaluator)
        if fn is None:
            logger.warning("Unknown evaluator %r for rule %s", rule.evaluator, rule.rule_id)
            return None
        return fn(rule, data, context or {})
    text = data.get("text", "")
    if not isinstance(rule.condition, str) or not rule.condition:
        return None
    try:
        match = re.search(rule.condition, text or "", re.IGNORECASE)
    except re.error as e:
        logger.error("Invalid regex in rule %s: %s", rule.rule_id, e)
        return None
    if not match:
        return None

    ctx = _extract_context(data, context)
    status, confidence, ctx_assumptions = _qualify_with_context(rule, ctx)

    evidence_locations = data.get("evidence", [])
    if not evidence_locations:
        # Record the matched snippet as evidence (truncated, never secrets).
        snippet = (text or "")[max(0, match.start() - 40) : match.end() + 40]
        evidence_locations = [f"matched: ...{snippet.strip()[:160]}..."]

    observed = dict(data.get("observed", {}) or {})
    observed.setdefault("matched_pattern", rule.condition)
    expected = dict(data.get("expected", {}) or {})
    assumptions = dict(data.get("assumptions", {}) or {})
    assumptions.update(ctx_assumptions)
    if ctx:
        assumptions.setdefault("context", {k: v for k, v in ctx.items() if k in KNOWN_CONTEXT_KEYS})

    rec = rule.recommendation or f"Avoid: {rule.description}"
    now = datetime.now(UTC)
    evidence = EvidenceRecord(
        rule_id=rule.rule_id,
        status=status,
        severity=rule.severity,
        observed=observed,
        expected=expected,
        evidence=list(evidence_locations),
        recommendation=rec,
        confidence=confidence,
        method=AnalysisMethod.UNAVAILABLE,
    )
    return Finding(
        finding_id=f"{rule.rule_id}-{rule.category}",
        rule_id=rule.rule_id,
        name=rule.name,
        title=rule.name,
        description=rule.description,
        category=rule.category,
        status=status,
        severity=rule.severity,
        pipeline_name=str(data.get("pipeline_name", "unknown")),
        timestamp=now,
        evidence=evidence,
        recommendation=rec,
        confidence=confidence,
        blocking=rule.blocking,
        assumptions=assumptions,
    )


def evaluate_rules(
    rules: list[Rule],
    code_text: str,
    pipeline_name: str = "unknown",
    assumptions: dict[str, Any] | None = None,
    context: dict[str, Any] | None = None,
) -> list[Finding]:
    """Evaluate a list of rules against code text and return findings."""
    findings: list[Finding] = []
    assumptions = dict(assumptions or {})

    for rule in rules:
        try:
            data = {
                "text": code_text,
                "pipeline_name": pipeline_name,
                "assumptions": dict(assumptions),
            }
            finding = evaluate_rule(rule, data, context)
            if finding is not None:
                findings.append(finding)
        except Exception as e:
            logger.error("Rule evaluation failed for %s: %s", rule.rule_id, e)
            now = datetime.now(UTC)
            findings.append(
                Finding(
                    finding_id=f"{rule.rule_id}-eval-error",
                    rule_id=rule.rule_id,
                    name=f"Rule evaluation error: {rule.name}",
                    title=f"Rule evaluation error: {rule.name}",
                    description=str(e),
                    category=rule.category,
                    status=CheckpointStatus.FAIL,
                    severity=Severity.HIGH,
                    pipeline_name=pipeline_name,
                    timestamp=now,
                    evidence=EvidenceRecord(
                        rule_id=rule.rule_id,
                        status=CheckpointStatus.FAIL,
                        severity=Severity.HIGH,
                        observed={"error": str(e)},
                        expected={"success": True},
                        evidence=[f"Rule evaluation failed: {e}"],
                        recommendation=f"Fix rule evaluation for {rule.rule_id}",
                        confidence=1.0,
                    ),
                    recommendation=f"Fix rule evaluation for {rule.rule_id}",
                    confidence=1.0,
                    blocking=False,
                    assumptions=dict(assumptions),
                )
            )
    return findings


def evaluate_rule_condition(
    condition: str,
    code_text: str,
    case_insensitive: bool = True,
) -> bool:
    """Evaluate a simple regex/pattern condition against code text."""
    flags = re.IGNORECASE if case_insensitive else 0
    return bool(re.search(condition, code_text, flags))


_rule_registry: dict[str, Rule] = {}


def register_rule(rule: Rule) -> None:
    """Register a rule in the global registry."""
    _rule_registry[rule.rule_id] = rule
    logger.debug("Registered rule: %s", rule.rule_id)


def get_rule(rule_id: str) -> Rule | None:
    """Get a rule by its ID."""
    return _rule_registry.get(rule_id)


def evaluate_all_rules(
    code_text: str,
    pipeline_name: str = "unknown",
    rule_ids: list[str] | None = None,
    assumptions: dict[str, Any] | None = None,
    context: dict[str, Any] | None = None,
) -> list[Finding]:
    """Evaluate all registered rules (or filtered rule IDs) against code text."""
    if rule_ids:
        rules = [r for r in (get_rule(rid) for rid in rule_ids) if r is not None]
    else:
        rules = list(_rule_registry.values())
    return evaluate_rules(rules, code_text, pipeline_name, assumptions, context)
