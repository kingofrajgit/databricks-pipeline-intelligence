# ADR-001: Framework Architecture Decision

## Status: Accepted

## Context
The Databricks Pipeline Intelligence Framework needs a modular, extensible architecture that supports incremental development across 15+ phases. The framework must be evidence-driven, distinguish UNKNOWN from PASS/FAIL, and support offline validation without Databricks credentials.

## Decision
Adopt a plug-in analyzer architecture with the following principles:

1. **Three-layer validation model**: DESIGN vs IMPLEMENTED vs ACTUAL comparison
2. **Evidence-driven**: Every finding has structured evidence
3. **Unknown is important**: PASS/FAIL/WARNING/UNKNOWN are distinct; UNKNOWN never converts to PASS
4. **Context-aware rules**: Rules evaluate code + data + configuration together
5. **Modular checkpoints**: Checkpoints can depend on previous checkpoints; failures propagate as UNKNOWN
6. **Offline-first**: Dry-run mode works without Databricks credentials
7. **Configurable everything**: Thresholds, rules, weights, and analysis methods are all configurable
8. **No fabricated runtime**: If metrics unavailable, report "Insufficient historical execution data"

## Alternatives Considered
- **Simplistic checklist approach**: Rejected - too rigid, no context, converts UNKNOWN to PASS
- **AI-first approach**: Rejected - AI is secondary layer, cannot replace deterministic validation
- **Monolithic architecture**: Rejected - difficult to incrementally develop across 15+ phases

## Consequences
- New analyzers are pluggable without rewriting core
- Rules externalized to YAML for independent testing
- Scoring is configurable per organization profile
- CLI supports both offline (fixture-based) and live modes
- Framework can be extended without breaking existing functionality

## Related ADRs
- ADR-002: Rule Engine Format
- ADR-003: Checkpoint Dependency Model
- ADR-004: Scoring Configuration