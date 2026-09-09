# ADR-003: Pydantic v2 Model Fix (Phase 2)

## Status: Accepted (Phase 2)

## Context
Phase 1 model classes used Pydantic `Field()` annotations but did **not**
inherit from `pydantic.BaseModel`:

```python
class Source:          # <- plain class
    source_id: str
    type: SourceType
    ...
```

Consequences reproduced in Phase 2:
- `Source()` created an object with **no attributes set** (`AttributeError`
  on access).
- `Source(source_id="x", type="adls")` raised
  `TypeError: Source() takes no arguments` (no generated `__init__`).

## Decision
Make every model a proper `BaseModel` subclass with Pydantic v2 syntax:

- `class Source(BaseModel): ...` (same for Target, DataProfile, Checkpoint,
  Rule, Finding, EvidenceRecord, PipelineContract, SLARules, ScheduleRules,
  ReliabilityRules, ScalabilityRules, Score, ValidationRun).
- Keep `from __future__ import annotations` (supported natively by Pydantic v2).
- `Checkpoint.evidence` default is a factory lambda producing a minimal
  UNKNOWN `EvidenceRecord` (a bare `default_factory=EvidenceRecord` crashes
  because `rule_id`/`status`/`severity` are required).
- Rename shadowing fields: `DataProfile.schema` → `schema_text` (alias
  `"schema"` retained, `populate_by_name=True`), same for `Target.schema`,
  because `schema` shadows `BaseModel.schema` and spams warnings.
- `Settings` no longer uses Pydantic-v1 `Field(env=...)`; environment loading
  is explicit (`DPIF_*` vars with coercion in `_settings_from_env`), since
  `pydantic-settings` is not a dependency.
- `PipelineContract.validate` renamed to `validate_contract` to avoid
  clashing with `BaseModel.validate`.
- `Finding` extended with `title`, `description`, `recommendation`,
  `confidence`, `blocking` (§11 finding model).

## Consequences
- Normal keyword construction works and is locked in by
  `tests/unit/test_models.py` (16 tests covering all core models).
- Enums migrated to `StrEnum` (UP042); behavior unchanged.
- No workaround (`Source()` + attribute assignment) remains in the codebase.
