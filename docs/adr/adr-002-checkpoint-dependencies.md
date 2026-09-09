# ADR-002: Checkpoint Dependency Behavior (FAIL/UNKNOWN Propagation)

## Status: Accepted (Phase 2)

## Context
Checkpoints form a DAG (e.g. CP-007 Data depends on CP-001 Source; CP-024
Readiness depends on CP-001/004/007/009/011). A dependent checkpoint must not
claim PASS when its inputs are untrustworthy.

## Decision
- Any dependency with status FAIL → dependent resolves to UNKNOWN.
- Any dependency with status UNKNOWN (or missing) → dependent resolves to
  UNKNOWN.
- WARN dependencies are tolerated (execution proceeds; severity may escalate).
- The blocking reason is recorded under both `assumptions["unknown-reason"]`
  and `assumptions["dependency"]` so reports can explain *why* something is
  UNKNOWN.
- Execution order is topological over executed (not passed) dependencies, so
  FAIL/UNKNOWN parents never deadlock children — children run and resolve to
  UNKNOWN with the reason attached.

## Consequences
- UNKNOWN is contagious by design; reports list every UNKNOWN checkpoint with
  its reason instead of silently passing.
- `CheckpointEngine.run_all_checkpoints` registers all checkpoints up front so
  dependency lookups never miss; cycles resolve to UNKNOWN, never hang.
- Covered by `tests/unit/test_checkpoint_engine.py` (fail→unknown,
  unknown→unknown, missing→unknown, pass→executes, ordering).
