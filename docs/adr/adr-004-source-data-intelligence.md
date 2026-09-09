# ADR-004: Source & Data Intelligence (Phase 3)

## Status: Accepted (Phase 3)

## Context
Phase 2 validated pipeline shape (contracts, code patterns, config presence)
but did not *understand* data: file distributions, partitions, schemas,
volumes, growth, or source-specific concerns (JDBC sharding, streaming
checkpoints, format fit). Recommendations must depend on workload
characteristics — never "large = bad" or "CSV = bad".

## Decision

### Source intelligence (`Source` model + `src/dpif/sources/`)
- Source carries `format` (`SourceFormat`), `ingestion_mode`, `location`,
  `expected/peak_volume_gb` and `growth_rate_percent` as **`None` when
  unconfigured** — a missing configuration is UNKNOWN, never a valid zero.
- `JdbcMetadata` (query, incremental/partition columns, bounds, partitions,
  fetch size) and `StreamingMetadata` (partitions, checkpoint, trigger,
  watermark, starting position); runtime-only fields stay `None` offline.
- `src/dpif/sources/base.py` holds capabilities (`supports_incremental`,
  `required_config_keys`) and contextual `format_guidance` — no live I/O.

### Data profile (`DataProfile` + `CollectionMethod`)
- New `collection_method`: METADATA | SAMPLE | FULL_SCAN | RUNTIME | FIXTURE
  | UNKNOWN. Fixture loaders stamp FIXTURE + `evidence_source="fixture
  metadata"`; a 500 GB fixture is therefore never representable as a
  Databricks runtime scan.
- Added `compression`, `partition_sizes_gb`, `partition_record_counts`,
  `schema_columns`; every data finding records observed values, the
  configured threshold, severity, recommendation, confidence, and evidence
  source.

### Analyzers (`src/dpif/analyzers/data/`, pure functions over metadata)
- `small_files` (format-adjusted thresholds, avg+median+P95+count),
  `distribution` (median/average skew shape), `partitions` (max/min ratio and
  hot-partition share; wording is "Potential partition imbalance" — runtime
  skew is never claimed), `schema` (expected-vs-observed with configurable
  policy), `volume` (missing-volume detection), `growth` (projects only with
  a declared rate, else UNKNOWN), `formats`/`jdbc`/`streaming` (contextual),
  `coverage` (`EvidenceCoverage`: total/evaluated/unknown/percentage).

### Rules
- YAML rules may declare an `evaluator:` naming a registered analyzer
  (`DATA-001→small_files`, `DATA-002→partition_imbalance`,
  `DATA-003→schema_drift`, `DATA-004→missing_volume`,
  `DATA-005→excessive_count`, `SOURCE-001→source_format`,
  `SOURCE-002→incremental_strategy`, `SOURCE-003→jdbc_parallel`).
- The checkpoint engine routes evaluator rules to analyzers (thresholds from
  `Settings`, overridable per call); regex `condition`s remain for syntax
  rules. CP-001/CP-007 builders are thin skeletons whose UNKNOWN-on-missing
  status the engine preserves; findings set the final verdict.

### Evidence coverage vs score
- `EvidenceCoverage` is reported alongside the score (`Evidence Coverage:
  81% (13/16 checks evidenced)`) and never modifies it; UNKNOWN is reported,
  not punished, unless a scoring profile explicitly says so.

### Fixture vs runtime evidence
- All offline fixtures are metadata JSON/YAML under `tests/fixtures/`; the
  CLI labels them `fixture metadata` with collection method FIXTURE and
  prints "Runtime prediction unavailable" instead of inventing runtimes.

## Consequences
- 8 rules × 5 tests each + 33 analyzer tests + 9 Phase-3 integration tests;
  all 71 Phase-2 tests still pass (153 total).
- Thresholds are configurable (`DPIF_SMALL_FILE_THRESHOLD_KB`,
  `DPIF_EXCESSIVE_FILE_COUNT`, `DPIF_PARTITION_IMBALANCE_RATIO`, …) including
  per-format multipliers.
- Known heuristic limits: restartability keyword scan can match a comment;
  CSV/JSON-at-scale guidance assumes analytical access (stated in findings).
