"""CSV manifest loading and validation for multi-pipeline validation."""

from __future__ import annotations

import csv
from pathlib import Path

from dpif.orchestration.models import (
    PipelineProcessingStatus,
    PipelineSubmission,
    PipelineValidationResult,
)

REQUIRED_HEADERS = {"pipeline_id", "developer", "contract_path", "code_path"}
OPTIONAL_HEADERS = {"metadata_profile", "runtime_run", "historical_runs"}
ALL_HEADERS = REQUIRED_HEADERS | OPTIONAL_HEADERS


def _repo_root(start: Path) -> Path:
    for parent in [start, *start.parents]:
        if (parent / "pyproject.toml").exists():
            return parent
    return start


def resolve_safe_path(
    raw_path: str | None,
    base_dir: Path,
    root_dir: Path,
) -> tuple[Path | None, str | None]:
    """Safely resolve relative or absolute paths guarding against path traversal attacks.

    Returns:
        (resolved_path, error_message). If invalid or non-existent, resolved_path is None.
    """
    if not raw_path:
        return None, None

    clean_raw = raw_path.strip()
    if not clean_raw:
        return None, None

    normalized_slash = clean_raw.replace("\\", "/")

    # Reject Windows drive-letter paths cross-platform (e.g. C:\..., C:/..., C:foo, Z:/...)
    if len(clean_raw) >= 2 and clean_raw[0].isalpha() and clean_raw[1] == ":":
        return None, f"Windows drive letter path not allowed: '{clean_raw}'"

    # Reject UNC paths (e.g. //server/share or \\server\share)
    if clean_raw.startswith(("\\\\", "//")):
        return None, f"UNC path not allowed: '{clean_raw}'"

    # Path traversal detection: check for '..' components in path
    parts = [p for p in normalized_slash.split("/") if p]
    if ".." in parts:
        return None, f"Path traversal ('..') not allowed: '{clean_raw}'"

    path_obj = Path(clean_raw)
    effective_root = root_dir.resolve()

    if path_obj.is_absolute():
        resolved = path_obj.resolve()
    else:
        # Resolve relative to base_dir (directory containing manifest)
        resolved = (base_dir / path_obj).resolve()

    # Security check: ensure resolved path (and symlink target) does not escape effective_root
    try:
        resolved.relative_to(effective_root)
    except ValueError:
        msg = f"Path traversal security violation: '{clean_raw}' escapes root '{effective_root}'"
        return None, msg

    if not resolved.exists():
        return None, f"File not found: '{clean_raw}'"

    if not resolved.is_file():
        return None, f"Path is not a regular file: '{clean_raw}'"

    return resolved, None


def load_and_validate_manifest(
    manifest_path: str | Path,
    allowed_root: Path | None = None,
) -> tuple[list[PipelineSubmission], list[PipelineValidationResult]]:
    """Parse and validate a multi-pipeline CSV manifest strictly.

    Returns:
        tuple of (valid_submissions, invalid_results)
    """
    path = Path(manifest_path)
    base_dir = path.parent.resolve()
    # Explicit allowed_root or fall back deterministically to base_dir (directory containing manifest)
    root_dir = allowed_root.resolve() if allowed_root is not None else base_dir

    valid_submissions: list[PipelineSubmission] = []
    invalid_results: list[PipelineValidationResult] = []

    if not path.exists():
        err_sub = PipelineSubmission(
            pipeline_id="MANIFEST_ERROR",
            developer="UNKNOWN",
            contract_path=str(manifest_path),
            code_path="",
        )
        invalid_results.append(
            PipelineValidationResult(
                submission=err_sub,
                processing_status=PipelineProcessingStatus.MISSING_INPUT,
                errors=[f"Manifest CSV file not found: {manifest_path}"],
            )
        )
        return valid_submissions, invalid_results

    try:
        content = path.read_text(encoding="utf-8")
    except Exception as e:
        err_sub = PipelineSubmission(
            pipeline_id="MANIFEST_ERROR",
            developer="UNKNOWN",
            contract_path=str(manifest_path),
            code_path="",
        )
        invalid_results.append(
            PipelineValidationResult(
                submission=err_sub,
                processing_status=PipelineProcessingStatus.INVALID_SUBMISSION,
                errors=[f"Failed to read manifest CSV: {e}"],
            )
        )
        return valid_submissions, invalid_results

    if not content.strip():
        err_sub = PipelineSubmission(
            pipeline_id="MANIFEST_ERROR",
            developer="UNKNOWN",
            contract_path=str(manifest_path),
            code_path="",
        )
        invalid_results.append(
            PipelineValidationResult(
                submission=err_sub,
                processing_status=PipelineProcessingStatus.INVALID_SUBMISSION,
                errors=["Manifest CSV is empty"],
            )
        )
        return valid_submissions, invalid_results

    # Use csv.reader in strict mode over original content lines (preserving quotes)
    content_lines = content.splitlines()
    strict_reader = csv.reader(content_lines, strict=True)

    try:
        raw_headers = next(strict_reader, None)
    except csv.Error as e:
        err_sub = PipelineSubmission(
            pipeline_id="MANIFEST_ERROR",
            developer="UNKNOWN",
            contract_path=str(manifest_path),
            code_path="",
        )
        invalid_results.append(
            PipelineValidationResult(
                submission=err_sub,
                processing_status=PipelineProcessingStatus.INVALID_SUBMISSION,
                errors=[f"CSV header syntax error: {e}"],
            )
        )
        return valid_submissions, invalid_results

    if raw_headers is None:
        err_sub = PipelineSubmission(
            pipeline_id="MANIFEST_ERROR",
            developer="UNKNOWN",
            contract_path=str(manifest_path),
            code_path="",
        )
        invalid_results.append(
            PipelineValidationResult(
                submission=err_sub,
                processing_status=PipelineProcessingStatus.INVALID_SUBMISSION,
                errors=["Manifest CSV is empty"],
            )
        )
        return valid_submissions, invalid_results

    header_list = [h.strip() for h in raw_headers]
    header_count = len(header_list)
    present_headers = set(header_list)

    missing_headers = REQUIRED_HEADERS - present_headers
    if missing_headers:
        err_sub = PipelineSubmission(
            pipeline_id="MANIFEST_ERROR",
            developer="UNKNOWN",
            contract_path=str(manifest_path),
            code_path="",
        )
        invalid_results.append(
            PipelineValidationResult(
                submission=err_sub,
                processing_status=PipelineProcessingStatus.INVALID_SUBMISSION,
                errors=[
                    f"Missing required CSV header(s): {', '.join(sorted(missing_headers))}"
                ],
            )
        )
        return valid_submissions, invalid_results

    unknown_headers = present_headers - ALL_HEADERS
    if unknown_headers:
        err_sub = PipelineSubmission(
            pipeline_id="MANIFEST_ERROR",
            developer="UNKNOWN",
            contract_path=str(manifest_path),
            code_path="",
        )
        invalid_results.append(
            PipelineValidationResult(
                submission=err_sub,
                processing_status=PipelineProcessingStatus.INVALID_SUBMISSION,
                errors=[
                    f"Unknown CSV header(s): {', '.join(sorted(unknown_headers))}"
                ],
            )
        )
        return valid_submissions, invalid_results

    seen_pipeline_ids: set[str] = set()

    for idx, line_text in enumerate(content_lines[1:], start=2):
        if not line_text.strip():
            continue  # Skip blank lines safely

        try:
            row_fields = next(csv.reader([line_text], strict=True))
        except csv.Error as e:
            err_sub = PipelineSubmission(
                pipeline_id=f"ROW_{idx}",
                developer="UNKNOWN",
                contract_path=str(manifest_path),
                code_path="",
                line_number=idx,
            )
            invalid_results.append(
                PipelineValidationResult(
                    submission=err_sub,
                    processing_status=PipelineProcessingStatus.INVALID_SUBMISSION,
                    errors=[f"Row {idx}: malformed CSV syntax - {e}"],
                )
            )
            continue

        if len(row_fields) != header_count:
            err_sub = PipelineSubmission(
                pipeline_id=f"ROW_{idx}",
                developer="UNKNOWN",
                contract_path=str(manifest_path),
                code_path="",
                line_number=idx,
            )
            invalid_results.append(
                PipelineValidationResult(
                    submission=err_sub,
                    processing_status=PipelineProcessingStatus.INVALID_SUBMISSION,
                    errors=[
                        f"Row {idx}: expected {header_count} columns, received {len(row_fields)}"
                    ],
                )
            )
            continue

        row_dict = dict(zip(header_list, row_fields))
        cleaned_row = {k: v.strip() for k, v in row_dict.items()}

        pipeline_id = cleaned_row.get("pipeline_id", "")
        developer = cleaned_row.get("developer", "")
        contract_path = cleaned_row.get("contract_path", "")
        code_path = cleaned_row.get("code_path", "")
        metadata_profile = cleaned_row.get("metadata_profile") or None
        runtime_run = cleaned_row.get("runtime_run") or None
        historical_runs = cleaned_row.get("historical_runs") or None

        sub = PipelineSubmission(
            pipeline_id=pipeline_id or f"ROW_{idx}",
            developer=developer or "UNKNOWN",
            contract_path=contract_path,
            code_path=code_path,
            metadata_profile=metadata_profile,
            runtime_run=runtime_run,
            historical_runs=historical_runs,
            line_number=idx,
        )

        row_errors: list[str] = []

        if not pipeline_id:
            row_errors.append(f"Row {idx}: missing required column 'pipeline_id'")
        elif pipeline_id in seen_pipeline_ids:
            row_errors.append(f"Row {idx}: duplicate pipeline_id '{pipeline_id}' in manifest")
        else:
            seen_pipeline_ids.add(pipeline_id)

        if not developer:
            row_errors.append(f"Row {idx}: missing required column 'developer'")

        if not contract_path:
            row_errors.append(f"Row {idx}: missing required column 'contract_path'")
        else:
            resolved_contract, err = resolve_safe_path(contract_path, base_dir, root_dir)
            if err:
                row_errors.append(f"Row {idx} contract_path error: {err}")
            elif resolved_contract:
                sub.resolved_contract_path = str(resolved_contract)

        if not code_path:
            row_errors.append(f"Row {idx}: missing required column 'code_path'")
        else:
            resolved_code, err = resolve_safe_path(code_path, base_dir, root_dir)
            if err:
                row_errors.append(f"Row {idx} code_path error: {err}")
            elif resolved_code:
                sub.resolved_code_path = str(resolved_code)

        if metadata_profile:
            resolved_meta, err = resolve_safe_path(metadata_profile, base_dir, root_dir)
            if err:
                row_errors.append(f"Row {idx} metadata_profile error: {err}")
            elif resolved_meta:
                sub.resolved_metadata_path = str(resolved_meta)

        if runtime_run:
            resolved_rt, err = resolve_safe_path(runtime_run, base_dir, root_dir)
            if err:
                row_errors.append(f"Row {idx} runtime_run error: {err}")
            elif resolved_rt:
                sub.resolved_runtime_path = str(resolved_rt)

        if historical_runs:
            resolved_hist, err = resolve_safe_path(historical_runs, base_dir, root_dir)
            if err:
                row_errors.append(f"Row {idx} historical_runs error: {err}")
            elif resolved_hist:
                sub.resolved_historical_path = str(resolved_hist)

        if row_errors:
            # Distinguish missing file vs invalid/unsafe submission
            status = PipelineProcessingStatus.INVALID_SUBMISSION
            if any("File not found" in e for e in row_errors) and not any(
                ("security violation" in e or "not allowed" in e) for e in row_errors
            ):
                status = PipelineProcessingStatus.MISSING_INPUT

            invalid_results.append(
                PipelineValidationResult(
                    submission=sub,
                    processing_status=status,
                    errors=row_errors,
                )
            )
        else:
            valid_submissions.append(sub)

    return valid_submissions, invalid_results
