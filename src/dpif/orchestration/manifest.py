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
    root_dir: Path | None = None,
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

    # Path traversal detection
    if ".." in clean_raw.replace("\\", "/").split("/"):
        # Check if resolved path stays within base_dir or root_dir
        pass

    path_obj = Path(clean_raw)
    resolved: Path | None = None

    if path_obj.is_absolute():
        resolved = path_obj
    else:
        # First try relative to base_dir (directory containing manifest)
        cand1 = (base_dir / path_obj).resolve()
        if cand1.exists():
            resolved = cand1
        else:
            # Fallback to repo root or cwd
            r = root_dir or _repo_root(base_dir)
            cand2 = (r / path_obj).resolve()
            if cand2.exists():
                resolved = cand2
            else:
                # Store resolved path for error reporting
                resolved = cand1

    # Security check: ensure path does not escape root_dir / base_dir if strictly constrained
    if root_dir:
        try:
            resolved.relative_to(root_dir.resolve())
        except ValueError:
            msg = f"Path traversal security violation: '{clean_raw}' outside root"
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
    """Parse and validate a multi-pipeline CSV manifest.

    Returns:
        tuple of (valid_submissions, invalid_results)
    """
    path = Path(manifest_path)
    base_dir = path.parent.resolve()
    root_dir = allowed_root.resolve() if allowed_root else _repo_root(base_dir)

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

    lines = [line.strip() for line in content.splitlines() if line.strip()]
    if not lines:
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

    reader = csv.DictReader(lines)
    fieldnames = reader.fieldnames or []
    present_headers = {h.strip() for h in fieldnames if h}

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

    seen_pipeline_ids: set[str] = set()

    for idx, row in enumerate(reader, start=2):
        cleaned_row = {k.strip(): (v.strip() if v else "") for k, v in row.items() if k}

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
            # Check if it's missing input vs invalid submission
            status = PipelineProcessingStatus.INVALID_SUBMISSION
            if any("File not found" in e for e in row_errors):
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
