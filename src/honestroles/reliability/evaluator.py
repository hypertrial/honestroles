from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
import os
import sys
from typing import Any

import polars as pl

from honestroles.config import load_pipeline_config, load_plugin_manifest
from honestroles.errors import ConfigValidationError
from honestroles.io import (
    apply_source_adapter,
    normalize_source_data_contract,
    read_parquet,
    resolve_source_aliases,
    validate_source_data_contract,
)

from .policy import ReliabilityPolicy, load_reliability_policy


@dataclass(frozen=True, slots=True)
class ReliabilityEvaluation:
    status: str
    summary: dict[str, int]
    checks: list[dict[str, Any]]
    check_codes: list[str]
    policy_source: str
    policy_hash: str
    has_config_input_error: bool


def evaluate_reliability(
    *,
    pipeline_config: str,
    plugin_manifest: str | None,
    sample_rows: int,
    policy_file: str | None,
    validate_source_data_contract_fn: Callable[[pl.DataFrame], Any] = validate_source_data_contract,
) -> ReliabilityEvaluation:
    if sample_rows < 1:
        raise ConfigValidationError("sample-rows must be >= 1")

    loaded_policy = load_reliability_policy(policy_file)
    policy = loaded_policy.policy

    checks: list[dict[str, Any]] = []
    has_config_input_error = False

    _append_check(
        checks,
        check_id="python_version",
        code="ENV_PYTHON_VERSION",
        status="pass" if sys.version_info >= (3, 11) else "fail",
        message=f"Python {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
        fix="Use Python >= 3.11",
    )

    missing_imports: list[str] = []
    for module in ("polars", "pydantic"):
        try:
            __import__(module)
        except Exception:
            missing_imports.append(module)
    if missing_imports:
        _append_check(
            checks,
            check_id="required_imports",
            code="ENV_REQUIRED_IMPORTS",
            status="fail",
            message=f"Missing imports: {', '.join(sorted(missing_imports))}",
            fix="Install package dependencies (e.g. pip install honestroles)",
        )
    else:
        _append_check(
            checks,
            check_id="required_imports",
            code="ENV_REQUIRED_IMPORTS",
            status="pass",
            message="Required runtime imports are available",
            fix="-",
        )

    cfg = None
    try:
        cfg = load_pipeline_config(pipeline_config)
    except ConfigValidationError as exc:
        has_config_input_error = True
        _append_check(
            checks,
            check_id="pipeline_config",
            code="CONFIG_PIPELINE_PARSE",
            status="fail",
            message=str(exc),
            fix="Run: honestroles config validate --pipeline <pipeline.toml>",
        )
    else:
        _append_check(
            checks,
            check_id="pipeline_config",
            code="CONFIG_PIPELINE_PARSE",
            status="pass",
            message=f"Loaded pipeline config: {cfg.input.path}",
            fix="-",
        )

    if plugin_manifest:
        try:
            load_plugin_manifest(plugin_manifest)
        except ConfigValidationError as exc:
            has_config_input_error = True
            _append_check(
                checks,
                check_id="plugin_manifest",
                code="CONFIG_PLUGIN_MANIFEST_PARSE",
                status="fail",
                message=str(exc),
                fix="Run: honestroles plugins validate --manifest <plugins.toml>",
            )
        else:
            _append_check(
                checks,
                check_id="plugin_manifest",
                code="CONFIG_PLUGIN_MANIFEST_PARSE",
                status="pass",
                message="Plugin manifest is valid",
                fix="-",
            )
    else:
        _append_check(
            checks,
            check_id="plugin_manifest",
            code="CONFIG_PLUGIN_MANIFEST_PARSE",
            status="pass",
            message="No plugin manifest provided",
            fix="-",
        )

    sample: pl.DataFrame | None = None
    aliased: pl.DataFrame | None = None
    normalized: pl.DataFrame | None = None

    if cfg is not None:
        input_path = cfg.input.path
        if not input_path.exists():
            _append_check(
                checks,
                check_id="input_exists",
                code="INPUT_EXISTS",
                status="fail",
                message=f"Input parquet missing: {input_path}",
                fix="Set [input].path to an existing parquet file",
                fix_snippet='[input]\npath = "<existing.parquet>"',
            )
        else:
            _append_check(
                checks,
                check_id="input_exists",
                code="INPUT_EXISTS",
                status="pass",
                message=f"Input parquet exists: {input_path}",
                fix="-",
            )
            try:
                sample = read_parquet(input_path).head(sample_rows)
            except Exception as exc:
                _append_check(
                    checks,
                    check_id="input_sample_read",
                    code="INPUT_SAMPLE_READ",
                    status="fail",
                    message=f"Failed reading input sample: {exc}",
                    fix="Verify parquet readability and permissions",
                )
            else:
                _append_check(
                    checks,
                    check_id="input_sample_read",
                    code="INPUT_SAMPLE_READ",
                    status="pass",
                    message=f"Read sample rows: {sample.height}",
                    fix="-",
                )
                try:
                    adapted, _ = apply_source_adapter(sample, cfg.input.adapter)
                    aliased, _ = resolve_source_aliases(adapted, cfg.input.aliases)
                    normalized = normalize_source_data_contract(aliased)
                    validate_source_data_contract_fn(normalized)
                except ConfigValidationError as exc:
                    _append_check(
                        checks,
                        check_id="canonical_contract",
                        code="INPUT_CANONICAL_CONTRACT",
                        status="fail",
                        message=str(exc),
                        fix="Update input aliases/adapter mappings to populate canonical fields",
                        fix_snippet=(
                            "[input.aliases]\n"
                            'title = ["title_text"]\n'
                            'description_text = ["job_description"]'
                        ),
                    )
                else:
                    _append_check(
                        checks,
                        check_id="canonical_contract",
                        code="INPUT_CANONICAL_CONTRACT",
                        status="pass",
                        message="Canonical contract validation passed",
                        fix="-",
                    )

                    if normalized.height == 0:
                        _append_check(
                            checks,
                            check_id="content_readiness",
                            code="INPUT_CONTENT_READINESS",
                            status="warn",
                            message="Input sample is empty",
                            fix="Verify source extraction returns rows",
                        )
                    elif normalized["title"].null_count() == normalized.height:
                        _append_check(
                            checks,
                            check_id="content_readiness",
                            code="INPUT_CONTENT_READINESS",
                            status="warn",
                            message="All sampled rows have null title",
                            fix="Map title via [input.aliases] or [input.adapter]",
                            fix_snippet='[input.aliases]\ntitle = ["title_text"]',
                        )
                    else:
                        _append_check(
                            checks,
                            check_id="content_readiness",
                            code="INPUT_CONTENT_READINESS",
                            status="pass",
                            message="Sample contains required content signals",
                            fix="-",
                        )

        if cfg.output is None:
            _append_check(
                checks,
                check_id="output_path",
                code="OUTPUT_PATH_WRITABLE",
                status="warn",
                message="No [output] path configured",
                fix="Add [output].path to persist pipeline results",
                fix_snippet='[output]\npath = "dist/jobs_scored.parquet"',
            )
        else:
            parent = cfg.output.path.parent
            if parent.exists():
                writable = parent.is_dir() and os.access(parent, os.W_OK)
                _append_check(
                    checks,
                    check_id="output_path",
                    code="OUTPUT_PATH_WRITABLE",
                    status="pass" if writable else "fail",
                    message=f"Output parent directory: {parent}",
                    fix="Ensure output directory exists and is writable",
                )
            else:
                _append_check(
                    checks,
                    check_id="output_path",
                    code="OUTPUT_PATH_WRITABLE",
                    status="warn",
                    message=f"Output parent directory does not exist: {parent}",
                    fix=f"Create directory '{parent}' before running pipeline",
                    fix_snippet=f"mkdir -p {parent}",
                )
    else:
        _append_check(
            checks,
            check_id="output_path",
            code="OUTPUT_PATH_WRITABLE",
            status="warn",
            message="Output path check skipped because pipeline config failed to load",
            fix="Fix pipeline config first",
        )

    _evaluate_policy_checks(
        checks=checks,
        sample=sample,
        aliased=aliased,
        normalized=normalized,
        policy=policy,
    )

    status, summary = _status_summary(checks)
    check_codes = _warn_fail_codes(checks)
    return ReliabilityEvaluation(
        status=status,
        summary=summary,
        checks=checks,
        check_codes=check_codes,
        policy_source=loaded_policy.source,
        policy_hash=loaded_policy.policy_hash,
        has_config_input_error=has_config_input_error,
    )


def _evaluate_policy_checks(
    *,
    checks: list[dict[str, Any]],
    sample: pl.DataFrame | None,
    aliased: pl.DataFrame | None,
    normalized: pl.DataFrame | None,
    policy: ReliabilityPolicy,
) -> None:
    if sample is None:
        _append_check(
            checks,
            check_id="policy_min_rows",
            code="POLICY_MIN_ROWS",
            status="warn",
            message="Policy min_rows not evaluated because sample is unavailable",
            fix="Fix input read failures before policy evaluation",
        )
    elif sample.height == 0:
        _append_check(
            checks,
            check_id="policy_min_rows",
            code="POLICY_MIN_ROWS",
            status="fail",
            message="Sample row count is 0; cannot satisfy reliability minimum rows",
            fix="Verify source extraction and upstream data availability",
        )
    elif sample.height < policy.min_rows:
        _append_check(
            checks,
            check_id="policy_min_rows",
            code="POLICY_MIN_ROWS",
            status="warn",
            message=f"Sample row count {sample.height} is below policy min_rows {policy.min_rows}",
            fix="Increase input coverage or lower policy min_rows",
        )
    else:
        _append_check(
            checks,
            check_id="policy_min_rows",
            code="POLICY_MIN_ROWS",
            status="pass",
            message=f"Sample row count {sample.height} meets policy min_rows {policy.min_rows}",
            fix="-",
        )

    required_frame = aliased if aliased is not None else sample
    if required_frame is None:
        _append_check(
            checks,
            check_id="policy_required_columns",
            code="POLICY_REQUIRED_COLUMNS",
            status="warn",
            message="Policy required_columns not evaluated because sample is unavailable",
            fix="Fix input read failures before policy evaluation",
        )
    else:
        missing = [name for name in policy.required_columns if name not in required_frame.columns]
        if missing:
            _append_check(
                checks,
                check_id="policy_required_columns",
                code="POLICY_REQUIRED_COLUMNS",
                status="warn",
                message=f"Required columns missing from adapted input: {', '.join(sorted(missing))}",
                fix="Map required columns with aliases or adapter rules",
                fix_snippet='[input.aliases]\n<column> = ["source_column"]',
            )
        else:
            _append_check(
                checks,
                check_id="policy_required_columns",
                code="POLICY_REQUIRED_COLUMNS",
                status="pass",
                message="All policy required_columns are present",
                fix="-",
            )

    null_frame = normalized if normalized is not None else required_frame
    if null_frame is None or null_frame.height == 0:
        _append_check(
            checks,
            check_id="policy_null_rate",
            code="POLICY_NULL_RATE",
            status="warn",
            message="Policy null-rate checks skipped because sample has no rows",
            fix="Increase sample size or verify ingestion",
        )
    else:
        violations: list[str] = []
        for column, threshold in sorted((policy.max_null_pct or {}).items()):
            if column not in null_frame.columns:
                continue
            null_pct = (null_frame[column].null_count() / null_frame.height) * 100.0
            if null_pct > threshold:
                violations.append(f"{column}={null_pct:.2f}%>{threshold:.2f}%")
        if violations:
            _append_check(
                checks,
                check_id="policy_null_rate",
                code="POLICY_NULL_RATE",
                status="warn",
                message="Null-rate threshold exceeded: " + ", ".join(violations),
                fix="Map or clean affected fields to reduce null ratios",
                fix_snippet='[input.adapter.fields.<column>]\nfrom = ["source_column"]',
            )
        else:
            _append_check(
                checks,
                check_id="policy_null_rate",
                code="POLICY_NULL_RATE",
                status="pass",
                message="Policy null-rate thresholds satisfied",
                fix="-",
            )

    freshness_frame = normalized if normalized is not None else required_frame
    freshness_column = policy.freshness.column
    if freshness_frame is None:
        _append_check(
            checks,
            check_id="policy_freshness",
            code="POLICY_FRESHNESS",
            status="warn",
            message="Freshness check skipped because sample is unavailable",
            fix="Fix input read failures before policy evaluation",
        )
    elif freshness_column not in freshness_frame.columns:
        _append_check(
            checks,
            check_id="policy_freshness",
            code="POLICY_FRESHNESS",
            status="warn",
            message=f"Freshness column '{freshness_column}' is missing",
            fix="Set freshness.column to an available date-like field",
        )
    else:
        latest = _latest_timestamp(freshness_frame[freshness_column])
        if latest is None:
            _append_check(
                checks,
                check_id="policy_freshness",
                code="POLICY_FRESHNESS",
                status="warn",
                message=(
                    f"Freshness column '{freshness_column}' has no parseable timestamps"
                ),
                fix="Normalize freshness column values to ISO dates/timestamps",
            )
        else:
            age_days = int((datetime.now(UTC) - latest).total_seconds() // 86400)
            if age_days > policy.freshness.max_age_days:
                _append_check(
                    checks,
                    check_id="policy_freshness",
                    code="POLICY_FRESHNESS",
                    status="warn",
                    message=(
                        f"Latest {freshness_column} is {age_days} days old "
                        f"(policy max {policy.freshness.max_age_days})"
                    ),
                    fix="Refresh source extraction cadence or adjust policy max_age_days",
                )
            else:
                _append_check(
                    checks,
                    check_id="policy_freshness",
                    code="POLICY_FRESHNESS",
                    status="pass",
                    message=(
                        f"Freshness check passed: latest {freshness_column} age is {age_days} days"
                    ),
                    fix="-",
                )


def _latest_timestamp(series: pl.Series) -> datetime | None:
    latest: datetime | None = None
    for value in series.to_list():
        parsed = _parse_datetime(value)
        if parsed is None:
            continue
        if latest is None or parsed > latest:
            latest = parsed
    return latest


def _parse_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _append_check(
    checks: list[dict[str, Any]],
    *,
    check_id: str,
    code: str,
    status: str,
    message: str,
    fix: str,
    fix_snippet: str | None = None,
) -> None:
    severity = "info" if status == "pass" else status
    payload: dict[str, Any] = {
        "id": check_id,
        "code": code,
        "status": status,
        "severity": severity,
        "message": message,
        "fix": fix,
    }
    if fix_snippet not in (None, ""):
        payload["fix_snippet"] = fix_snippet
    checks.append(payload)


def _status_summary(checks: list[dict[str, Any]]) -> tuple[str, dict[str, int]]:
    summary = {"pass": 0, "warn": 0, "fail": 0}
    for item in checks:
        state = str(item["status"])
        summary[state] = summary.get(state, 0) + 1
    if summary["fail"] > 0:
        return "fail", summary
    if summary["warn"] > 0:
        return "warn", summary
    return "pass", summary


def _warn_fail_codes(checks: list[dict[str, Any]]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in checks:
        status = str(item.get("status", ""))
        if status not in {"warn", "fail"}:
            continue
        code = str(item.get("code", "")).strip()
        if not code or code in seen:
            continue
        seen.add(code)
        out.append(code)
    return out
