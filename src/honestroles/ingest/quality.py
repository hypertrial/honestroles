from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
from typing import Any

import tomllib

from honestroles.errors import ConfigValidationError

INGEST_QUALITY_POLICY_SCHEMA_VERSION = "1.0"

_DEFAULT_REQUIRED_COLUMNS: tuple[str, ...] = (
    "id",
    "title",
    "apply_url",
    "posted_at",
    "source",
    "source_ref",
    "source_job_id",
    "source_payload_hash",
)

_DEFAULT_NULL_THRESHOLDS: dict[str, float] = {
    "id": 0.05,
    "title": 0.20,
    "apply_url": 0.40,
}

_DEFAULT_POSTED_AT_MAX_AGE_DAYS = 365
_DEFAULT_SOURCE_UPDATED_AT_MAX_AGE_DAYS = 730

_POLICY_ALLOWED_ROOT_KEYS = {
    "schema_version",
    "min_rows",
    "required_columns",
    "null_thresholds",
    "freshness",
}
_POLICY_ALLOWED_FRESHNESS_KEYS = {
    "posted_at_max_age_days",
    "source_updated_at_max_age_days",
}


@dataclass(frozen=True, slots=True)
class IngestQualityPolicy:
    min_rows: int = 1
    required_columns: tuple[str, ...] = _DEFAULT_REQUIRED_COLUMNS
    null_thresholds: dict[str, float] = field(
        default_factory=lambda: dict(_DEFAULT_NULL_THRESHOLDS)
    )
    posted_at_max_age_days: int | None = _DEFAULT_POSTED_AT_MAX_AGE_DAYS
    source_updated_at_max_age_days: int | None = _DEFAULT_SOURCE_UPDATED_AT_MAX_AGE_DAYS

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "schema_version": INGEST_QUALITY_POLICY_SCHEMA_VERSION,
            "min_rows": int(self.min_rows),
            "required_columns": list(self.required_columns),
            "null_thresholds": {
                str(key): float(value)
                for key, value in sorted(self.null_thresholds.items())
            },
            "freshness": {
                "posted_at_max_age_days": self.posted_at_max_age_days,
                "source_updated_at_max_age_days": self.source_updated_at_max_age_days,
            },
        }
        return payload


@dataclass(frozen=True, slots=True)
class IngestQualityResult:
    status: str
    summary: dict[str, int]
    checks: tuple[dict[str, Any], ...]
    check_codes: tuple[str, ...]


def load_ingest_quality_policy(
    policy_file: str | Path | None,
) -> tuple[IngestQualityPolicy, str, str]:
    if policy_file in (None, ""):
        policy = IngestQualityPolicy()
        return policy, "builtin", _policy_hash(policy)

    path = Path(policy_file).expanduser().resolve()
    try:
        raw = tomllib.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ConfigValidationError(f"cannot read ingest quality policy '{path}': {exc}") from exc
    except tomllib.TOMLDecodeError as exc:
        raise ConfigValidationError(
            f"invalid TOML in ingest quality policy '{path}': {exc}"
        ) from exc
    if not isinstance(raw, dict):
        raise ConfigValidationError(
            f"invalid ingest quality policy '{path}': root must be a table"
        )

    unknown = sorted(set(raw) - _POLICY_ALLOWED_ROOT_KEYS)
    if unknown:
        raise ConfigValidationError(
            f"invalid ingest quality policy '{path}': unknown keys: {', '.join(unknown)}"
        )
    schema_version = raw.get("schema_version", INGEST_QUALITY_POLICY_SCHEMA_VERSION)
    if schema_version != INGEST_QUALITY_POLICY_SCHEMA_VERSION:
        raise ConfigValidationError(
            f"invalid ingest quality policy '{path}': schema_version must be "
            f"'{INGEST_QUALITY_POLICY_SCHEMA_VERSION}'"
        )

    min_rows = _parse_int(raw.get("min_rows"), field_name="min_rows", minimum=1, default=1)
    required_columns = _parse_string_list(
        raw.get("required_columns"),
        field_name="required_columns",
        default=_DEFAULT_REQUIRED_COLUMNS,
    )
    null_thresholds = _parse_null_thresholds(raw.get("null_thresholds"))
    freshness = _parse_freshness(raw.get("freshness"))
    policy = IngestQualityPolicy(
        min_rows=min_rows,
        required_columns=required_columns,
        null_thresholds=null_thresholds,
        posted_at_max_age_days=freshness["posted_at_max_age_days"],
        source_updated_at_max_age_days=freshness["source_updated_at_max_age_days"],
    )
    return policy, str(path), _policy_hash(policy)


def evaluate_ingest_quality(
    records: list[dict[str, Any]],
    *,
    policy: IngestQualityPolicy,
    now_utc: datetime | None = None,
) -> IngestQualityResult:
    checks: list[dict[str, Any]] = []
    if now_utc is None:
        now_utc = datetime.now(UTC)
    row_count = len(records)
    observed_columns = sorted(
        {
            str(key)
            for record in records
            for key in record
        }
    )

    missing_columns = [col for col in policy.required_columns if col not in observed_columns]
    checks.append(
        _quality_check(
            check_id="quality.required_columns",
            code="INGEST_QUALITY_REQUIRED_COLUMNS",
            ok=not missing_columns,
            message=(
                "required columns are present"
                if not missing_columns
                else f"missing required columns: {', '.join(missing_columns)}"
            ),
            fix=(
                "check connector normalization mapping and ensure canonical columns are emitted"
                if missing_columns
                else None
            ),
        )
    )

    min_rows_ok = row_count >= policy.min_rows
    checks.append(
        _quality_check(
            check_id="quality.min_rows",
            code="INGEST_QUALITY_MIN_ROWS",
            ok=min_rows_ok,
            message=(
                f"row count {row_count} meets minimum {policy.min_rows}"
                if min_rows_ok
                else f"row count {row_count} is below minimum {policy.min_rows}"
            ),
            fix=(
                "increase max-pages/max-jobs or confirm the source-ref points to an active board"
                if not min_rows_ok
                else None
            ),
        )
    )

    for column in sorted(policy.null_thresholds):
        threshold = policy.null_thresholds[column]
        null_count = 0
        for record in records:
            if _is_missing(record.get(column)):
                null_count += 1
        ratio = (null_count / row_count) if row_count > 0 else 1.0
        checks.append(
            _quality_check(
                check_id=f"quality.null_rate.{column}",
                code="INGEST_QUALITY_NULL_RATE",
                ok=ratio <= threshold,
                message=(
                    f"{column} null rate {ratio:.3f} <= {threshold:.3f}"
                    if ratio <= threshold
                    else f"{column} null rate {ratio:.3f} exceeds {threshold:.3f}"
                ),
                fix=(
                    "adjust source mapping or relax null thresholds in ingest_quality.toml"
                    if ratio > threshold
                    else None
                ),
            )
        )

    checks.extend(
        _timestamp_quality_checks(
            records=records,
            column="posted_at",
            max_age_days=policy.posted_at_max_age_days,
            now_utc=now_utc,
            parse_code="INGEST_QUALITY_POSTED_AT_PARSEABLE",
            freshness_code="INGEST_QUALITY_POSTED_AT_FRESHNESS",
        )
    )
    checks.extend(
        _timestamp_quality_checks(
            records=records,
            column="source_updated_at",
            max_age_days=policy.source_updated_at_max_age_days,
            now_utc=now_utc,
            parse_code="INGEST_QUALITY_SOURCE_UPDATED_AT_PARSEABLE",
            freshness_code="INGEST_QUALITY_SOURCE_UPDATED_AT_FRESHNESS",
        )
    )

    summary = {"pass": 0, "warn": 0, "fail": 0}
    check_codes: list[str] = []
    seen_codes: set[str] = set()
    for check in checks:
        status = str(check.get("status", "pass"))
        if status in summary:
            summary[status] += 1
        if status in {"warn", "fail"}:
            code = str(check.get("code", "")).strip()
            if code and code not in seen_codes:
                seen_codes.add(code)
                check_codes.append(code)
    if summary["fail"] > 0:
        status = "fail"
    elif summary["warn"] > 0:
        status = "warn"
    else:
        status = "pass"
    return IngestQualityResult(
        status=status,
        summary=summary,
        checks=tuple(checks),
        check_codes=tuple(check_codes),
    )


def _timestamp_quality_checks(
    *,
    records: list[dict[str, Any]],
    column: str,
    max_age_days: int | None,
    now_utc: datetime,
    parse_code: str,
    freshness_code: str,
) -> list[dict[str, Any]]:
    non_empty_values: list[str] = []
    for record in records:
        value = record.get(column)
        text = _text_or_none(value)
        if text is not None:
            non_empty_values.append(text)
    parsed_values: list[datetime] = []
    unparseable = 0
    for text in non_empty_values:
        parsed = _parse_datetime(text)
        if parsed is None:
            unparseable += 1
            continue
        parsed_values.append(parsed)

    parse_ok = unparseable == 0
    checks = [
        _quality_check(
            check_id=f"quality.timestamp_parse.{column}",
            code=parse_code,
            ok=parse_ok,
            message=(
                f"{column} timestamps are parseable"
                if parse_ok
                else f"{column} contains {unparseable} unparseable timestamp values"
            ),
            fix=(
                "normalize timestamp formatting to ISO8601 in connector mappings"
                if not parse_ok
                else None
            ),
        )
    ]
    if max_age_days is None:
        return checks
    if not parsed_values:
        checks.append(
            _quality_check(
                check_id=f"quality.freshness.{column}",
                code=freshness_code,
                ok=False,
                message=f"{column} freshness cannot be evaluated without parseable values",
                fix=f"ensure {column} is populated with parseable timestamps",
            )
        )
        return checks

    newest = max(parsed_values)
    age_days = int((now_utc - newest).total_seconds() / 86400)
    freshness_ok = age_days <= max_age_days
    checks.append(
        _quality_check(
            check_id=f"quality.freshness.{column}",
            code=freshness_code,
            ok=freshness_ok,
            message=(
                f"{column} freshness age {age_days}d <= {max_age_days}d"
                if freshness_ok
                else f"{column} freshness age {age_days}d exceeds {max_age_days}d"
            ),
            fix=(
                "check source-ref activity or increase freshness threshold in ingest_quality.toml"
                if not freshness_ok
                else None
            ),
        )
    )
    return checks


def _quality_check(
    *,
    check_id: str,
    code: str,
    ok: bool,
    message: str,
    fix: str | None = None,
) -> dict[str, Any]:
    status = "pass" if ok else "warn"
    payload: dict[str, Any] = {
        "id": check_id,
        "code": code,
        "severity": "info" if ok else "warn",
        "status": status,
        "message": message,
        "fix": fix if not ok else None,
    }
    return payload


def _is_missing(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    return False


def _parse_datetime(value: str) -> datetime | None:
    text = value.strip()
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


def _text_or_none(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _parse_int(value: object, *, field_name: str, minimum: int, default: int) -> int:
    if value is None:
        return default
    if not isinstance(value, int):
        raise ConfigValidationError(f"ingest quality policy: {field_name} must be an integer")
    if value < minimum:
        raise ConfigValidationError(f"ingest quality policy: {field_name} must be >= {minimum}")
    return value


def _parse_string_list(
    value: object,
    *,
    field_name: str,
    default: tuple[str, ...],
) -> tuple[str, ...]:
    if value is None:
        return default
    if not isinstance(value, list):
        raise ConfigValidationError(f"ingest quality policy: {field_name} must be an array")
    out: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item.strip():
            raise ConfigValidationError(
                f"ingest quality policy: {field_name} must contain non-empty strings"
            )
        out.append(item.strip())
    return tuple(out)


def _parse_null_thresholds(value: object) -> dict[str, float]:
    if value is None:
        return dict(_DEFAULT_NULL_THRESHOLDS)
    if not isinstance(value, dict):
        raise ConfigValidationError("ingest quality policy: null_thresholds must be a table")
    out: dict[str, float] = {}
    for key in sorted(value):
        if not isinstance(key, str) or not key.strip():
            raise ConfigValidationError(
                "ingest quality policy: null_thresholds keys must be non-empty strings"
            )
        raw = value[key]
        if not isinstance(raw, (int, float)):
            raise ConfigValidationError(
                f"ingest quality policy: null_thresholds.{key} must be numeric"
            )
        parsed = float(raw)
        if parsed < 0 or parsed > 1:
            raise ConfigValidationError(
                f"ingest quality policy: null_thresholds.{key} must be within [0, 1]"
            )
        out[key.strip()] = parsed
    return out


def _parse_freshness(value: object) -> dict[str, int | None]:
    defaults = {
        "posted_at_max_age_days": _DEFAULT_POSTED_AT_MAX_AGE_DAYS,
        "source_updated_at_max_age_days": _DEFAULT_SOURCE_UPDATED_AT_MAX_AGE_DAYS,
    }
    if value is None:
        return defaults
    if not isinstance(value, dict):
        raise ConfigValidationError("ingest quality policy: freshness must be a table")
    unknown = sorted(set(value) - _POLICY_ALLOWED_FRESHNESS_KEYS)
    if unknown:
        raise ConfigValidationError(
            "ingest quality policy: unknown freshness keys: " + ", ".join(unknown)
        )
    out: dict[str, int | None] = dict(defaults)
    for key in _POLICY_ALLOWED_FRESHNESS_KEYS:
        raw = value.get(key)
        if raw is None:
            continue
        if not isinstance(raw, int):
            raise ConfigValidationError(f"ingest quality policy: freshness.{key} must be integer")
        if raw < 0:
            raise ConfigValidationError(f"ingest quality policy: freshness.{key} must be >= 0")
        out[key] = raw
    return out


def _policy_hash(policy: IngestQualityPolicy) -> str:
    digest = hashlib.sha256()
    digest.update(json.dumps(policy.to_dict(), sort_keys=True).encode("utf-8"))
    return digest.hexdigest()
