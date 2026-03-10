from __future__ import annotations

from pathlib import Path
from typing import Any

import tomllib

from honestroles.errors import ConfigValidationError
from honestroles.ingest.models import (
    IngestionDefaults,
    IngestionManifest,
    IngestionSourceConfig,
    IngestionSource,
    SUPPORTED_INGEST_SOURCES,
)

_DEFAULTS_ALLOWED_KEYS = {
    "state_file",
    "write_raw",
    "max_pages",
    "max_jobs",
    "full_refresh",
    "timeout_seconds",
    "max_retries",
    "base_backoff_seconds",
    "user_agent",
}

_SOURCE_ALLOWED_KEYS = {
    "source",
    "source_ref",
    "enabled",
    "output_parquet",
    "report_file",
    "state_file",
    "write_raw",
    "max_pages",
    "max_jobs",
    "full_refresh",
    "timeout_seconds",
    "max_retries",
    "base_backoff_seconds",
    "user_agent",
}


def load_ingest_manifest(path: str | Path) -> IngestionManifest:
    manifest_path = Path(path).expanduser().resolve()
    raw = _read_manifest_toml(manifest_path)
    defaults = _parse_defaults(
        raw.get("defaults", {}),
        base_dir=manifest_path.parent,
        manifest_path=manifest_path,
    )
    raw_sources = raw.get("sources")
    if not isinstance(raw_sources, list):
        raise ConfigValidationError(
            f"invalid ingest manifest '{manifest_path}': [[sources]] must be provided"
        )
    sources = tuple(
        _parse_source(item, index=index, base_dir=manifest_path.parent, manifest_path=manifest_path)
        for index, item in enumerate(raw_sources)
    )
    if not sources:
        raise ConfigValidationError(
            f"invalid ingest manifest '{manifest_path}': at least one [[sources]] entry is required"
        )
    return IngestionManifest(path=manifest_path, defaults=defaults, sources=sources)


def _read_manifest_toml(path: Path) -> dict[str, Any]:
    try:
        payload = tomllib.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ConfigValidationError(f"cannot read ingest manifest '{path}': {exc}") from exc
    except tomllib.TOMLDecodeError as exc:
        raise ConfigValidationError(f"invalid TOML in ingest manifest '{path}': {exc}") from exc
    if not isinstance(payload, dict):
        raise ConfigValidationError(f"invalid ingest manifest '{path}': root must be a table")
    return payload


def _parse_defaults(
    raw: object,
    *,
    base_dir: Path,
    manifest_path: Path,
) -> IngestionDefaults:
    if raw in (None, {}):
        return IngestionDefaults()
    if not isinstance(raw, dict):
        raise ConfigValidationError(
            f"invalid ingest manifest '{manifest_path}': [defaults] must be a table"
        )
    unknown = sorted(set(raw) - _DEFAULTS_ALLOWED_KEYS)
    if unknown:
        raise ConfigValidationError(
            f"invalid ingest manifest '{manifest_path}': unknown defaults keys: {', '.join(unknown)}"
        )
    return IngestionDefaults(
        state_file=_parse_optional_path(raw.get("state_file"), base_dir, "defaults.state_file") or IngestionDefaults().state_file,
        write_raw=_parse_bool(raw.get("write_raw"), "defaults.write_raw", default=IngestionDefaults().write_raw),
        max_pages=_parse_int(raw.get("max_pages"), "defaults.max_pages", default=IngestionDefaults().max_pages, minimum=1),
        max_jobs=_parse_int(raw.get("max_jobs"), "defaults.max_jobs", default=IngestionDefaults().max_jobs, minimum=1),
        full_refresh=_parse_bool(raw.get("full_refresh"), "defaults.full_refresh", default=IngestionDefaults().full_refresh),
        timeout_seconds=_parse_float(raw.get("timeout_seconds"), "defaults.timeout_seconds", default=IngestionDefaults().timeout_seconds, minimum=0.1),
        max_retries=_parse_int(raw.get("max_retries"), "defaults.max_retries", default=IngestionDefaults().max_retries, minimum=0),
        base_backoff_seconds=_parse_float(raw.get("base_backoff_seconds"), "defaults.base_backoff_seconds", default=IngestionDefaults().base_backoff_seconds, minimum=0.0),
        user_agent=_parse_string(raw.get("user_agent"), "defaults.user_agent", default=IngestionDefaults().user_agent),
    )


def _parse_source(
    raw: object,
    *,
    index: int,
    base_dir: Path,
    manifest_path: Path,
) -> IngestionSourceConfig:
    label = f"sources[{index}]"
    if not isinstance(raw, dict):
        raise ConfigValidationError(
            f"invalid ingest manifest '{manifest_path}': [[sources]] entry {index} must be a table"
        )
    unknown = sorted(set(raw) - _SOURCE_ALLOWED_KEYS)
    if unknown:
        raise ConfigValidationError(
            f"invalid ingest manifest '{manifest_path}': unknown keys in [[sources]] {index}: {', '.join(unknown)}"
        )
    source = _parse_source_name(raw.get("source"), f"{label}.source")
    source_ref = _parse_string(raw.get("source_ref"), f"{label}.source_ref", default=None)
    if source_ref is None:
        raise ConfigValidationError(
            f"invalid ingest manifest '{manifest_path}': {label}.source_ref is required"
        )
    enabled = _parse_bool(raw.get("enabled"), f"{label}.enabled", default=True)
    return IngestionSourceConfig(
        source=source,
        source_ref=source_ref,
        enabled=enabled,
        output_parquet=_parse_optional_path(raw.get("output_parquet"), base_dir, f"{label}.output_parquet"),
        report_file=_parse_optional_path(raw.get("report_file"), base_dir, f"{label}.report_file"),
        state_file=_parse_optional_path(raw.get("state_file"), base_dir, f"{label}.state_file"),
        write_raw=_parse_optional_bool(raw.get("write_raw"), f"{label}.write_raw"),
        max_pages=_parse_optional_int(raw.get("max_pages"), f"{label}.max_pages", minimum=1),
        max_jobs=_parse_optional_int(raw.get("max_jobs"), f"{label}.max_jobs", minimum=1),
        full_refresh=_parse_optional_bool(raw.get("full_refresh"), f"{label}.full_refresh"),
        timeout_seconds=_parse_optional_float(raw.get("timeout_seconds"), f"{label}.timeout_seconds", minimum=0.1),
        max_retries=_parse_optional_int(raw.get("max_retries"), f"{label}.max_retries", minimum=0),
        base_backoff_seconds=_parse_optional_float(raw.get("base_backoff_seconds"), f"{label}.base_backoff_seconds", minimum=0.0),
        user_agent=_parse_string(raw.get("user_agent"), f"{label}.user_agent", default=None),
    )


def _parse_source_name(value: object, field_name: str) -> IngestionSource:
    source = _parse_string(value, field_name, default=None)
    if source is None:
        raise ConfigValidationError(f"{field_name} is required")
    if source not in SUPPORTED_INGEST_SOURCES:
        valid = ", ".join(SUPPORTED_INGEST_SOURCES)
        raise ConfigValidationError(f"{field_name} must be one of: {valid}")
    return source


def _parse_optional_path(value: object, base_dir: Path, field_name: str) -> Path | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ConfigValidationError(f"{field_name} must be a string path")
    candidate = Path(value).expanduser()
    if not candidate.is_absolute():
        candidate = (base_dir / candidate).resolve()
    else:
        candidate = candidate.resolve()
    return candidate


def _parse_string(value: object, field_name: str, default: str | None) -> str | None:
    if value is None:
        return default
    if not isinstance(value, str):
        raise ConfigValidationError(f"{field_name} must be a string")
    text = value.strip()
    if not text:
        raise ConfigValidationError(f"{field_name} must be non-empty")
    return text


def _parse_bool(value: object, field_name: str, default: bool) -> bool:
    if value is None:
        return default
    if not isinstance(value, bool):
        raise ConfigValidationError(f"{field_name} must be a boolean")
    return value


def _parse_optional_bool(value: object, field_name: str) -> bool | None:
    if value is None:
        return None
    if not isinstance(value, bool):
        raise ConfigValidationError(f"{field_name} must be a boolean")
    return value


def _parse_int(value: object, field_name: str, default: int, minimum: int) -> int:
    if value is None:
        return default
    if not isinstance(value, int):
        raise ConfigValidationError(f"{field_name} must be an integer")
    if value < minimum:
        raise ConfigValidationError(f"{field_name} must be >= {minimum}")
    return value


def _parse_optional_int(value: object, field_name: str, minimum: int) -> int | None:
    if value is None:
        return None
    if not isinstance(value, int):
        raise ConfigValidationError(f"{field_name} must be an integer")
    if value < minimum:
        raise ConfigValidationError(f"{field_name} must be >= {minimum}")
    return value


def _parse_float(value: object, field_name: str, default: float, minimum: float) -> float:
    if value is None:
        return default
    if not isinstance(value, (int, float)):
        raise ConfigValidationError(f"{field_name} must be numeric")
    parsed = float(value)
    if parsed < minimum:
        raise ConfigValidationError(f"{field_name} must be >= {minimum}")
    return parsed


def _parse_optional_float(value: object, field_name: str, minimum: float) -> float | None:
    if value is None:
        return None
    if not isinstance(value, (int, float)):
        raise ConfigValidationError(f"{field_name} must be numeric")
    parsed = float(value)
    if parsed < minimum:
        raise ConfigValidationError(f"{field_name} must be >= {minimum}")
    return parsed
