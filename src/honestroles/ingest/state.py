from __future__ import annotations

from datetime import UTC, datetime
import json
from pathlib import Path
from typing import Any

from honestroles.errors import ConfigValidationError
from honestroles.ingest.models import (
    INGEST_STATE_SCHEMA_VERSION,
    IngestionStateEntry,
)

_MAX_RECENT_IDS = 500


def state_key(source: str, source_ref: str) -> str:
    return f"{source}::{source_ref}"


def load_state(path: str | Path) -> dict[str, IngestionStateEntry]:
    state_path = Path(path).expanduser().resolve()
    if not state_path.exists():
        return {}
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ConfigValidationError(f"invalid ingestion state file '{state_path}': {exc}") from exc
    if not isinstance(payload, dict):
        raise ConfigValidationError(f"invalid ingestion state file '{state_path}': root must be an object")
    entries = payload.get("entries", {})
    if not isinstance(entries, dict):
        raise ConfigValidationError(f"invalid ingestion state file '{state_path}': 'entries' must be an object")
    out: dict[str, IngestionStateEntry] = {}
    for key, value in entries.items():
        if not isinstance(key, str):
            continue
        if not isinstance(value, dict):
            continue
        out[key] = IngestionStateEntry.from_mapping(value)
    return out


def write_state(path: str | Path, entries: dict[str, IngestionStateEntry]) -> Path:
    state_path = Path(path).expanduser().resolve()
    state_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": INGEST_STATE_SCHEMA_VERSION,
        "entries": {k: v.to_dict() for k, v in sorted(entries.items())},
    }
    state_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return state_path


def filter_incremental(
    records: list[dict[str, Any]],
    *,
    entry: IngestionStateEntry | None,
    full_refresh: bool,
) -> tuple[list[dict[str, Any]], str | None, int]:
    if full_refresh or entry is None:
        return records, entry.high_watermark_posted_at if entry else None, 0
    watermark = entry.high_watermark_posted_at
    watermark_updated = entry.high_watermark_updated_at
    seen_ids = set(entry.recent_source_job_ids)
    if not watermark and not seen_ids:
        return records, watermark, 0

    filtered: list[dict[str, Any]] = []
    watermark_dt = _parse_iso(watermark)
    watermark_updated_dt = _parse_iso(watermark_updated)
    skipped = 0
    for record in records:
        source_job_id = _text_or_none(record.get("source_job_id"))
        if source_job_id is not None and source_job_id in seen_ids:
            skipped += 1
            continue
        posted_dt = _parse_iso(_text_or_none(record.get("posted_at")))
        updated_dt = _parse_iso(_text_or_none(record.get("source_updated_at")))
        effective_dt = _max_dt(posted_dt, updated_dt)
        effective_watermark = _max_dt(watermark_dt, watermark_updated_dt)
        if effective_watermark is not None and effective_dt is not None and effective_dt <= effective_watermark:
            skipped += 1
            continue
        filtered.append(record)
    return filtered, watermark, skipped


def update_state_entry(
    current: IngestionStateEntry | None,
    *,
    records: list[dict[str, Any]],
    finished_at_utc: str,
    coverage_complete: bool,
) -> IngestionStateEntry:
    watermark_dt = _parse_iso(current.high_watermark_posted_at if current else None)
    watermark_updated_dt = _parse_iso(current.high_watermark_updated_at if current else None)
    ids: list[str] = list(current.recent_source_job_ids if current else ())

    for record in records:
        posted_dt = _parse_iso(_text_or_none(record.get("posted_at")))
        if posted_dt is not None and (watermark_dt is None or posted_dt > watermark_dt):
            watermark_dt = posted_dt
        updated_dt = _parse_iso(_text_or_none(record.get("source_updated_at")))
        if updated_dt is not None and (
            watermark_updated_dt is None or updated_dt > watermark_updated_dt
        ):
            watermark_updated_dt = updated_dt
        source_job_id = _text_or_none(record.get("source_job_id"))
        if source_job_id:
            ids.append(source_job_id)

    if len(ids) > _MAX_RECENT_IDS:
        ids = ids[-_MAX_RECENT_IDS:]
    watermark = watermark_dt.isoformat() if watermark_dt is not None else (
        current.high_watermark_posted_at if current else None
    )
    watermark_updated = watermark_updated_dt.isoformat() if watermark_updated_dt is not None else (
        current.high_watermark_updated_at if current else None
    )
    return IngestionStateEntry(
        high_watermark_posted_at=watermark,
        high_watermark_updated_at=watermark_updated,
        last_success_at_utc=finished_at_utc,
        last_coverage_complete=bool(coverage_complete),
        recent_source_job_ids=tuple(ids),
    )


def _max_dt(first: datetime | None, second: datetime | None) -> datetime | None:
    if first is None:
        return second
    if second is None:
        return first
    return first if first >= second else second


def _parse_iso(value: str | None) -> datetime | None:
    if value in (None, ""):
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


def _text_or_none(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
