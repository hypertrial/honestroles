from __future__ import annotations

from datetime import UTC, datetime
import json
from pathlib import Path
import re
from typing import Any, Callable, cast

from honestroles.errors import ConfigValidationError, HonestRolesError
from honestroles.ingest.dedup import deduplicate_records
from honestroles.ingest.http import fetch_json
from honestroles.ingest.models import (
    INGEST_SCHEMA_VERSION,
    IngestionReport,
    IngestionResult,
    IngestionSource,
    IngestionStateEntry,
    SUPPORTED_INGEST_SOURCES,
)
from honestroles.ingest.normalize import normalize_records, normalized_dataframe
from honestroles.ingest.sources import (
    fetch_ashby_jobs,
    fetch_greenhouse_jobs,
    fetch_lever_jobs,
    fetch_workable_jobs,
)
from honestroles.ingest.state import (
    filter_incremental,
    load_state,
    state_key,
    update_state_entry,
    write_state,
)
from honestroles.io import write_parquet

_SOURCE_FETCHERS: dict[str, Callable[..., tuple[list[dict[str, Any]], int]]] = {
    "greenhouse": fetch_greenhouse_jobs,
    "lever": fetch_lever_jobs,
    "ashby": fetch_ashby_jobs,
    "workable": fetch_workable_jobs,
}
_SOURCE_REF_RE = re.compile(r"^[A-Za-z0-9._-]+$")


def sync_source(
    *,
    source: str,
    source_ref: str,
    output_parquet: str | Path | None = None,
    report_file: str | Path | None = None,
    state_file: str | Path = ".honestroles/ingest/state.json",
    write_raw: bool = False,
    max_pages: int = 25,
    max_jobs: int = 5000,
    full_refresh: bool = False,
    http_get_json: Callable[[str], Any] = fetch_json,
) -> IngestionResult:
    _validate_inputs(
        source=source,
        source_ref=source_ref,
        max_pages=max_pages,
        max_jobs=max_jobs,
    )
    source_name = cast(IngestionSource, source)
    output_path, report_path, raw_path = _resolve_paths(
        source=source_name,
        source_ref=source_ref,
        output_parquet=output_parquet,
        report_file=report_file,
        write_raw=write_raw,
    )
    started_at = datetime.now(UTC)
    request_count = 0
    fetched_count = 0
    normalized_count = 0
    dedup_dropped = 0
    state_entries: dict[str, IngestionStateEntry] = {}
    key = state_key(source_name, source_ref)
    current_entry = None
    high_before = None

    try:
        state_entries = load_state(state_file)
        current_entry = state_entries.get(key)
        high_before = current_entry.high_watermark_posted_at if current_entry else None

        fetcher = _SOURCE_FETCHERS[source_name]
        raw_records, request_count = fetcher(
            source_ref,
            max_pages=max_pages,
            max_jobs=max_jobs,
            http_get_json=http_get_json,
        )
        fetched_count = len(raw_records)

        if write_raw and raw_path is not None:
            _write_raw_jsonl(raw_path, raw_records)

        ingested_at = datetime.now(UTC).isoformat()
        normalized = normalize_records(
            raw_records,
            source=source_name,
            source_ref=source_ref,
            ingested_at_utc=ingested_at,
        )
        normalized_count = len(normalized)
        incremental_records, _ = filter_incremental(
            normalized,
            entry=current_entry,
            full_refresh=full_refresh,
        )
        deduped_records, dedup_dropped = deduplicate_records(incremental_records)
        frame = normalized_dataframe(deduped_records)
        write_parquet(frame, output_path)

        finished_at = datetime.now(UTC)
        entry = update_state_entry(
            current_entry,
            records=deduped_records,
            finished_at_utc=finished_at.isoformat(),
        )
        state_entries[key] = entry
        write_state(state_file, state_entries)

        output_paths = {
            "parquet": str(output_path),
            "report": str(report_path),
        }
        if raw_path is not None:
            output_paths["raw_jsonl"] = str(raw_path)
        report = IngestionReport(
            schema_version=INGEST_SCHEMA_VERSION,
            status="pass",
            source=source_name,
            source_ref=source_ref,
            started_at_utc=started_at.isoformat(),
            finished_at_utc=finished_at.isoformat(),
            duration_ms=_duration_ms(started_at, finished_at),
            request_count=request_count,
            fetched_count=fetched_count,
            normalized_count=normalized_count,
            dedup_dropped=dedup_dropped,
            high_watermark_before=high_before,
            high_watermark_after=entry.high_watermark_posted_at,
            output_paths=output_paths,
            error=None,
        )
        _write_report(report_path, report.to_dict())
        return IngestionResult(
            report=report,
            output_parquet=output_path,
            report_file=report_path,
            raw_file=raw_path,
            rows_written=frame.height,
            check_codes=(),
        )
    except (ConfigValidationError, HonestRolesError) as exc:
        _write_failure_report(
            report_path=report_path,
            source=source_name,
            source_ref=source_ref,
            started_at=started_at,
            request_count=request_count,
            fetched_count=fetched_count,
            normalized_count=normalized_count,
            dedup_dropped=dedup_dropped,
            high_before=high_before,
            error=exc,
        )
        raise
    except Exception as exc:
        wrapped = HonestRolesError(f"ingestion sync failed: {exc}")
        _write_failure_report(
            report_path=report_path,
            source=source_name,
            source_ref=source_ref,
            started_at=started_at,
            request_count=request_count,
            fetched_count=fetched_count,
            normalized_count=normalized_count,
            dedup_dropped=dedup_dropped,
            high_before=high_before,
            error=wrapped,
        )
        raise wrapped from exc


def _validate_inputs(*, source: str, source_ref: str, max_pages: int, max_jobs: int) -> None:
    if source not in SUPPORTED_INGEST_SOURCES:
        valid = ", ".join(SUPPORTED_INGEST_SOURCES)
        raise ConfigValidationError(f"unsupported source '{source}', expected one of: {valid}")
    if not source_ref.strip():
        raise ConfigValidationError("source-ref must be non-empty")
    if _SOURCE_REF_RE.fullmatch(source_ref) is None:
        raise ConfigValidationError(
            "source-ref may only contain letters, numbers, '.', '_' and '-'"
        )
    if max_pages < 1:
        raise ConfigValidationError("max-pages must be >= 1")
    if max_jobs < 1:
        raise ConfigValidationError("max-jobs must be >= 1")


def _resolve_paths(
    *,
    source: str,
    source_ref: str,
    output_parquet: str | Path | None,
    report_file: str | Path | None,
    write_raw: bool,
) -> tuple[Path, Path, Path | None]:
    slug = re.sub(r"[^A-Za-z0-9._-]+", "_", source_ref.strip())
    default_root = Path("dist/ingest") / source / slug
    output_path = (
        Path(output_parquet).expanduser().resolve()
        if output_parquet is not None
        else (default_root / "jobs.parquet").expanduser().resolve()
    )
    report_path = (
        Path(report_file).expanduser().resolve()
        if report_file is not None
        else (default_root / "sync_report.json").expanduser().resolve()
    )
    raw_path = ((default_root / "raw.jsonl").expanduser().resolve() if write_raw else None)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    if raw_path is not None:
        raw_path.parent.mkdir(parents=True, exist_ok=True)
    return output_path, report_path, raw_path


def _write_raw_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    lines = [json.dumps(record, sort_keys=True, default=str) for record in records]
    path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")


def _write_report(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _write_failure_report(
    *,
    report_path: Path,
    source: IngestionSource,
    source_ref: str,
    started_at: datetime,
    request_count: int,
    fetched_count: int,
    normalized_count: int,
    dedup_dropped: int,
    high_before: str | None,
    error: Exception,
) -> None:
    finished_at = datetime.now(UTC)
    report = IngestionReport(
        schema_version=INGEST_SCHEMA_VERSION,
        status="fail",
        source=source,
        source_ref=source_ref,
        started_at_utc=started_at.isoformat(),
        finished_at_utc=finished_at.isoformat(),
        duration_ms=_duration_ms(started_at, finished_at),
        request_count=request_count,
        fetched_count=fetched_count,
        normalized_count=normalized_count,
        dedup_dropped=dedup_dropped,
        high_watermark_before=high_before,
        high_watermark_after=high_before,
        output_paths={"report": str(report_path)},
        error={"type": error.__class__.__name__, "message": str(error)},
    )
    _write_report(report_path, report.to_dict())


def _duration_ms(started: datetime, finished: datetime) -> int:
    return int((finished - started).total_seconds() * 1000)
