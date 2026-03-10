from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
import json
from pathlib import Path
import re
from typing import Any, Callable, Mapping, cast
import uuid

import polars as pl

from honestroles.errors import ConfigValidationError, HonestRolesError
from honestroles.ingest.dedup import dedup_key, deduplicate_records
from honestroles.ingest.http import build_http_getter, fetch_json
from honestroles.ingest.manifest import load_ingest_manifest
from honestroles.ingest.models import (
    BatchIngestionResult,
    INGEST_SCHEMA_VERSION,
    IngestionDefaults,
    IngestionReport,
    IngestionResult,
    IngestionSource,
    IngestionSourceConfig,
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


@dataclass(slots=True)
class _HttpTelemetry:
    retry_count: int = 0
    http_status_counts: dict[str, int] = field(default_factory=dict)

    def observe(self, status_code: int | None, was_retry: bool) -> None:
        key = "network_error" if status_code is None else str(int(status_code))
        self.http_status_counts[key] = self.http_status_counts.get(key, 0) + 1
        if was_retry:
            self.retry_count += 1


@dataclass(slots=True)
class _CatalogSummary:
    new_count: int = 0
    updated_count: int = 0
    unchanged_count: int = 0
    tombstoned_count: int = 0


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
    timeout_seconds: float = 15.0,
    max_retries: int = 3,
    base_backoff_seconds: float = 0.25,
    user_agent: str = "honestroles-ingest/2.0",
    http_get_json: Callable[[str], Any] = fetch_json,
) -> IngestionResult:
    _validate_inputs(
        source=source,
        source_ref=source_ref,
        max_pages=max_pages,
        max_jobs=max_jobs,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        base_backoff_seconds=base_backoff_seconds,
        user_agent=user_agent,
    )
    source_name = cast(IngestionSource, source)
    output_path, report_path, raw_path = _resolve_paths(
        source=source_name,
        source_ref=source_ref,
        output_parquet=output_parquet,
        report_file=report_file,
        write_raw=write_raw,
    )
    catalog_path = _catalog_path_for(output_path)
    started_at = datetime.now(UTC)
    request_count = 0
    fetched_count = 0
    normalized_count = 0
    dedup_dropped = 0
    skipped_by_state = 0
    state_entries: dict[str, IngestionStateEntry] = {}
    key = state_key(source_name, source_ref)
    current_entry = None
    high_before = None
    warning_codes: list[str] = []
    telemetry = _HttpTelemetry()
    fetch_fn = (
        build_http_getter(
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            base_backoff_seconds=base_backoff_seconds,
            user_agent=user_agent,
            on_request=telemetry.observe,
        )
        if http_get_json is fetch_json
        else http_get_json
    )

    try:
        state_entries = load_state(state_file)
        current_entry = state_entries.get(key)
        high_before = current_entry.high_watermark_posted_at if current_entry else None

        fetcher = _SOURCE_FETCHERS[source_name]
        raw_records, request_count = fetcher(
            source_ref,
            max_pages=max_pages,
            max_jobs=max_jobs,
            http_get_json=fetch_fn,
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
        incremental_records, _, skipped_by_state = filter_incremental(
            normalized,
            entry=current_entry,
            full_refresh=full_refresh,
        )
        deduped_records, dedup_dropped = deduplicate_records(incremental_records)
        coverage_complete = _is_coverage_complete(
            request_count=request_count,
            max_pages=max_pages,
            fetched_count=fetched_count,
            max_jobs=max_jobs,
        )
        if not coverage_complete:
            warning_codes.append("INGEST_TRUNCATED")
        snapshot_path = _snapshot_path_for(output_path, started_at)
        snapshot_frame = normalized_dataframe(deduped_records)
        write_parquet(snapshot_frame, snapshot_path)

        catalog = _load_catalog(catalog_path)
        catalog, summary = _apply_catalog_updates(
            catalog=catalog,
            records=deduped_records,
            seen_at_utc=ingested_at,
            coverage_complete=coverage_complete,
        )
        _write_catalog(catalog_path, catalog)

        active_records = _active_records_from_catalog(catalog)
        latest_frame = normalized_dataframe(active_records)
        write_parquet(latest_frame, output_path)

        finished_at = datetime.now(UTC)
        entry = update_state_entry(
            current_entry,
            records=deduped_records,
            finished_at_utc=finished_at.isoformat(),
            coverage_complete=coverage_complete,
        )
        state_entries[key] = entry
        written_state = write_state(state_file, state_entries)

        output_paths = {
            "parquet": str(output_path),
            "report": str(report_path),
            "snapshot_parquet": str(snapshot_path),
            "catalog_parquet": str(catalog_path),
            "state_file": str(written_state),
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
            new_count=summary.new_count,
            updated_count=summary.updated_count,
            unchanged_count=summary.unchanged_count,
            skipped_by_state=skipped_by_state,
            tombstoned_count=summary.tombstoned_count,
            coverage_complete=coverage_complete,
            retry_count=telemetry.retry_count,
            http_status_counts=telemetry.http_status_counts,
            error=None,
        )
        _write_report(report_path, report.to_dict())
        return IngestionResult(
            report=report,
            output_parquet=output_path,
            report_file=report_path,
            raw_file=raw_path,
            snapshot_file=snapshot_path,
            catalog_file=catalog_path,
            state_file=written_state,
            rows_written=latest_frame.height,
            check_codes=tuple(sorted(set(warning_codes))),
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
            retry_count=telemetry.retry_count,
            http_status_counts=telemetry.http_status_counts,
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
            retry_count=telemetry.retry_count,
            http_status_counts=telemetry.http_status_counts,
            error=wrapped,
        )
        raise wrapped from exc


def sync_sources_from_manifest(
    *,
    manifest_path: str | Path,
    report_file: str | Path | None = None,
    fail_fast: bool = False,
) -> BatchIngestionResult:
    manifest = load_ingest_manifest(manifest_path)
    started_at = datetime.now(UTC)
    source_payloads: list[dict[str, Any]] = []
    pass_count = 0
    fail_count = 0
    total_rows_written = 0
    total_fetched_count = 0
    total_request_count = 0
    warnings: set[str] = set()

    for source_cfg in manifest.sources:
        if not source_cfg.enabled:
            continue
        params = _resolve_source_params(source_cfg, manifest.defaults)
        try:
            result = sync_source(**params)
            payload = result.to_payload()
            source_payloads.append(payload)
            pass_count += 1
            total_rows_written += int(payload.get("rows_written", 0))
            total_fetched_count += int(payload.get("fetched_count", 0))
            total_request_count += int(payload.get("request_count", 0))
            for code in payload.get("check_codes", []):
                warnings.add(str(code))
        except Exception as exc:
            fail_count += 1
            fail_payload = {
                "schema_version": INGEST_SCHEMA_VERSION,
                "status": "fail",
                "source": source_cfg.source,
                "source_ref": source_cfg.source_ref,
                "rows_written": 0,
                "fetched_count": 0,
                "request_count": 0,
                "error": {"type": exc.__class__.__name__, "message": str(exc)},
            }
            source_payloads.append(fail_payload)
            if fail_fast:
                break

    finished_at = datetime.now(UTC)
    status = "pass" if fail_count == 0 else "fail"
    batch_report_path = (
        Path(report_file).expanduser().resolve()
        if report_file is not None
        else (manifest.path.parent / "dist" / "ingest" / "sync_all_report.json").resolve()
    )
    batch_report_path.parent.mkdir(parents=True, exist_ok=True)
    result = BatchIngestionResult(
        schema_version=INGEST_SCHEMA_VERSION,
        status=status,
        started_at_utc=started_at.isoformat(),
        finished_at_utc=finished_at.isoformat(),
        duration_ms=_duration_ms(started_at, finished_at),
        total_sources=len([item for item in manifest.sources if item.enabled]),
        pass_count=pass_count,
        fail_count=fail_count,
        total_rows_written=total_rows_written,
        total_fetched_count=total_fetched_count,
        total_request_count=total_request_count,
        sources=tuple(source_payloads),
        report_file=batch_report_path,
        check_codes=tuple(sorted(warnings)),
    )
    _write_report(batch_report_path, result.to_payload())
    return result


def _resolve_source_params(
    source_cfg: IngestionSourceConfig,
    defaults: IngestionDefaults,
) -> dict[str, Any]:
    return {
        "source": source_cfg.source,
        "source_ref": source_cfg.source_ref,
        "output_parquet": source_cfg.output_parquet,
        "report_file": source_cfg.report_file,
        "state_file": source_cfg.state_file or defaults.state_file,
        "write_raw": defaults.write_raw if source_cfg.write_raw is None else source_cfg.write_raw,
        "max_pages": defaults.max_pages if source_cfg.max_pages is None else source_cfg.max_pages,
        "max_jobs": defaults.max_jobs if source_cfg.max_jobs is None else source_cfg.max_jobs,
        "full_refresh": defaults.full_refresh
        if source_cfg.full_refresh is None
        else source_cfg.full_refresh,
        "timeout_seconds": defaults.timeout_seconds
        if source_cfg.timeout_seconds is None
        else source_cfg.timeout_seconds,
        "max_retries": defaults.max_retries
        if source_cfg.max_retries is None
        else source_cfg.max_retries,
        "base_backoff_seconds": defaults.base_backoff_seconds
        if source_cfg.base_backoff_seconds is None
        else source_cfg.base_backoff_seconds,
        "user_agent": defaults.user_agent if source_cfg.user_agent is None else source_cfg.user_agent,
    }


def _validate_inputs(
    *,
    source: str,
    source_ref: str,
    max_pages: int,
    max_jobs: int,
    timeout_seconds: float,
    max_retries: int,
    base_backoff_seconds: float,
    user_agent: str,
) -> None:
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
    if timeout_seconds <= 0:
        raise ConfigValidationError("timeout-seconds must be > 0")
    if max_retries < 0:
        raise ConfigValidationError("max-retries must be >= 0")
    if base_backoff_seconds < 0:
        raise ConfigValidationError("base-backoff-seconds must be >= 0")
    if not user_agent.strip():
        raise ConfigValidationError("user-agent must be non-empty")


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


def _catalog_path_for(output_path: Path) -> Path:
    return output_path.with_name("catalog.parquet")


def _snapshot_path_for(output_path: Path, started_at: datetime) -> Path:
    run_id = uuid.uuid4().hex[:12]
    stamp = started_at.strftime("%Y%m%dT%H%M%S")
    snapshots_dir = output_path.parent / "snapshots"
    snapshots_dir.mkdir(parents=True, exist_ok=True)
    return (snapshots_dir / f"{stamp}-{run_id}.parquet").resolve()


def _load_catalog(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    frame = pl.read_parquet(path)
    return frame.to_dicts()


def _write_catalog(path: Path, rows: list[dict[str, Any]]) -> None:
    frame = pl.DataFrame(rows, infer_schema_length=None) if rows else _empty_catalog_frame()
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(path)


def _empty_catalog_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "stable_key": pl.Series([], dtype=pl.String),
            "first_seen_at_utc": pl.Series([], dtype=pl.String),
            "last_seen_at_utc": pl.Series([], dtype=pl.String),
            "is_active": pl.Series([], dtype=pl.Boolean),
            "last_payload_hash": pl.Series([], dtype=pl.String),
            "latest_posted_at": pl.Series([], dtype=pl.String),
            "latest_updated_at": pl.Series([], dtype=pl.String),
            "latest_record_json": pl.Series([], dtype=pl.String),
        }
    )


def _apply_catalog_updates(
    *,
    catalog: list[dict[str, Any]],
    records: list[dict[str, Any]],
    seen_at_utc: str,
    coverage_complete: bool,
) -> tuple[list[dict[str, Any]], _CatalogSummary]:
    summary = _CatalogSummary()
    by_key: dict[str, dict[str, Any]] = {}
    for row in catalog:
        key = str(row.get("stable_key", "")).strip()
        if key:
            by_key[key] = dict(row)

    seen_keys: set[str] = set()
    for record in records:
        key = dedup_key(record)
        seen_keys.add(key)
        payload_hash = str(record.get("source_payload_hash") or "")
        posted_at = _text_or_none(record.get("posted_at"))
        updated_at = _text_or_none(record.get("source_updated_at"))
        latest_record_json = json.dumps(record, sort_keys=True, default=str)
        existing = by_key.get(key)
        if existing is None:
            summary.new_count += 1
            by_key[key] = {
                "stable_key": key,
                "first_seen_at_utc": seen_at_utc,
                "last_seen_at_utc": seen_at_utc,
                "is_active": True,
                "last_payload_hash": payload_hash,
                "latest_posted_at": posted_at,
                "latest_updated_at": updated_at,
                "latest_record_json": latest_record_json,
            }
            continue
        if str(existing.get("last_payload_hash") or "") == payload_hash:
            summary.unchanged_count += 1
        else:
            summary.updated_count += 1
        existing["last_seen_at_utc"] = seen_at_utc
        existing["is_active"] = True
        existing["last_payload_hash"] = payload_hash
        existing["latest_posted_at"] = posted_at
        existing["latest_updated_at"] = updated_at
        existing["latest_record_json"] = latest_record_json

    if coverage_complete:
        for key, existing in by_key.items():
            if key in seen_keys:
                continue
            if bool(existing.get("is_active", False)):
                summary.tombstoned_count += 1
            existing["is_active"] = False

    rows = [by_key[key] for key in sorted(by_key)]
    return rows, summary


def _active_records_from_catalog(catalog: list[dict[str, Any]]) -> list[dict[str, Any]]:
    active: list[dict[str, Any]] = []
    for row in catalog:
        if not bool(row.get("is_active", False)):
            continue
        payload = _text_or_none(row.get("latest_record_json"))
        if payload is None:
            continue
        try:
            decoded = json.loads(payload)
        except json.JSONDecodeError:
            continue
        if isinstance(decoded, dict):
            active.append(decoded)
    active.sort(key=lambda item: dedup_key(item))
    return active


def _is_coverage_complete(
    *,
    request_count: int,
    max_pages: int,
    fetched_count: int,
    max_jobs: int,
) -> bool:
    if request_count >= max_pages:
        return False
    if fetched_count >= max_jobs:
        return False
    return True


def _text_or_none(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _write_raw_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    lines = [json.dumps(record, sort_keys=True, default=str) for record in records]
    path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")


def _write_report(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_text(json.dumps(dict(payload), indent=2, sort_keys=True), encoding="utf-8")


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
    retry_count: int,
    http_status_counts: dict[str, int],
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
        retry_count=retry_count,
        http_status_counts=http_status_counts,
        error={"type": error.__class__.__name__, "message": str(error)},
    )
    _write_report(report_path, report.to_dict())


def _duration_ms(started: datetime, finished: datetime) -> int:
    return int((finished - started).total_seconds() * 1000)
