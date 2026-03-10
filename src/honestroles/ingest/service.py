from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
import json
from pathlib import Path
from time import perf_counter
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
    IngestionMergePolicy,
    IngestionReport,
    IngestionResult,
    IngestionSource,
    IngestionSourceConfig,
    IngestionStateEntry,
    IngestionValidationResult,
    SUPPORTED_INGEST_SOURCES,
)
from honestroles.ingest.normalize import normalize_records, normalized_dataframe
from honestroles.ingest.quality import (
    IngestQualityPolicy,
    IngestQualityResult,
    evaluate_ingest_quality,
    load_ingest_quality_policy,
)
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

_SOURCE_FETCHERS: dict[str, Callable[..., tuple[list[dict[str, Any]], int, tuple[str, ...]]]] = {
    "greenhouse": fetch_greenhouse_jobs,
    "lever": fetch_lever_jobs,
    "ashby": fetch_ashby_jobs,
    "workable": fetch_workable_jobs,
}
_SOURCE_REF_RE = re.compile(r"^[A-Za-z0-9._-]+$")
_VALID_MERGE_POLICIES: tuple[IngestionMergePolicy, ...] = (
    "updated_hash",
    "first_seen",
    "last_seen",
)


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
    pruned_inactive_count: int = 0


@dataclass(slots=True)
class _PreparedRecords:
    raw_records: list[dict[str, Any]]
    request_count: int
    fetched_count: int
    normalized_records: list[dict[str, Any]]
    normalized_count: int
    incremental_records: list[dict[str, Any]]
    deduped_records: list[dict[str, Any]]
    dedup_dropped: int
    skipped_by_state: int
    coverage_complete: bool
    warning_codes: tuple[str, ...]
    quality_result: IngestQualityResult
    quality_policy_source: str
    quality_policy_hash: str


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
    quality_policy_file: str | Path | None = None,
    strict_quality: bool = False,
    merge_policy: IngestionMergePolicy = "updated_hash",
    retain_snapshots: int = 30,
    prune_inactive_days: int = 90,
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
        merge_policy=merge_policy,
        retain_snapshots=retain_snapshots,
        prune_inactive_days=prune_inactive_days,
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
    stage_timings_ms: dict[str, int] = {}
    total_started = perf_counter()

    request_count = 0
    fetched_count = 0
    normalized_count = 0
    dedup_dropped = 0
    skipped_by_state = 0
    high_before: str | None = None
    written_state: Path | None = None
    quality_result = evaluate_ingest_quality(records=[], policy=IngestQualityPolicy())
    quality_policy_source = "builtin"
    quality_policy_hash: str | None = None
    warning_codes: set[str] = set()

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
        policy, quality_policy_source, quality_policy_hash = load_ingest_quality_policy(
            quality_policy_file
        )

        load_state_started = perf_counter()
        state_entries = load_state(state_file)
        stage_timings_ms["state_load"] = _elapsed_ms(load_state_started)
        key = state_key(source_name, source_ref)
        current_entry = state_entries.get(key)
        high_before = current_entry.high_watermark_posted_at if current_entry else None

        prepared = _prepare_records(
            source=source_name,
            source_ref=source_ref,
            max_pages=max_pages,
            max_jobs=max_jobs,
            fetch_fn=fetch_fn,
            write_raw=write_raw,
            raw_path=raw_path,
            current_entry=current_entry,
            full_refresh=full_refresh,
            policy=policy,
            quality_policy_source=quality_policy_source,
            quality_policy_hash=quality_policy_hash,
            stage_timings_ms=stage_timings_ms,
        )

        request_count = prepared.request_count
        fetched_count = prepared.fetched_count
        normalized_count = prepared.normalized_count
        dedup_dropped = prepared.dedup_dropped
        skipped_by_state = prepared.skipped_by_state
        quality_result = prepared.quality_result
        quality_policy_source = prepared.quality_policy_source
        quality_policy_hash = prepared.quality_policy_hash
        warning_codes.update(prepared.warning_codes)
        warning_codes.update(quality_result.check_codes)

        if strict_quality and quality_result.status != "pass":
            finished_at = datetime.now(UTC)
            stage_timings_ms["total"] = _elapsed_ms(total_started)
            output_paths: dict[str, str] = {"report": str(report_path)}
            if raw_path is not None and raw_path.exists():
                output_paths["raw_jsonl"] = str(raw_path)
            report = IngestionReport(
                schema_version=INGEST_SCHEMA_VERSION,
                status="fail",
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
                high_watermark_after=high_before,
                output_paths=output_paths,
                skipped_by_state=skipped_by_state,
                coverage_complete=prepared.coverage_complete,
                retry_count=telemetry.retry_count,
                http_status_counts=telemetry.http_status_counts,
                quality_status=quality_result.status,
                quality_summary=quality_result.summary,
                quality_check_codes=quality_result.check_codes,
                stage_timings_ms=stage_timings_ms,
                warnings=tuple(sorted(warning_codes)),
                merge_policy=merge_policy,
                quality_policy_source=quality_policy_source,
                quality_policy_hash=quality_policy_hash,
                error={
                    "type": "IngestQualityGateError",
                    "message": "ingestion quality gate failed in strict mode",
                },
            )
            _write_report(report_path, report.to_dict())
            check_codes = tuple(sorted(set(warning_codes).union(set(quality_result.check_codes))))
            return IngestionResult(
                report=report,
                output_parquet=output_path,
                report_file=report_path,
                raw_file=raw_path,
                snapshot_file=None,
                catalog_file=None,
                state_file=None,
                rows_written=0,
                check_codes=check_codes,
            )

        writes_started = perf_counter()
        snapshot_path = _snapshot_path_for(output_path, started_at)
        snapshot_frame = normalized_dataframe(prepared.deduped_records)
        write_parquet(snapshot_frame, snapshot_path)

        catalog_merge_started = perf_counter()
        catalog = _load_catalog(catalog_path)
        catalog, summary = _apply_catalog_updates(
            catalog=catalog,
            records=prepared.deduped_records,
            seen_at_utc=_utc_now_iso(),
            coverage_complete=prepared.coverage_complete,
            merge_policy=merge_policy,
            prune_inactive_days=prune_inactive_days,
        )
        _write_catalog(catalog_path, catalog)
        stage_timings_ms["catalog_merge"] = _elapsed_ms(catalog_merge_started)

        active_records = _active_records_from_catalog(catalog)
        latest_frame = normalized_dataframe(active_records)
        write_parquet(latest_frame, output_path)

        retained_snapshot_count, pruned_snapshot_count = _prune_snapshots(
            snapshot_path=snapshot_path,
            retain_snapshots=retain_snapshots,
        )

        finished_at = datetime.now(UTC)
        entry = update_state_entry(
            current_entry,
            records=prepared.deduped_records,
            finished_at_utc=finished_at.isoformat(),
            coverage_complete=prepared.coverage_complete,
        )
        state_entries[key] = entry
        written_state = write_state(state_file, state_entries)
        stage_timings_ms["writes"] = _elapsed_ms(writes_started)
        stage_timings_ms["total"] = _elapsed_ms(total_started)

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
            coverage_complete=prepared.coverage_complete,
            retry_count=telemetry.retry_count,
            http_status_counts=telemetry.http_status_counts,
            quality_status=quality_result.status,
            quality_summary=quality_result.summary,
            quality_check_codes=quality_result.check_codes,
            stage_timings_ms=stage_timings_ms,
            warnings=tuple(sorted(warning_codes)),
            merge_policy=merge_policy,
            retained_snapshot_count=retained_snapshot_count,
            pruned_snapshot_count=pruned_snapshot_count,
            pruned_inactive_count=summary.pruned_inactive_count,
            quality_policy_source=quality_policy_source,
            quality_policy_hash=quality_policy_hash,
            error=None,
        )
        _write_report(report_path, report.to_dict())
        check_codes = tuple(sorted(set(warning_codes).union(set(quality_result.check_codes))))
        return IngestionResult(
            report=report,
            output_parquet=output_path,
            report_file=report_path,
            raw_file=raw_path,
            snapshot_file=snapshot_path,
            catalog_file=catalog_path,
            state_file=written_state,
            rows_written=latest_frame.height,
            check_codes=check_codes,
        )
    except (ConfigValidationError, HonestRolesError) as exc:
        stage_timings_ms["total"] = _elapsed_ms(total_started)
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
            quality_result=quality_result,
            stage_timings_ms=stage_timings_ms,
            warning_codes=tuple(sorted(warning_codes)),
            merge_policy=merge_policy,
            quality_policy_source=quality_policy_source,
            quality_policy_hash=quality_policy_hash,
        )
        raise
    except Exception as exc:
        wrapped = HonestRolesError(f"ingestion sync failed: {exc}")
        stage_timings_ms["total"] = _elapsed_ms(total_started)
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
            quality_result=quality_result,
            stage_timings_ms=stage_timings_ms,
            warning_codes=tuple(sorted(warning_codes)),
            merge_policy=merge_policy,
            quality_policy_source=quality_policy_source,
            quality_policy_hash=quality_policy_hash,
        )
        raise wrapped from exc


def validate_ingestion_source(
    *,
    source: str,
    source_ref: str,
    report_file: str | Path | None = None,
    write_raw: bool = False,
    max_pages: int = 25,
    max_jobs: int = 5000,
    timeout_seconds: float = 15.0,
    max_retries: int = 3,
    base_backoff_seconds: float = 0.25,
    user_agent: str = "honestroles-ingest/2.0",
    quality_policy_file: str | Path | None = None,
    strict_quality: bool = False,
    http_get_json: Callable[[str], Any] = fetch_json,
) -> IngestionValidationResult:
    _validate_inputs(
        source=source,
        source_ref=source_ref,
        max_pages=max_pages,
        max_jobs=max_jobs,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        base_backoff_seconds=base_backoff_seconds,
        user_agent=user_agent,
        merge_policy="updated_hash",
        retain_snapshots=30,
        prune_inactive_days=90,
    )
    source_name = cast(IngestionSource, source)
    _, report_path, raw_path = _resolve_paths(
        source=source_name,
        source_ref=source_ref,
        output_parquet=None,
        report_file=report_file,
        write_raw=write_raw,
        report_name="validate_report.json",
    )
    started_at = datetime.now(UTC)
    stage_timings_ms: dict[str, int] = {}
    total_started = perf_counter()

    request_count = 0
    fetched_count = 0
    normalized_count = 0
    dedup_dropped = 0
    warning_codes: set[str] = set()
    quality_policy_source = "builtin"
    quality_policy_hash: str | None = None
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
        policy, quality_policy_source, loaded_policy_hash = load_ingest_quality_policy(
            quality_policy_file
        )
        quality_policy_hash = loaded_policy_hash
        prepared = _prepare_records(
            source=source_name,
            source_ref=source_ref,
            max_pages=max_pages,
            max_jobs=max_jobs,
            fetch_fn=fetch_fn,
            write_raw=write_raw,
            raw_path=raw_path,
            current_entry=None,
            full_refresh=True,
            policy=policy,
            quality_policy_source=quality_policy_source,
            quality_policy_hash=quality_policy_hash,
            stage_timings_ms=stage_timings_ms,
        )

        request_count = prepared.request_count
        fetched_count = prepared.fetched_count
        normalized_count = prepared.normalized_count
        dedup_dropped = prepared.dedup_dropped
        warning_codes.update(prepared.warning_codes)
        warning_codes.update(prepared.quality_result.check_codes)

        quality_status = prepared.quality_result.status
        status = "pass"
        if quality_status == "warn":
            status = "warn"
        if strict_quality and quality_status != "pass":
            status = "fail"

        finished_at = datetime.now(UTC)
        stage_timings_ms["total"] = _elapsed_ms(total_started)
        output_paths = {"report": str(report_path)}
        if raw_path is not None:
            output_paths["raw_jsonl"] = str(raw_path)

        report = IngestionReport(
            schema_version=INGEST_SCHEMA_VERSION,
            status=status,
            source=source_name,
            source_ref=source_ref,
            started_at_utc=started_at.isoformat(),
            finished_at_utc=finished_at.isoformat(),
            duration_ms=_duration_ms(started_at, finished_at),
            request_count=request_count,
            fetched_count=fetched_count,
            normalized_count=normalized_count,
            dedup_dropped=dedup_dropped,
            high_watermark_before=None,
            high_watermark_after=None,
            output_paths=output_paths,
            skipped_by_state=0,
            coverage_complete=prepared.coverage_complete,
            retry_count=telemetry.retry_count,
            http_status_counts=telemetry.http_status_counts,
            quality_status=quality_status,
            quality_summary=prepared.quality_result.summary,
            quality_check_codes=prepared.quality_result.check_codes,
            stage_timings_ms=stage_timings_ms,
            warnings=tuple(sorted(warning_codes)),
            merge_policy="updated_hash",
            quality_policy_source=quality_policy_source,
            quality_policy_hash=quality_policy_hash,
            error=None,
        )
        _write_report(report_path, report.to_dict())

        check_codes = tuple(
            sorted(set(warning_codes).union(set(prepared.quality_result.check_codes)))
        )
        return IngestionValidationResult(
            report=report,
            report_file=report_path,
            raw_file=raw_path,
            rows_evaluated=len(prepared.deduped_records),
            check_codes=check_codes,
        )
    except (ConfigValidationError, HonestRolesError) as exc:
        stage_timings_ms["total"] = _elapsed_ms(total_started)
        _write_failure_report(
            report_path=report_path,
            source=source_name,
            source_ref=source_ref,
            started_at=started_at,
            request_count=request_count,
            fetched_count=fetched_count,
            normalized_count=normalized_count,
            dedup_dropped=dedup_dropped,
            high_before=None,
            retry_count=telemetry.retry_count,
            http_status_counts=telemetry.http_status_counts,
            error=exc,
            stage_timings_ms=stage_timings_ms,
            warning_codes=tuple(sorted(warning_codes)),
            merge_policy="updated_hash",
            quality_policy_source=quality_policy_source,
            quality_policy_hash=quality_policy_hash,
        )
        raise
    except Exception as exc:
        wrapped = HonestRolesError(f"ingestion validate failed: {exc}")
        stage_timings_ms["total"] = _elapsed_ms(total_started)
        _write_failure_report(
            report_path=report_path,
            source=source_name,
            source_ref=source_ref,
            started_at=started_at,
            request_count=request_count,
            fetched_count=fetched_count,
            normalized_count=normalized_count,
            dedup_dropped=dedup_dropped,
            high_before=None,
            retry_count=telemetry.retry_count,
            http_status_counts=telemetry.http_status_counts,
            error=wrapped,
            stage_timings_ms=stage_timings_ms,
            warning_codes=tuple(sorted(warning_codes)),
            merge_policy="updated_hash",
            quality_policy_source=quality_policy_source,
            quality_policy_hash=quality_policy_hash,
        )
        raise wrapped from exc


def _prepare_records(
    *,
    source: IngestionSource,
    source_ref: str,
    max_pages: int,
    max_jobs: int,
    fetch_fn: Callable[[str], Any],
    write_raw: bool,
    raw_path: Path | None,
    current_entry: IngestionStateEntry | None,
    full_refresh: bool,
    policy: IngestQualityPolicy,
    quality_policy_source: str,
    quality_policy_hash: str,
    stage_timings_ms: dict[str, int],
) -> _PreparedRecords:
    fetch_started = perf_counter()
    raw_records, request_count, fetch_warning_codes = _fetch_source_records(
        source=source,
        source_ref=source_ref,
        max_pages=max_pages,
        max_jobs=max_jobs,
        fetch_fn=fetch_fn,
    )
    stage_timings_ms["fetch"] = _elapsed_ms(fetch_started)

    fetched_count = len(raw_records)
    if write_raw and raw_path is not None:
        raw_write_started = perf_counter()
        _write_raw_jsonl(raw_path, raw_records)
        stage_timings_ms["write_raw"] = _elapsed_ms(raw_write_started)

    normalize_started = perf_counter()
    ingested_at = _utc_now_iso()
    normalized = normalize_records(
        raw_records,
        source=source,
        source_ref=source_ref,
        ingested_at_utc=ingested_at,
    )
    stage_timings_ms["normalize"] = _elapsed_ms(normalize_started)
    normalized_count = len(normalized)

    incremental_started = perf_counter()
    incremental_records, _, skipped_by_state = filter_incremental(
        normalized,
        entry=current_entry,
        full_refresh=full_refresh,
    )
    stage_timings_ms["incremental_filter"] = _elapsed_ms(incremental_started)

    dedup_started = perf_counter()
    deduped_records, dedup_dropped = deduplicate_records(incremental_records)
    stage_timings_ms["dedup"] = _elapsed_ms(dedup_started)

    quality_started = perf_counter()
    quality_result = evaluate_ingest_quality(records=deduped_records, policy=policy)
    stage_timings_ms["quality"] = _elapsed_ms(quality_started)

    coverage_complete = _is_coverage_complete(
        request_count=request_count,
        max_pages=max_pages,
        fetched_count=fetched_count,
        max_jobs=max_jobs,
    )
    warning_codes = set(fetch_warning_codes)
    if not coverage_complete:
        warning_codes.add("INGEST_TRUNCATED")

    return _PreparedRecords(
        raw_records=raw_records,
        request_count=request_count,
        fetched_count=fetched_count,
        normalized_records=normalized,
        normalized_count=normalized_count,
        incremental_records=incremental_records,
        deduped_records=deduped_records,
        dedup_dropped=dedup_dropped,
        skipped_by_state=skipped_by_state,
        coverage_complete=coverage_complete,
        warning_codes=tuple(sorted(warning_codes)),
        quality_result=quality_result,
        quality_policy_source=quality_policy_source,
        quality_policy_hash=quality_policy_hash,
    )


def sync_sources_from_manifest(
    *,
    manifest_path: str | Path,
    report_file: str | Path | None = None,
    fail_fast: bool = False,
) -> BatchIngestionResult:
    manifest = load_ingest_manifest(manifest_path)
    started_at = datetime.now(UTC)
    total_started = perf_counter()

    source_payloads: list[dict[str, Any]] = []
    pass_count = 0
    fail_count = 0
    total_rows_written = 0
    total_fetched_count = 0
    total_request_count = 0
    quality_summary = {"pass": 0, "warn": 0, "fail": 0}
    warnings: set[str] = set()

    enabled_sources = [item for item in manifest.sources if item.enabled]
    for source_cfg in enabled_sources:
        params = _resolve_source_params(source_cfg, manifest.defaults)
        try:
            result = sync_source(**params)
            payload = result.to_payload()
            source_payloads.append(payload)
            pass_count += 1
            total_rows_written += int(payload.get("rows_written", 0))
            total_fetched_count += int(payload.get("fetched_count", 0))
            total_request_count += int(payload.get("request_count", 0))
            quality_status = str(payload.get("quality_status", "pass"))
            if quality_status in quality_summary:
                quality_summary[quality_status] += 1
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
                "quality_status": "fail",
                "quality_summary": {"pass": 0, "warn": 0, "fail": 1},
                "error": {"type": exc.__class__.__name__, "message": str(exc)},
            }
            source_payloads.append(fail_payload)
            quality_summary["fail"] += 1
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
        total_sources=len(enabled_sources),
        pass_count=pass_count,
        fail_count=fail_count,
        total_rows_written=total_rows_written,
        total_fetched_count=total_fetched_count,
        total_request_count=total_request_count,
        quality_summary=quality_summary,
        sources=tuple(source_payloads),
        stage_timings_ms={"total": _elapsed_ms(total_started)},
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
        "quality_policy_file": defaults.quality_policy_file
        if source_cfg.quality_policy_file is None
        else source_cfg.quality_policy_file,
        "strict_quality": defaults.strict_quality
        if source_cfg.strict_quality is None
        else source_cfg.strict_quality,
        "merge_policy": defaults.merge_policy
        if source_cfg.merge_policy is None
        else source_cfg.merge_policy,
        "retain_snapshots": defaults.retain_snapshots
        if source_cfg.retain_snapshots is None
        else source_cfg.retain_snapshots,
        "prune_inactive_days": defaults.prune_inactive_days
        if source_cfg.prune_inactive_days is None
        else source_cfg.prune_inactive_days,
    }


def _fetch_source_records(
    *,
    source: IngestionSource,
    source_ref: str,
    max_pages: int,
    max_jobs: int,
    fetch_fn: Callable[[str], Any],
) -> tuple[list[dict[str, Any]], int, tuple[str, ...]]:
    fetcher = _SOURCE_FETCHERS[source]
    raw = fetcher(
        source_ref,
        max_pages=max_pages,
        max_jobs=max_jobs,
        http_get_json=fetch_fn,
    )
    if isinstance(raw, tuple) and len(raw) == 3:
        records, request_count, warning_codes = raw
        return records, request_count, tuple(str(item) for item in warning_codes)
    if isinstance(raw, tuple) and len(raw) == 2:
        records, request_count = raw
        return records, request_count, ()
    raise HonestRolesError(f"invalid source fetcher result from '{source}'")


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
    merge_policy: str,
    retain_snapshots: int,
    prune_inactive_days: int,
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
    if merge_policy not in _VALID_MERGE_POLICIES:
        raise ConfigValidationError(
            f"merge-policy must be one of: {', '.join(_VALID_MERGE_POLICIES)}"
        )
    if retain_snapshots < 1:
        raise ConfigValidationError("retain-snapshots must be >= 1")
    if prune_inactive_days < 0:
        raise ConfigValidationError("prune-inactive-days must be >= 0")


def _resolve_paths(
    *,
    source: str,
    source_ref: str,
    output_parquet: str | Path | None,
    report_file: str | Path | None,
    write_raw: bool,
    report_name: str = "sync_report.json",
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
        else (default_root / report_name).expanduser().resolve()
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
    merge_policy: IngestionMergePolicy = "updated_hash",
    prune_inactive_days: int = 90,
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

        existing["last_seen_at_utc"] = seen_at_utc
        existing["is_active"] = True
        if str(existing.get("last_payload_hash") or "") == payload_hash:
            summary.unchanged_count += 1
            continue

        should_replace = _should_replace_record(
            existing=existing,
            incoming_payload_hash=payload_hash,
            incoming_posted_at=posted_at,
            incoming_updated_at=updated_at,
            merge_policy=merge_policy,
        )
        if should_replace:
            summary.updated_count += 1
            existing["last_payload_hash"] = payload_hash
            existing["latest_posted_at"] = posted_at
            existing["latest_updated_at"] = updated_at
            existing["latest_record_json"] = latest_record_json
        else:
            summary.unchanged_count += 1

    if coverage_complete:
        for key, existing in by_key.items():
            if key in seen_keys:
                continue
            if bool(existing.get("is_active", False)):
                summary.tombstoned_count += 1
            existing["is_active"] = False

    if prune_inactive_days >= 0:
        cutoff = _parse_iso(seen_at_utc)
        if cutoff is not None:
            cutoff = cutoff - timedelta(days=prune_inactive_days)
            for key in list(by_key):
                row = by_key[key]
                if bool(row.get("is_active", False)):
                    continue
                last_seen = _parse_iso(_text_or_none(row.get("last_seen_at_utc")))
                if last_seen is None:
                    continue
                if last_seen < cutoff:
                    summary.pruned_inactive_count += 1
                    by_key.pop(key, None)

    rows = [by_key[key] for key in sorted(by_key)]
    return rows, summary


def _should_replace_record(
    *,
    existing: Mapping[str, Any],
    incoming_payload_hash: str,
    incoming_posted_at: str | None,
    incoming_updated_at: str | None,
    merge_policy: IngestionMergePolicy,
) -> bool:
    if merge_policy == "first_seen":
        return False
    if merge_policy == "last_seen":
        return True

    existing_updated = _parse_iso(_text_or_none(existing.get("latest_updated_at")))
    incoming_updated = _parse_iso(incoming_updated_at)
    compare_updated = _compare_optional_datetimes(existing_updated, incoming_updated)
    if compare_updated < 0:
        return True
    if compare_updated > 0:
        return False

    existing_posted = _parse_iso(_text_or_none(existing.get("latest_posted_at")))
    incoming_posted = _parse_iso(incoming_posted_at)
    compare_posted = _compare_optional_datetimes(existing_posted, incoming_posted)
    if compare_posted < 0:
        return True
    if compare_posted > 0:
        return False

    existing_hash = str(existing.get("last_payload_hash") or "")
    return incoming_payload_hash > existing_hash


def _compare_optional_datetimes(current: datetime | None, incoming: datetime | None) -> int:
    # Returns -1 if incoming is newer, 1 if current is newer, 0 when equal/unknown.
    if current is None and incoming is None:
        return 0
    if current is None and incoming is not None:
        return -1
    if current is not None and incoming is None:
        return 1
    assert current is not None
    assert incoming is not None
    if incoming > current:
        return -1
    if incoming < current:
        return 1
    return 0


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


def _prune_snapshots(*, snapshot_path: Path, retain_snapshots: int) -> tuple[int, int]:
    snapshots_dir = snapshot_path.parent
    if not snapshots_dir.exists():
        return 0, 0
    snapshots = sorted(snapshots_dir.glob("*.parquet"), reverse=True)
    keep = snapshots[:retain_snapshots]
    prune = snapshots[retain_snapshots:]
    for path in prune:
        try:
            path.unlink()
        except OSError:
            continue
    return len(keep), len(prune)


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
    quality_result: IngestQualityResult | None = None,
    stage_timings_ms: dict[str, int] | None = None,
    warning_codes: tuple[str, ...] = (),
    merge_policy: IngestionMergePolicy = "updated_hash",
    quality_policy_source: str = "builtin",
    quality_policy_hash: str | None = None,
) -> None:
    finished_at = datetime.now(UTC)
    effective_quality = (
        quality_result
        if quality_result is not None
        else evaluate_ingest_quality(records=[], policy=IngestQualityPolicy())
    )
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
        quality_status=effective_quality.status,
        quality_summary=effective_quality.summary,
        quality_check_codes=effective_quality.check_codes,
        stage_timings_ms=stage_timings_ms or {},
        warnings=warning_codes,
        merge_policy=merge_policy,
        quality_policy_source=quality_policy_source,
        quality_policy_hash=quality_policy_hash,
        error={"type": error.__class__.__name__, "message": str(error)},
    )
    _write_report(report_path, report.to_dict())


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _elapsed_ms(started: float) -> int:
    return int((perf_counter() - started) * 1000)


def _duration_ms(started: datetime, finished: datetime) -> int:
    return int((finished - started).total_seconds() * 1000)
