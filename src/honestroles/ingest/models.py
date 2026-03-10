from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

IngestionSource = Literal["greenhouse", "lever", "ashby", "workable"]
SUPPORTED_INGEST_SOURCES: tuple[IngestionSource, ...] = (
    "greenhouse",
    "lever",
    "ashby",
    "workable",
)
INGEST_SCHEMA_VERSION = "1.0"
INGEST_STATE_SCHEMA_VERSION = "2.0"


@dataclass(frozen=True, slots=True)
class IngestionRequest:
    source: IngestionSource
    source_ref: str
    output_parquet: Path | None = None
    report_file: Path | None = None
    state_file: Path = Path(".honestroles/ingest/state.json")
    write_raw: bool = False
    max_pages: int = 25
    max_jobs: int = 5000
    full_refresh: bool = False
    timeout_seconds: float = 15.0
    max_retries: int = 3
    base_backoff_seconds: float = 0.25
    user_agent: str = "honestroles-ingest/2.0"


@dataclass(frozen=True, slots=True)
class IngestionStateEntry:
    high_watermark_posted_at: str | None = None
    high_watermark_updated_at: str | None = None
    last_success_at_utc: str | None = None
    last_coverage_complete: bool = False
    recent_source_job_ids: tuple[str, ...] = ()

    @classmethod
    def from_mapping(cls, payload: dict[str, Any] | None) -> "IngestionStateEntry":
        if payload is None:
            return cls()
        ids = payload.get("recent_source_job_ids", [])
        if not isinstance(ids, list):
            ids = []
        return cls(
            high_watermark_posted_at=_string_or_none(
                payload.get("high_watermark_posted_at")
            ),
            high_watermark_updated_at=_string_or_none(
                payload.get("high_watermark_updated_at")
            ),
            last_success_at_utc=_string_or_none(payload.get("last_success_at_utc")),
            last_coverage_complete=bool(payload.get("last_coverage_complete", False)),
            recent_source_job_ids=tuple(
                str(value) for value in ids if str(value).strip()
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "high_watermark_posted_at": self.high_watermark_posted_at,
            "high_watermark_updated_at": self.high_watermark_updated_at,
            "last_success_at_utc": self.last_success_at_utc,
            "last_coverage_complete": bool(self.last_coverage_complete),
            "recent_source_job_ids": list(self.recent_source_job_ids),
        }


@dataclass(frozen=True, slots=True)
class IngestionReport:
    schema_version: str
    status: str
    source: IngestionSource
    source_ref: str
    started_at_utc: str
    finished_at_utc: str
    duration_ms: int
    request_count: int
    fetched_count: int
    normalized_count: int
    dedup_dropped: int
    high_watermark_before: str | None
    high_watermark_after: str | None
    output_paths: dict[str, str]
    new_count: int = 0
    updated_count: int = 0
    unchanged_count: int = 0
    skipped_by_state: int = 0
    tombstoned_count: int = 0
    coverage_complete: bool = False
    retry_count: int = 0
    http_status_counts: dict[str, int] = field(default_factory=dict)
    error: dict[str, str] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "status": self.status,
            "source": self.source,
            "source_ref": self.source_ref,
            "started_at_utc": self.started_at_utc,
            "finished_at_utc": self.finished_at_utc,
            "duration_ms": self.duration_ms,
            "request_count": self.request_count,
            "fetched_count": self.fetched_count,
            "normalized_count": self.normalized_count,
            "dedup_dropped": self.dedup_dropped,
            "new_count": self.new_count,
            "updated_count": self.updated_count,
            "unchanged_count": self.unchanged_count,
            "skipped_by_state": self.skipped_by_state,
            "tombstoned_count": self.tombstoned_count,
            "coverage_complete": bool(self.coverage_complete),
            "retry_count": int(self.retry_count),
            "http_status_counts": {
                str(key): int(value)
                for key, value in sorted(self.http_status_counts.items())
            },
            "high_watermark_before": self.high_watermark_before,
            "high_watermark_after": self.high_watermark_after,
            "output_paths": dict(sorted(self.output_paths.items())),
            "error": self.error,
        }


@dataclass(frozen=True, slots=True)
class IngestionResult:
    report: IngestionReport
    output_parquet: Path
    report_file: Path
    raw_file: Path | None = None
    snapshot_file: Path | None = None
    catalog_file: Path | None = None
    state_file: Path | None = None
    rows_written: int = 0
    check_codes: tuple[str, ...] = field(default_factory=tuple)

    def to_payload(self) -> dict[str, Any]:
        payload = self.report.to_dict()
        payload["output_parquet"] = str(self.output_parquet)
        payload["report_file"] = str(self.report_file)
        payload["rows_written"] = int(self.rows_written)
        payload["check_codes"] = list(self.check_codes)
        if self.raw_file is not None:
            payload["raw_file"] = str(self.raw_file)
        if self.snapshot_file is not None:
            payload["snapshot_file"] = str(self.snapshot_file)
        if self.catalog_file is not None:
            payload["catalog_file"] = str(self.catalog_file)
        if self.state_file is not None:
            payload["state_file"] = str(self.state_file)
        return payload


@dataclass(frozen=True, slots=True)
class IngestionSourceConfig:
    source: IngestionSource
    source_ref: str
    enabled: bool = True
    output_parquet: Path | None = None
    report_file: Path | None = None
    state_file: Path | None = None
    write_raw: bool | None = None
    max_pages: int | None = None
    max_jobs: int | None = None
    full_refresh: bool | None = None
    timeout_seconds: float | None = None
    max_retries: int | None = None
    base_backoff_seconds: float | None = None
    user_agent: str | None = None


@dataclass(frozen=True, slots=True)
class IngestionDefaults:
    state_file: Path = Path(".honestroles/ingest/state.json")
    write_raw: bool = False
    max_pages: int = 25
    max_jobs: int = 5000
    full_refresh: bool = False
    timeout_seconds: float = 15.0
    max_retries: int = 3
    base_backoff_seconds: float = 0.25
    user_agent: str = "honestroles-ingest/2.0"


@dataclass(frozen=True, slots=True)
class IngestionManifest:
    path: Path
    defaults: IngestionDefaults
    sources: tuple[IngestionSourceConfig, ...]


@dataclass(frozen=True, slots=True)
class BatchIngestionResult:
    schema_version: str
    status: str
    started_at_utc: str
    finished_at_utc: str
    duration_ms: int
    total_sources: int
    pass_count: int
    fail_count: int
    total_rows_written: int
    total_fetched_count: int
    total_request_count: int
    sources: tuple[dict[str, Any], ...]
    report_file: Path | None = None
    check_codes: tuple[str, ...] = field(default_factory=tuple)

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "schema_version": self.schema_version,
            "status": self.status,
            "started_at_utc": self.started_at_utc,
            "finished_at_utc": self.finished_at_utc,
            "duration_ms": self.duration_ms,
            "total_sources": self.total_sources,
            "pass_count": self.pass_count,
            "fail_count": self.fail_count,
            "total_rows_written": self.total_rows_written,
            "total_fetched_count": self.total_fetched_count,
            "total_request_count": self.total_request_count,
            "sources": list(self.sources),
            "check_codes": list(self.check_codes),
        }
        if self.report_file is not None:
            payload["report_file"] = str(self.report_file)
        return payload


def utc_now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _string_or_none(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
