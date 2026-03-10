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


@dataclass(frozen=True, slots=True)
class IngestionStateEntry:
    high_watermark_posted_at: str | None = None
    last_success_at_utc: str | None = None
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
            last_success_at_utc=_string_or_none(payload.get("last_success_at_utc")),
            recent_source_job_ids=tuple(
                str(value) for value in ids if str(value).strip()
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "high_watermark_posted_at": self.high_watermark_posted_at,
            "last_success_at_utc": self.last_success_at_utc,
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
        return payload


def utc_now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _string_or_none(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
