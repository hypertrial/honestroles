from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

SCHEMA_VERSION = "1.0"

CheckStatus = Literal["pass", "fail"]


@dataclass(frozen=True, slots=True)
class NeonCheck:
    code: str
    status: CheckStatus
    message: str

    def to_dict(self) -> dict[str, str]:
        return {
            "code": self.code,
            "status": self.status,
            "message": self.message,
        }


@dataclass(frozen=True, slots=True)
class NeonMigrationResult:
    schema_version: str
    status: str
    schema: str
    database_url_env: str
    migrations_applied: tuple[str, ...]
    migrations_total: int
    duration_ms: int
    checks: tuple[NeonCheck, ...] = ()
    check_codes: tuple[str, ...] = ()

    def to_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "status": self.status,
            "schema": self.schema,
            "database_url_env": self.database_url_env,
            "migrations_applied": list(self.migrations_applied),
            "migrations_total": int(self.migrations_total),
            "duration_ms": int(self.duration_ms),
            "checks": [item.to_dict() for item in self.checks],
            "check_codes": list(self.check_codes),
        }


@dataclass(frozen=True, slots=True)
class NeonPublishResult:
    schema_version: str
    status: str
    schema: str
    database_url_env: str
    batch_id: str
    jobs_parquet: str
    index_dir: str
    sync_report: str | None
    require_quality_pass: bool
    quality_gate_status: str
    full_refresh: bool
    inserted_count: int
    updated_count: int
    deactivated_count: int
    facet_rows: int
    feature_rows: int
    active_jobs: int
    migration_version: str
    duration_ms: int
    checks: tuple[NeonCheck, ...] = ()
    check_codes: tuple[str, ...] = ()

    def to_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "status": self.status,
            "schema": self.schema,
            "database_url_env": self.database_url_env,
            "batch_id": self.batch_id,
            "jobs_parquet": self.jobs_parquet,
            "index_dir": self.index_dir,
            "sync_report": self.sync_report,
            "require_quality_pass": bool(self.require_quality_pass),
            "quality_gate_status": self.quality_gate_status,
            "full_refresh": bool(self.full_refresh),
            "inserted_count": int(self.inserted_count),
            "updated_count": int(self.updated_count),
            "deactivated_count": int(self.deactivated_count),
            "facet_rows": int(self.facet_rows),
            "feature_rows": int(self.feature_rows),
            "active_jobs": int(self.active_jobs),
            "migration_version": self.migration_version,
            "duration_ms": int(self.duration_ms),
            "checks": [item.to_dict() for item in self.checks],
            "check_codes": list(self.check_codes),
        }


@dataclass(frozen=True, slots=True)
class NeonVerifyResult:
    schema_version: str
    status: str
    schema: str
    database_url_env: str
    duration_ms: int
    checks: tuple[NeonCheck, ...]
    check_codes: tuple[str, ...]

    def to_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "status": self.status,
            "schema": self.schema,
            "database_url_env": self.database_url_env,
            "duration_ms": int(self.duration_ms),
            "checks": [item.to_dict() for item in self.checks],
            "check_codes": list(self.check_codes),
        }
