from __future__ import annotations

from collections import Counter
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import hashlib
import json
import os
from pathlib import Path
import re
from time import perf_counter
from typing import Any, Iterator, Protocol

from honestroles.errors import ConfigValidationError, HonestRolesError
from honestroles.io import read_parquet
from honestroles.recommend.index import load_index
from honestroles.recommend.scoring import normalize_job_record, tokenize_text

from .models import (
    SCHEMA_VERSION,
    NeonCheck,
    NeonMigrationResult,
    NeonPublishResult,
    NeonVerifyResult,
)
from .sql import REQUIRED_FUNCTIONS, REQUIRED_TABLES, migrations_for_schema

DEFAULT_DB_ENV = "NEON_DATABASE_URL"
DEFAULT_SCHEMA = "honestroles_api"
_PROFILE_TTL_DAYS = 30


class NeonRuntimeError(HonestRolesError):
    """Raised for runtime DB execution failures."""


class _CursorProtocol(Protocol):
    rowcount: int

    def execute(self, query: str, params: Any | None = None) -> Any: ...

    def executemany(self, query: str, params_seq: list[tuple[Any, ...]]) -> Any: ...

    def fetchone(self) -> tuple[Any, ...] | None: ...

    def fetchall(self) -> list[tuple[Any, ...]]: ...


@dataclass(frozen=True, slots=True)
class _PreparedSyncPayload:
    jobs_rows: list[tuple[Any, ...]]
    feature_rows: list[tuple[Any, ...]]
    facets_rows: list[tuple[Any, ...]]
    jobs_parquet_hash: str
    index_manifest_hash: str | None
    policy_hash: str | None
    active_jobs: int


@dataclass(frozen=True, slots=True)
class _FeedbackSyncPayload:
    events_rows: list[tuple[Any, ...]]
    weights_rows: list[tuple[Any, ...]]


def migrate_neondb(
    *,
    database_url_env: str = DEFAULT_DB_ENV,
    schema: str = DEFAULT_SCHEMA,
) -> NeonMigrationResult:
    started = perf_counter()
    validated_schema = _validate_schema(schema)
    env_name, database_url = _resolve_database_url(database_url_env)

    applied: list[str] = []
    checks: list[NeonCheck] = []
    try:
        with _db_cursor(database_url) as cursor:
            applied = _apply_migrations(cursor, validated_schema)
            checks.append(
                NeonCheck(
                    code="NEON_MIGRATIONS_APPLIED",
                    status="pass",
                    message=f"applied={len(applied)}",
                )
            )
    except ConfigValidationError:
        raise
    except Exception as exc:
        raise NeonRuntimeError(f"neondb migrate failed: {exc}") from exc

    duration_ms = int((perf_counter() - started) * 1000)
    migrations_total = len(migrations_for_schema(validated_schema))
    return NeonMigrationResult(
        schema_version=SCHEMA_VERSION,
        status="pass",
        schema=validated_schema,
        database_url_env=env_name,
        migrations_applied=tuple(applied),
        migrations_total=migrations_total,
        duration_ms=duration_ms,
        checks=tuple(checks),
        check_codes=tuple(sorted({item.code for item in checks})),
    )


def verify_neondb_contract(
    *,
    database_url_env: str = DEFAULT_DB_ENV,
    schema: str = DEFAULT_SCHEMA,
) -> NeonVerifyResult:
    started = perf_counter()
    validated_schema = _validate_schema(schema)
    env_name, database_url = _resolve_database_url(database_url_env)

    checks: list[NeonCheck] = []
    try:
        with _db_cursor(database_url) as cursor:
            existing_tables = _fetch_table_names(cursor, validated_schema)
            for table_name in REQUIRED_TABLES:
                status = "pass" if table_name in existing_tables else "fail"
                checks.append(
                    NeonCheck(
                        code=f"NEON_TABLE_{table_name.upper()}",
                        status=status,
                        message=(
                            f"{validated_schema}.{table_name} exists"
                            if status == "pass"
                            else f"missing {validated_schema}.{table_name}"
                        ),
                    )
                )

            existing_functions = _fetch_function_names(cursor, validated_schema)
            for function_name in REQUIRED_FUNCTIONS:
                status = "pass" if function_name in existing_functions else "fail"
                checks.append(
                    NeonCheck(
                        code=f"NEON_FUNCTION_{function_name.upper()}",
                        status=status,
                        message=(
                            f"{validated_schema}.{function_name} exists"
                            if status == "pass"
                            else f"missing {validated_schema}.{function_name}"
                        ),
                    )
                )

            latest = migrations_for_schema(validated_schema)[-1].version
            latest_applied = ""
            if "migration_history" in existing_tables:
                cursor.execute(
                    f"SELECT version FROM {validated_schema}.migration_history ORDER BY applied_at DESC LIMIT 1"
                )
                row = cursor.fetchone()
                latest_applied = str(row[0]) if row is not None else ""
            checks.append(
                NeonCheck(
                    code="NEON_MIGRATION_LATEST",
                    status="pass" if latest_applied == latest else "fail",
                    message=(
                        f"latest migration={latest_applied or 'none'}"
                        if latest_applied == latest
                        else f"expected latest migration={latest} got={latest_applied or 'none'}"
                    ),
                )
            )
    except ConfigValidationError:
        raise
    except Exception as exc:
        raise NeonRuntimeError(f"neondb verify failed: {exc}") from exc

    status = "pass" if all(item.status == "pass" for item in checks) else "fail"
    duration_ms = int((perf_counter() - started) * 1000)
    check_codes = tuple(sorted({item.code for item in checks if item.status == "fail"}))
    return NeonVerifyResult(
        schema_version=SCHEMA_VERSION,
        status=status,
        schema=validated_schema,
        database_url_env=env_name,
        duration_ms=duration_ms,
        checks=tuple(checks),
        check_codes=check_codes,
    )


def publish_neondb_sync(
    *,
    jobs_parquet: str | Path,
    index_dir: str | Path,
    database_url_env: str = DEFAULT_DB_ENV,
    schema: str = DEFAULT_SCHEMA,
    sync_report: str | Path | None = None,
    require_quality_pass: bool = True,
    full_refresh: bool = False,
    batch_id: str | None = None,
) -> NeonPublishResult:
    started = perf_counter()
    validated_schema = _validate_schema(schema)
    env_name, database_url = _resolve_database_url(database_url_env)

    jobs_path = Path(jobs_parquet).expanduser().resolve()
    if not jobs_path.exists() or not jobs_path.is_file():
        raise ConfigValidationError(f"jobs parquet does not exist: '{jobs_path}'")

    resolved_index_dir = Path(index_dir).expanduser().resolve()
    if not resolved_index_dir.exists() or not resolved_index_dir.is_dir():
        raise ConfigValidationError(f"index directory does not exist: '{resolved_index_dir}'")

    quality_path = _resolve_sync_report_path(sync_report=sync_report, jobs_path=jobs_path)
    quality_gate_status = _evaluate_quality_gate(
        sync_report_path=quality_path,
        require_quality_pass=require_quality_pass,
    )

    prepared = _prepare_sync_payload(
        jobs_parquet=jobs_path,
        index_dir=resolved_index_dir,
    )
    feedback_payload = _collect_feedback_sync_payload()
    resolved_batch_id = _resolve_batch_id(batch_id)

    inserted_count = 0
    updated_count = 0
    deactivated_count = 0
    facet_rows = len(prepared.facets_rows)
    feature_rows = len(prepared.feature_rows)

    try:
        with _db_cursor(database_url) as cursor:
            _apply_migrations(cursor, validated_schema)
            _insert_publish_batch_started(
                cursor,
                schema=validated_schema,
                batch_id=resolved_batch_id,
                require_quality_pass=require_quality_pass,
                quality_gate_status=quality_gate_status,
                full_refresh=full_refresh,
                jobs_parquet_hash=prepared.jobs_parquet_hash,
                index_manifest_hash=prepared.index_manifest_hash,
                policy_hash=prepared.policy_hash,
            )

            inserted_count, updated_count, deactivated_count = _sync_jobs_and_features(
                cursor,
                schema=validated_schema,
                prepared=prepared,
                full_refresh=full_refresh,
            )
            _sync_facets(cursor, schema=validated_schema, facets_rows=prepared.facets_rows)
            _sync_feedback(cursor, schema=validated_schema, payload=feedback_payload)
            _complete_publish_batch(
                cursor,
                schema=validated_schema,
                batch_id=resolved_batch_id,
                status="pass",
                inserted_count=inserted_count,
                updated_count=updated_count,
                deactivated_count=deactivated_count,
                active_jobs=prepared.active_jobs,
                error_message=None,
            )
    except ConfigValidationError:
        raise
    except NeonRuntimeError:
        raise
    except Exception as exc:
        raise NeonRuntimeError(f"neondb sync failed: {exc}") from exc

    duration_ms = int((perf_counter() - started) * 1000)
    checks = (
        NeonCheck(code="NEON_QUALITY_GATE", status="pass", message=quality_gate_status),
        NeonCheck(code="NEON_SYNC_COMPLETED", status="pass", message=f"batch_id={resolved_batch_id}"),
    )
    return NeonPublishResult(
        schema_version=SCHEMA_VERSION,
        status="pass",
        schema=validated_schema,
        database_url_env=env_name,
        batch_id=resolved_batch_id,
        jobs_parquet=str(jobs_path),
        index_dir=str(resolved_index_dir),
        sync_report=str(quality_path) if quality_path is not None else None,
        require_quality_pass=require_quality_pass,
        quality_gate_status=quality_gate_status,
        full_refresh=full_refresh,
        inserted_count=inserted_count,
        updated_count=updated_count,
        deactivated_count=deactivated_count,
        facet_rows=facet_rows,
        feature_rows=feature_rows,
        active_jobs=prepared.active_jobs,
        migration_version=migrations_for_schema(validated_schema)[-1].version,
        duration_ms=duration_ms,
        checks=checks,
        check_codes=tuple(sorted({item.code for item in checks})),
    )


def upsert_profile_cache_neondb(
    *,
    database_url_env: str = DEFAULT_DB_ENV,
    schema: str = DEFAULT_SCHEMA,
    profile_id: str,
    profile_payload: dict[str, Any],
    source: str = "api",
    ttl_days: int = _PROFILE_TTL_DAYS,
) -> dict[str, Any]:
    validated_schema = _validate_schema(schema)
    env_name, database_url = _resolve_database_url(database_url_env)
    normalized_profile_id = str(profile_id).strip().lower()
    if not normalized_profile_id:
        raise ConfigValidationError("profile_id must be non-empty")
    if not isinstance(profile_payload, dict):
        raise ConfigValidationError("profile payload must be an object")
    if ttl_days < 1:
        raise ConfigValidationError("ttl_days must be >= 1")

    expires_at = datetime.now(UTC) + timedelta(days=ttl_days)
    try:
        with _db_cursor(database_url) as cursor:
            _apply_migrations(cursor, validated_schema)
            cursor.execute(
                f"""
                INSERT INTO {validated_schema}.profile_cache(profile_id, profile, source, expires_at, updated_at)
                VALUES (%s, %s::jsonb, %s, %s, NOW())
                ON CONFLICT (profile_id) DO UPDATE SET
                    profile = EXCLUDED.profile,
                    source = EXCLUDED.source,
                    expires_at = EXCLUDED.expires_at,
                    updated_at = NOW()
                """,
                (
                    normalized_profile_id,
                    json.dumps(profile_payload, sort_keys=True),
                    str(source).strip() or "api",
                    expires_at,
                ),
            )
    except ConfigValidationError:
        raise
    except Exception as exc:
        raise NeonRuntimeError(f"neondb profile cache upsert failed: {exc}") from exc

    return {
        "schema_version": SCHEMA_VERSION,
        "status": "pass",
        "schema": validated_schema,
        "database_url_env": env_name,
        "profile_id": normalized_profile_id,
        "expires_at_utc": expires_at.replace(microsecond=0).isoformat(),
        "check_codes": [],
    }


def _resolve_database_url(database_url_env: str) -> tuple[str, str]:
    env_name = str(database_url_env).strip()
    if not env_name:
        raise ConfigValidationError("database-url-env must be non-empty")
    value = os.getenv(env_name)
    if value is None or not str(value).strip():
        raise ConfigValidationError(
            f"missing database URL in environment variable '{env_name}'"
        )
    return env_name, str(value).strip()


def _validate_schema(schema: str) -> str:
    normalized = str(schema).strip()
    if not normalized:
        raise ConfigValidationError("schema must be non-empty")
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", normalized):
        raise ConfigValidationError(
            "schema must match ^[A-Za-z_][A-Za-z0-9_]*$"
        )
    return normalized


@contextmanager
def _db_cursor(database_url: str) -> Iterator[_CursorProtocol]:
    try:
        import psycopg
    except ImportError as exc:
        raise ConfigValidationError(
            "psycopg is required for neondb publish commands; install honestroles[db]"
        ) from exc

    conn = None
    cursor = None
    try:
        conn = psycopg.connect(database_url)
        cursor = conn.cursor()
        yield cursor
        conn.commit()
    except ConfigValidationError:
        if conn is not None:
            conn.rollback()
        raise
    except Exception:
        if conn is not None:
            conn.rollback()
        raise
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


def _apply_migrations(cursor: _CursorProtocol, schema: str) -> list[str]:
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.migration_history (
            version TEXT PRIMARY KEY,
            checksum TEXT NOT NULL,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )

    cursor.execute(f"SELECT version, checksum FROM {schema}.migration_history")
    existing_rows = cursor.fetchall()
    existing = {str(row[0]): str(row[1]) for row in existing_rows}

    applied: list[str] = []
    for migration in migrations_for_schema(schema):
        found = existing.get(migration.version)
        if found is not None:
            if found != migration.checksum:
                raise ConfigValidationError(
                    f"migration checksum mismatch for {migration.version}"
                )
            continue
        cursor.execute(migration.sql)
        cursor.execute(
            f"INSERT INTO {schema}.migration_history(version, checksum) VALUES (%s, %s)",
            (migration.version, migration.checksum),
        )
        applied.append(migration.version)
    return applied


def _fetch_table_names(cursor: _CursorProtocol, schema: str) -> set[str]:
    cursor.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s
        """,
        (schema,),
    )
    return {str(row[0]) for row in cursor.fetchall()}


def _fetch_function_names(cursor: _CursorProtocol, schema: str) -> set[str]:
    cursor.execute(
        """
        SELECT p.proname
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = %s
        """,
        (schema,),
    )
    return {str(row[0]) for row in cursor.fetchall()}


def _resolve_sync_report_path(
    *,
    sync_report: str | Path | None,
    jobs_path: Path,
) -> Path | None:
    if sync_report in (None, ""):
        candidate = jobs_path.with_name("sync_report.json")
        return candidate if candidate.exists() else None
    return Path(sync_report).expanduser().resolve()


def _evaluate_quality_gate(
    *,
    sync_report_path: Path | None,
    require_quality_pass: bool,
) -> str:
    if not require_quality_pass:
        return "skipped"
    if sync_report_path is None:
        raise ConfigValidationError(
            "require-quality-pass is enabled but no sync report was provided or discovered"
        )
    if not sync_report_path.exists() or not sync_report_path.is_file():
        raise ConfigValidationError(f"sync report does not exist: '{sync_report_path}'")
    try:
        payload = json.loads(sync_report_path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ConfigValidationError(f"cannot read sync report '{sync_report_path}': {exc}") from exc
    except json.JSONDecodeError as exc:
        raise ConfigValidationError(f"invalid sync report JSON '{sync_report_path}': {exc}") from exc

    status = str(payload.get("quality_status") or payload.get("status") or "").strip().lower()
    if status != "pass":
        raise NeonRuntimeError(
            f"quality gate failed: sync report status is '{status or 'unknown'}'"
        )
    return "pass"


def _prepare_sync_payload(
    *,
    jobs_parquet: Path,
    index_dir: Path,
) -> _PreparedSyncPayload:
    frame = read_parquet(jobs_parquet)
    rows: list[dict[str, Any]] = []
    for item in frame.to_dicts():
        raw = dict(item)
        normalized = normalize_job_record(raw)
        normalized["source_job_id"] = (
            _coerce_text(raw.get("source_job_id")) or _coerce_text(normalized.get("id"))
        )
        rows.append(normalized)
    rows_sorted = sorted(rows, key=lambda item: str(item.get("job_id", "")))

    manifest, _jobs_json = load_index(index_dir)
    index_manifest_hash = _hash_file(index_dir / "manifest.json")
    policy_hash = _coerce_text(manifest.get("policy_hash"))

    jobs_rows: list[tuple[Any, ...]] = []
    feature_rows: list[tuple[Any, ...]] = []
    for row in rows_sorted:
        job_id = _coerce_text(row.get("job_id"))
        posted_at = _parse_timestamp(row.get("posted_at"))
        updated_at = _parse_timestamp(row.get("source_updated_at"))
        skills = _to_text_array(row.get("skills"))
        jobs_rows.append(
            (
                job_id,
                _coerce_text(row.get("source_job_id")) or _coerce_text(row.get("id")),
                _coerce_text(row.get("id")),
                _coerce_text(row.get("title")),
                _coerce_text(row.get("company")),
                _coerce_text(row.get("location")),
                _coerce_text(row.get("work_mode")) or "unknown",
                _coerce_text(row.get("seniority")) or "mid",
                _coerce_text(row.get("employment_type")) or "unknown",
                _coerce_bool(row.get("remote")),
                _coerce_text(row.get("description_text")),
                _coerce_text(row.get("description_html")),
                skills,
                _coerce_float(row.get("salary_min")),
                _coerce_float(row.get("salary_max")),
                _coerce_text(row.get("salary_currency")),
                _coerce_text(row.get("salary_interval")),
                _coerce_text(row.get("apply_url")),
                posted_at,
                updated_at,
                _coerce_text(row.get("source")),
                _coerce_text(row.get("source_ref")),
                _coerce_text(row.get("job_url")),
            )
        )

        feature_rows.append(
            (
                job_id,
                sorted(tokenize_text(_coerce_text(row.get("title")) or "")),
                _skill_tokens(row),
                _coerce_text(row.get("location")),
                _coerce_text(row.get("work_mode")) or "unknown",
                _coerce_text(row.get("seniority")) or "mid",
                _coerce_text(row.get("employment_type")) or "unknown",
                _coerce_float(row.get("salary_min")),
                _coerce_float(row.get("salary_max")),
                _quality_flags(row),
                _visa_no_sponsorship(row),
                posted_at,
                updated_at,
            )
        )

    facets_rows = _build_facets(rows_sorted)
    return _PreparedSyncPayload(
        jobs_rows=jobs_rows,
        feature_rows=feature_rows,
        facets_rows=facets_rows,
        jobs_parquet_hash=_hash_file(jobs_parquet),
        index_manifest_hash=index_manifest_hash,
        policy_hash=policy_hash,
        active_jobs=len(rows_sorted),
    )


def _collect_feedback_sync_payload() -> _FeedbackSyncPayload:
    root = (Path.cwd() / ".honestroles" / "recommend" / "feedback").resolve()
    events_path = root / "events.jsonl"
    weights_dir = root / "weights"

    events_rows: list[tuple[Any, ...]] = []
    if events_path.exists() and events_path.is_file():
        for line in events_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(payload, dict):
                continue
            profile_id = _coerce_text(payload.get("profile_id"))
            job_id = _coerce_text(payload.get("job_id"))
            event = _coerce_text(payload.get("event"))
            event_hash = _coerce_text(payload.get("event_hash"))
            if not profile_id or not job_id or not event or not event_hash:
                continue
            meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
            recorded = _parse_timestamp(payload.get("recorded_at_utc"))
            events_rows.append(
                (
                    profile_id,
                    job_id,
                    event,
                    json.dumps(meta, sort_keys=True),
                    event_hash,
                    recorded,
                )
            )

    weights_rows: list[tuple[Any, ...]] = []
    if weights_dir.exists() and weights_dir.is_dir():
        for file_path in sorted(weights_dir.glob("*.json")):
            try:
                payload = json.loads(file_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if not isinstance(payload, dict):
                continue
            profile_id = file_path.stem.strip().lower()
            if not profile_id:
                continue
            multipliers = {}
            for key in (
                "skills",
                "title",
                "location_work_mode",
                "seniority",
                "recency",
                "compensation",
            ):
                value = payload.get(key)
                if isinstance(value, (int, float)) and not isinstance(value, bool):
                    multipliers[key] = float(max(0.5, min(1.5, value)))
                else:
                    multipliers[key] = 1.0
            weights_rows.append(
                (
                    profile_id,
                    json.dumps(multipliers, sort_keys=True),
                    datetime.now(UTC) + timedelta(days=_PROFILE_TTL_DAYS),
                )
            )

    return _FeedbackSyncPayload(events_rows=events_rows, weights_rows=weights_rows)


def _sync_jobs_and_features(
    cursor: _CursorProtocol,
    *,
    schema: str,
    prepared: _PreparedSyncPayload,
    full_refresh: bool,
) -> tuple[int, int, int]:
    cursor.execute(
        """
        CREATE TEMP TABLE stage_jobs (
            job_id TEXT PRIMARY KEY,
            source_job_id TEXT,
            id TEXT,
            title TEXT,
            company TEXT,
            location TEXT,
            work_mode TEXT,
            seniority TEXT,
            employment_type TEXT,
            remote BOOLEAN,
            description_text TEXT,
            description_html TEXT,
            skills TEXT[],
            salary_min DOUBLE PRECISION,
            salary_max DOUBLE PRECISION,
            salary_currency TEXT,
            salary_interval TEXT,
            apply_url TEXT,
            posted_at TIMESTAMPTZ,
            source_updated_at TIMESTAMPTZ,
            source TEXT,
            source_ref TEXT,
            job_url TEXT
        ) ON COMMIT DROP
        """
    )
    cursor.execute(
        """
        CREATE TEMP TABLE stage_features (
            job_id TEXT PRIMARY KEY,
            title_tokens TEXT[],
            skill_tokens TEXT[],
            location_text TEXT,
            work_mode TEXT,
            seniority TEXT,
            employment_type TEXT,
            salary_min DOUBLE PRECISION,
            salary_max DOUBLE PRECISION,
            quality_flags TEXT[],
            visa_no_sponsorship BOOLEAN,
            posted_at TIMESTAMPTZ,
            source_updated_at TIMESTAMPTZ
        ) ON COMMIT DROP
        """
    )

    if prepared.jobs_rows:
        cursor.executemany(
            """
            INSERT INTO stage_jobs(
                job_id, source_job_id, id, title, company, location, work_mode, seniority,
                employment_type, remote, description_text, description_html, skills,
                salary_min, salary_max, salary_currency, salary_interval, apply_url,
                posted_at, source_updated_at, source, source_ref, job_url
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s
            )
            """,
            prepared.jobs_rows,
        )

    if prepared.feature_rows:
        cursor.executemany(
            """
            INSERT INTO stage_features(
                job_id, title_tokens, skill_tokens, location_text, work_mode, seniority,
                employment_type, salary_min, salary_max, quality_flags, visa_no_sponsorship,
                posted_at, source_updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s
            )
            """,
            prepared.feature_rows,
        )

    if full_refresh:
        cursor.execute(f"TRUNCATE TABLE {schema}.job_features")
        cursor.execute(f"TRUNCATE TABLE {schema}.jobs_live")

    cursor.execute(
        f"""
        SELECT COUNT(*)
        FROM stage_jobs sj
        LEFT JOIN {schema}.jobs_live jl ON jl.job_id = sj.job_id
        WHERE jl.job_id IS NULL
        """
    )
    inserted_row = cursor.fetchone()
    inserted_count = int(inserted_row[0]) if inserted_row is not None else 0

    cursor.execute(
        f"""
        SELECT COUNT(*)
        FROM stage_jobs sj
        JOIN {schema}.jobs_live jl ON jl.job_id = sj.job_id
        """
    )
    updated_row = cursor.fetchone()
    updated_count = int(updated_row[0]) if updated_row is not None else 0

    cursor.execute(
        f"""
        SELECT COUNT(*)
        FROM {schema}.jobs_live jl
        WHERE jl.is_active = TRUE
        AND NOT EXISTS (SELECT 1 FROM stage_jobs sj WHERE sj.job_id = jl.job_id)
        """
    )
    deactivated_row = cursor.fetchone()
    deactivated_count = int(deactivated_row[0]) if deactivated_row is not None else 0

    cursor.execute(
        f"""
        INSERT INTO {schema}.jobs_live(
            job_id, source_job_id, id, title, company, location, work_mode, seniority,
            employment_type, remote, description_text, description_html, skills,
            salary_min, salary_max, salary_currency, salary_interval, apply_url,
            posted_at, source_updated_at, source, source_ref, job_url, is_active, updated_at
        )
        SELECT
            job_id, source_job_id, id, title, company, location, work_mode, seniority,
            employment_type, remote, description_text, description_html, skills,
            salary_min, salary_max, salary_currency, salary_interval, apply_url,
            posted_at, source_updated_at, source, source_ref, job_url, TRUE, NOW()
        FROM stage_jobs
        ON CONFLICT (job_id) DO UPDATE SET
            source_job_id = EXCLUDED.source_job_id,
            id = EXCLUDED.id,
            title = EXCLUDED.title,
            company = EXCLUDED.company,
            location = EXCLUDED.location,
            work_mode = EXCLUDED.work_mode,
            seniority = EXCLUDED.seniority,
            employment_type = EXCLUDED.employment_type,
            remote = EXCLUDED.remote,
            description_text = EXCLUDED.description_text,
            description_html = EXCLUDED.description_html,
            skills = EXCLUDED.skills,
            salary_min = EXCLUDED.salary_min,
            salary_max = EXCLUDED.salary_max,
            salary_currency = EXCLUDED.salary_currency,
            salary_interval = EXCLUDED.salary_interval,
            apply_url = EXCLUDED.apply_url,
            posted_at = EXCLUDED.posted_at,
            source_updated_at = EXCLUDED.source_updated_at,
            source = EXCLUDED.source,
            source_ref = EXCLUDED.source_ref,
            job_url = EXCLUDED.job_url,
            is_active = TRUE,
            updated_at = NOW()
        """
    )

    cursor.execute(
        f"""
        UPDATE {schema}.jobs_live jl
        SET is_active = FALSE, updated_at = NOW()
        WHERE jl.is_active = TRUE
        AND NOT EXISTS (SELECT 1 FROM stage_jobs sj WHERE sj.job_id = jl.job_id)
        """
    )

    cursor.execute(
        f"""
        INSERT INTO {schema}.job_features(
            job_id, title_tokens, skill_tokens, location_text, work_mode, seniority,
            employment_type, salary_min, salary_max, quality_flags,
            visa_no_sponsorship, posted_at, source_updated_at, updated_at
        )
        SELECT
            sf.job_id,
            sf.title_tokens,
            sf.skill_tokens,
            sf.location_text,
            sf.work_mode,
            sf.seniority,
            sf.employment_type,
            sf.salary_min,
            sf.salary_max,
            sf.quality_flags,
            sf.visa_no_sponsorship,
            sf.posted_at,
            sf.source_updated_at,
            NOW()
        FROM stage_features sf
        ON CONFLICT (job_id) DO UPDATE SET
            title_tokens = EXCLUDED.title_tokens,
            skill_tokens = EXCLUDED.skill_tokens,
            location_text = EXCLUDED.location_text,
            work_mode = EXCLUDED.work_mode,
            seniority = EXCLUDED.seniority,
            employment_type = EXCLUDED.employment_type,
            salary_min = EXCLUDED.salary_min,
            salary_max = EXCLUDED.salary_max,
            quality_flags = EXCLUDED.quality_flags,
            visa_no_sponsorship = EXCLUDED.visa_no_sponsorship,
            posted_at = EXCLUDED.posted_at,
            source_updated_at = EXCLUDED.source_updated_at,
            updated_at = NOW()
        """
    )

    cursor.execute(
        f"""
        DELETE FROM {schema}.job_features jf
        WHERE NOT EXISTS (
            SELECT 1 FROM {schema}.jobs_live jl
            WHERE jl.job_id = jf.job_id
            AND jl.is_active = TRUE
        )
        """
    )

    return inserted_count, updated_count, deactivated_count


def _sync_facets(
    cursor: _CursorProtocol,
    *,
    schema: str,
    facets_rows: list[tuple[Any, ...]],
) -> None:
    cursor.execute(f"DELETE FROM {schema}.job_facets")
    if facets_rows:
        cursor.executemany(
            f"INSERT INTO {schema}.job_facets(facet_name, facet_value, facet_count, updated_at) VALUES (%s, %s, %s, NOW())",
            facets_rows,
        )


def _sync_feedback(
    cursor: _CursorProtocol,
    *,
    schema: str,
    payload: _FeedbackSyncPayload,
) -> None:
    if payload.events_rows:
        cursor.executemany(
            f"""
            INSERT INTO {schema}.feedback_events(profile_id, job_id, event, meta, event_hash, recorded_at)
            VALUES (%s, %s, %s, %s::jsonb, %s, COALESCE(%s, NOW()))
            ON CONFLICT (event_hash) DO NOTHING
            """,
            payload.events_rows,
        )
    if payload.weights_rows:
        cursor.executemany(
            f"""
            INSERT INTO {schema}.profile_weights(profile_id, multipliers, updated_at, expires_at)
            VALUES (%s, %s::jsonb, NOW(), %s)
            ON CONFLICT (profile_id) DO UPDATE SET
                multipliers = EXCLUDED.multipliers,
                updated_at = NOW(),
                expires_at = EXCLUDED.expires_at
            """,
            payload.weights_rows,
        )


def _insert_publish_batch_started(
    cursor: _CursorProtocol,
    *,
    schema: str,
    batch_id: str,
    require_quality_pass: bool,
    quality_gate_status: str,
    full_refresh: bool,
    jobs_parquet_hash: str,
    index_manifest_hash: str | None,
    policy_hash: str | None,
) -> None:
    cursor.execute(
        f"""
        INSERT INTO {schema}.publish_batches(
            batch_id, status, started_at, schema_version,
            jobs_parquet_hash, index_manifest_hash, policy_hash,
            require_quality_pass, quality_gate_status, full_refresh
        ) VALUES (%s, 'running', NOW(), %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (batch_id) DO UPDATE SET
            status = 'running',
            started_at = NOW(),
            finished_at = NULL,
            jobs_parquet_hash = EXCLUDED.jobs_parquet_hash,
            index_manifest_hash = EXCLUDED.index_manifest_hash,
            policy_hash = EXCLUDED.policy_hash,
            require_quality_pass = EXCLUDED.require_quality_pass,
            quality_gate_status = EXCLUDED.quality_gate_status,
            full_refresh = EXCLUDED.full_refresh,
            error_message = NULL
        """,
        (
            batch_id,
            SCHEMA_VERSION,
            jobs_parquet_hash,
            index_manifest_hash,
            policy_hash,
            require_quality_pass,
            quality_gate_status,
            full_refresh,
        ),
    )


def _complete_publish_batch(
    cursor: _CursorProtocol,
    *,
    schema: str,
    batch_id: str,
    status: str,
    inserted_count: int,
    updated_count: int,
    deactivated_count: int,
    active_jobs: int,
    error_message: str | None,
) -> None:
    cursor.execute(
        f"""
        UPDATE {schema}.publish_batches
        SET status = %s,
            finished_at = NOW(),
            inserted_count = %s,
            updated_count = %s,
            deactivated_count = %s,
            active_jobs = %s,
            error_message = %s
        WHERE batch_id = %s
        """,
        (
            status,
            inserted_count,
            updated_count,
            deactivated_count,
            active_jobs,
            error_message,
            batch_id,
        ),
    )


def _resolve_batch_id(batch_id: str | None) -> str:
    if batch_id not in (None, ""):
        normalized = str(batch_id).strip()
        if not normalized:
            raise ConfigValidationError("batch-id must be non-empty when provided")
        return normalized
    return datetime.now(UTC).strftime("batch-%Y%m%d%H%M%S")


def _build_facets(rows: list[dict[str, Any]]) -> list[tuple[str, str, int]]:
    counters: dict[str, Counter[str]] = {
        "source": Counter(),
        "location": Counter(),
        "work_mode": Counter(),
        "seniority": Counter(),
        "employment_type": Counter(),
    }
    for row in rows:
        _add_facet_value(counters["source"], row.get("source"))
        _add_facet_value(counters["location"], row.get("location"))
        _add_facet_value(counters["work_mode"], row.get("work_mode"))
        _add_facet_value(counters["seniority"], row.get("seniority"))
        _add_facet_value(counters["employment_type"], row.get("employment_type"))

    out: list[tuple[str, str, int]] = []
    for facet_name in sorted(counters):
        counter = counters[facet_name]
        for facet_value, count in sorted(counter.items()):
            out.append((facet_name, facet_value, int(count)))
    return out


def _add_facet_value(counter: Counter[str], value: Any) -> None:
    text = _coerce_text(value)
    if text:
        counter[text.lower()] += 1


def _skill_tokens(row: dict[str, Any]) -> list[str]:
    tokens = set(_to_text_array(row.get("skills")))
    tokens |= tokenize_text(_coerce_text(row.get("title")) or "")
    tokens |= tokenize_text(_coerce_text(row.get("description_text")) or "")
    return sorted(tokens)


def _quality_flags(row: dict[str, Any]) -> list[str]:
    flags: list[str] = []
    if not _coerce_text(row.get("company")):
        flags.append("MISSING_COMPANY")
    if _parse_timestamp(row.get("posted_at")) is None:
        flags.append("MISSING_POSTED_AT")
    if not _coerce_text(row.get("description_text")):
        flags.append("MISSING_DESCRIPTION")
    return flags


def _visa_no_sponsorship(row: dict[str, Any]) -> bool:
    text = " ".join(
        item
        for item in (
            _coerce_text(row.get("title")) or "",
            _coerce_text(row.get("description_text")) or "",
            _coerce_text(row.get("description_html")) or "",
        )
        if item
    ).lower()
    return "no sponsorship" in text or "not sponsor" in text


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _coerce_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _to_text_array(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        source = [str(item).strip().lower() for item in value]
    elif isinstance(value, str):
        source = [item.strip().lower() for item in value.split(",")]
    else:
        return []

    out: list[str] = []
    seen: set[str] = set()
    for item in source:
        if item and item not in seen:
            seen.add(item)
            out.append(item)
    return out


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str) and value.strip():
        try:
            return float(value.strip())
        except ValueError:
            return None
    return None


def _coerce_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes", "y"}:
            return True
        if lowered in {"false", "0", "no", "n"}:
            return False
    return None


def _parse_timestamp(value: Any) -> datetime | None:
    text = _coerce_text(value)
    if text is None:
        return None
    parsed_text = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(parsed_text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)
