from __future__ import annotations

from collections import Counter
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
import json
from pathlib import Path
import sys
import types

import polars as pl
import pytest

from honestroles.cli import lineage, output
from honestroles.cli.main import build_parser, main
from honestroles.errors import ConfigValidationError
from honestroles.publish import neondb as neondb_mod
from honestroles.publish.models import (
    NeonCheck,
    NeonMigrationResult,
    NeonPublishResult,
    NeonVerifyResult,
)
from honestroles.publish.sql import REQUIRED_FUNCTIONS, REQUIRED_TABLES, migrations_for_schema


@dataclass
class _ScriptedCursor:
    fetchone_values: list[tuple[object, ...]]
    fetchall_values: list[list[tuple[object, ...]]]

    def __post_init__(self) -> None:
        self.executed: list[tuple[str, object | None]] = []
        self.executemany_calls: list[tuple[str, list[tuple[object, ...]]]] = []
        self.rowcount = 0

    def execute(self, query: str, params: object | None = None) -> None:
        self.executed.append((query, params))
        self.rowcount = 1

    def executemany(self, query: str, params_seq: list[tuple[object, ...]]) -> None:
        rows = list(params_seq)
        self.executemany_calls.append((query, rows))
        self.rowcount = len(rows)

    def fetchone(self) -> tuple[object, ...] | None:
        if not self.fetchone_values:
            return None
        return self.fetchone_values.pop(0)

    def fetchall(self) -> list[tuple[object, ...]]:
        if not self.fetchall_values:
            return []
        return self.fetchall_values.pop(0)


@contextmanager
def _cursor_ctx(cursor: _ScriptedCursor):
    yield cursor


def _jobs_and_index(tmp_path: Path) -> tuple[Path, Path]:
    jobs = tmp_path / "jobs.parquet"
    pl.DataFrame(
        {
            "id": ["1", "2"],
            "source_job_id": ["gh-1", "gh-2"],
            "title": ["Senior Data Engineer", "Backend Engineer"],
            "company": ["Acme", "Beta"],
            "location": ["Remote US", "Lisbon"],
            "work_mode": ["remote", "onsite"],
            "seniority": ["senior", "mid"],
            "employment_type": ["full_time", "contract"],
            "remote": [True, False],
            "description_text": ["python sql sponsorship", "go postgres no sponsorship"],
            "description_html": ["<p>x</p>", "<p>y</p>"],
            "skills": [["python", "sql"], ["go", "postgres"]],
            "salary_min": [120000.0, 70000.0],
            "salary_max": [180000.0, 90000.0],
            "salary_currency": ["USD", "EUR"],
            "salary_interval": ["year", "year"],
            "apply_url": ["https://jobs/1", "https://jobs/2"],
            "posted_at": ["2026-03-01T00:00:00Z", "2026-03-02T00:00:00Z"],
            "source_updated_at": ["2026-03-03T00:00:00Z", "2026-03-03T00:00:00Z"],
            "source": ["greenhouse", "greenhouse"],
            "source_ref": ["stripe", "stripe"],
            "job_url": ["https://jobs/1", "https://jobs/2"],
        }
    ).write_parquet(jobs)

    index_dir = tmp_path / "index"
    index_dir.mkdir()
    (index_dir / "manifest.json").write_text(
        json.dumps(
            {
                "policy_hash": "abc123",
                "index_id": "idx",
                "files": {"jobs_latest": "jobs_latest.jsonl"},
            }
        ),
        encoding="utf-8",
    )
    (index_dir / "jobs_latest.jsonl").write_text(
        "\n".join(
            [
                json.dumps({"job_id": "gh-1", "title": "Senior Data Engineer"}),
                json.dumps({"job_id": "gh-2", "title": "Backend Engineer"}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return jobs, index_dir


def test_publish_parser_commands() -> None:
    parser = build_parser()

    args_migrate = parser.parse_args(["publish", "neondb", "migrate"])
    assert args_migrate.command == "publish"
    assert args_migrate.publish_target == "neondb"
    assert args_migrate.publish_neondb_command == "migrate"

    args_sync = parser.parse_args(
        [
            "publish",
            "neondb",
            "sync",
            "--jobs-parquet",
            "jobs.parquet",
            "--index-dir",
            "dist/recommend/index/x",
            "--no-require-quality-pass",
            "--full-refresh",
            "--batch-id",
            "b1",
        ]
    )
    assert args_sync.publish_neondb_command == "sync"
    assert args_sync.require_quality_pass is False
    assert args_sync.full_refresh is True

    args_verify = parser.parse_args(["publish", "neondb", "verify"])
    assert args_verify.publish_neondb_command == "verify"


def test_validate_schema_and_database_env(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(ConfigValidationError, match="schema must be non-empty"):
        neondb_mod._validate_schema(" ")
    with pytest.raises(ConfigValidationError, match="must match"):
        neondb_mod._validate_schema("bad-name")
    assert neondb_mod._validate_schema("honestroles_api") == "honestroles_api"

    with pytest.raises(ConfigValidationError, match="non-empty"):
        neondb_mod._resolve_database_url(" ")

    monkeypatch.delenv("NEON_DATABASE_URL", raising=False)
    with pytest.raises(ConfigValidationError, match="missing database URL"):
        neondb_mod._resolve_database_url("NEON_DATABASE_URL")

    monkeypatch.setenv("NEON_DATABASE_URL", "postgres://x")
    env_name, value = neondb_mod._resolve_database_url("NEON_DATABASE_URL")
    assert env_name == "NEON_DATABASE_URL"
    assert value == "postgres://x"


def test_db_cursor_import_and_runtime_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "psycopg", None)
    with pytest.raises(ConfigValidationError, match=r"honestroles\[db\]"):
        with neondb_mod._db_cursor("postgres://x"):
            pass

    class _FakeCursor:
        def execute(self, _q: str, _p: object | None = None) -> None:
            return None

        def executemany(self, _q: str, _rows: list[tuple[object, ...]]) -> None:
            return None

        def fetchone(self) -> tuple[object, ...] | None:
            return None

        def fetchall(self) -> list[tuple[object, ...]]:
            return []

        def close(self) -> None:
            return None

    class _FakeConn:
        def __init__(self) -> None:
            self.committed = False
            self.rolled_back = False

        def cursor(self) -> _FakeCursor:
            return _FakeCursor()

        def commit(self) -> None:
            self.committed = True

        def rollback(self) -> None:
            self.rolled_back = True

        def close(self) -> None:
            return None

    fake_module = types.SimpleNamespace(connect=lambda _url: _FakeConn())
    monkeypatch.setitem(sys.modules, "psycopg", fake_module)

    with neondb_mod._db_cursor("postgres://x") as cursor:
        assert cursor is not None

    with pytest.raises(RuntimeError):
        with neondb_mod._db_cursor("postgres://x"):
            raise RuntimeError("boom")


def test_apply_migrations_and_checksum_mismatch() -> None:
    cursor_ok = _ScriptedCursor(fetchone_values=[], fetchall_values=[[]])
    applied = neondb_mod._apply_migrations(cursor_ok, "honestroles_api")
    assert applied == ["0001_neon_agent_api_v1"]

    bad_checksum = _ScriptedCursor(
        fetchone_values=[],
        fetchall_values=[[ ("0001_neon_agent_api_v1", "bad") ]],
    )
    with pytest.raises(ConfigValidationError, match="checksum mismatch"):
        neondb_mod._apply_migrations(bad_checksum, "honestroles_api")


def test_migrate_neondb_success_and_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NEON_DATABASE_URL", "postgres://ok")
    cursor = _ScriptedCursor(fetchone_values=[], fetchall_values=[[]])
    monkeypatch.setattr(neondb_mod, "_db_cursor", lambda _url: _cursor_ctx(cursor))
    result = neondb_mod.migrate_neondb()
    assert result.status == "pass"
    assert result.migrations_applied

    monkeypatch.setattr(
        neondb_mod,
        "_db_cursor",
        lambda _url: (_ for _ in ()).throw(RuntimeError("db down")),
    )
    with pytest.raises(neondb_mod.NeonRuntimeError, match="migrate failed"):
        neondb_mod.migrate_neondb()


def test_verify_neondb_contract_pass_and_fail(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NEON_DATABASE_URL", "postgres://ok")

    table_rows = [[(name,) for name in REQUIRED_TABLES]]
    fn_rows = [[(name,) for name in REQUIRED_FUNCTIONS]]
    cursor_pass = _ScriptedCursor(
        fetchone_values=[("0001_neon_agent_api_v1",)],
        fetchall_values=[table_rows[0], fn_rows[0]],
    )
    monkeypatch.setattr(neondb_mod, "_db_cursor", lambda _url: _cursor_ctx(cursor_pass))
    result_pass = neondb_mod.verify_neondb_contract()
    assert result_pass.status == "pass"
    assert result_pass.check_codes == ()

    cursor_fail = _ScriptedCursor(
        fetchone_values=[("0000_old",)],
        fetchall_values=[[("jobs_live",)], []],
    )
    monkeypatch.setattr(neondb_mod, "_db_cursor", lambda _url: _cursor_ctx(cursor_fail))
    result_fail = neondb_mod.verify_neondb_contract()
    assert result_fail.status == "fail"
    assert result_fail.check_codes

    # Missing migration_history should still yield a deterministic fail payload.
    cursor_missing_history = _ScriptedCursor(
        fetchone_values=[],
        fetchall_values=[[("jobs_live",)], [(name,) for name in REQUIRED_FUNCTIONS]],
    )
    monkeypatch.setattr(
        neondb_mod, "_db_cursor", lambda _url: _cursor_ctx(cursor_missing_history)
    )
    result_missing_history = neondb_mod.verify_neondb_contract()
    assert result_missing_history.status == "fail"
    assert "NEON_MIGRATION_LATEST" in result_missing_history.check_codes


def test_quality_gate_paths(tmp_path: Path) -> None:
    with pytest.raises(ConfigValidationError, match="require-quality-pass"):
        neondb_mod._evaluate_quality_gate(sync_report_path=None, require_quality_pass=True)

    assert (
        neondb_mod._evaluate_quality_gate(sync_report_path=None, require_quality_pass=False)
        == "skipped"
    )

    missing = tmp_path / "missing.json"
    with pytest.raises(ConfigValidationError, match="does not exist"):
        neondb_mod._evaluate_quality_gate(sync_report_path=missing, require_quality_pass=True)

    bad = tmp_path / "bad.json"
    bad.write_text("{", encoding="utf-8")
    with pytest.raises(ConfigValidationError, match="invalid sync report JSON"):
        neondb_mod._evaluate_quality_gate(sync_report_path=bad, require_quality_pass=True)

    warn = tmp_path / "warn.json"
    warn.write_text(json.dumps({"quality_status": "warn"}), encoding="utf-8")
    with pytest.raises(neondb_mod.NeonRuntimeError, match="quality gate failed"):
        neondb_mod._evaluate_quality_gate(sync_report_path=warn, require_quality_pass=True)

    ok = tmp_path / "ok.json"
    ok.write_text(json.dumps({"quality_status": "pass"}), encoding="utf-8")
    assert neondb_mod._evaluate_quality_gate(sync_report_path=ok, require_quality_pass=True) == "pass"


def test_prepare_sync_payload_and_helpers(tmp_path: Path) -> None:
    jobs, index_dir = _jobs_and_index(tmp_path)
    payload = neondb_mod._prepare_sync_payload(jobs_parquet=jobs, index_dir=index_dir)
    assert payload.active_jobs == 2
    assert payload.jobs_rows
    assert payload.feature_rows
    assert payload.facets_rows
    assert payload.policy_hash == "abc123"

    assert neondb_mod._coerce_text(" ") is None
    assert neondb_mod._coerce_float("bad") is None
    assert neondb_mod._coerce_float(1) == 1.0
    assert neondb_mod._coerce_bool("yes") is True
    assert neondb_mod._coerce_bool("no") is False
    assert neondb_mod._coerce_bool("maybe") is None
    assert neondb_mod._parse_timestamp("2026-01-01T00:00:00Z") is not None
    assert neondb_mod._parse_timestamp("bad") is None
    assert neondb_mod._to_text_array("a,a,b") == ["a", "b"]
    assert neondb_mod._to_text_array(5) == []

    flags = neondb_mod._quality_flags({"company": None, "posted_at": None, "description_text": None})
    assert "MISSING_COMPANY" in flags

    assert neondb_mod._visa_no_sponsorship({"description_text": "No sponsorship"})


def test_collect_feedback_sync_payload(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    root = tmp_path / ".honestroles" / "recommend" / "feedback"
    root.mkdir(parents=True)
    (root / "events.jsonl").write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "profile_id": "jane",
                        "job_id": "g-1",
                        "event": "applied",
                        "event_hash": "h1",
                        "recorded_at_utc": "2026-01-01T00:00:00Z",
                        "meta": {"x": 1},
                    }
                ),
                "{}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    weights_dir = root / "weights"
    weights_dir.mkdir()
    (weights_dir / "jane.json").write_text(json.dumps({"skills": 2.0}), encoding="utf-8")
    (weights_dir / "bad.json").write_text("{", encoding="utf-8")

    payload = neondb_mod._collect_feedback_sync_payload()
    assert len(payload.events_rows) == 1
    assert len(payload.weights_rows) == 1


def test_sync_helpers_execute_paths() -> None:
    prepared = neondb_mod._PreparedSyncPayload(
        jobs_rows=[
            (
                "j1",
                "s1",
                "id1",
                "Title",
                "Company",
                "Remote",
                "remote",
                "senior",
                "full_time",
                True,
                "desc",
                "<p>desc</p>",
                ["python"],
                1.0,
                2.0,
                "USD",
                "year",
                "https://jobs/1",
                datetime.now(UTC),
                datetime.now(UTC),
                "greenhouse",
                "stripe",
                "https://jobs/1",
            )
        ],
        feature_rows=[
            (
                "j1",
                ["title"],
                ["python"],
                "remote",
                "remote",
                "senior",
                "full_time",
                1.0,
                2.0,
                ["MISSING_POSTED_AT"],
                False,
                datetime.now(UTC),
                datetime.now(UTC),
            )
        ],
        facets_rows=[("source", "greenhouse", 1)],
        jobs_parquet_hash="h",
        index_manifest_hash="i",
        policy_hash="p",
        active_jobs=1,
    )
    cursor = _ScriptedCursor(fetchone_values=[(1,), (0,), (2,)], fetchall_values=[])
    inserted, updated, deactivated = neondb_mod._sync_jobs_and_features(
        cursor,
        schema="honestroles_api",
        prepared=prepared,
        full_refresh=True,
    )
    assert (inserted, updated, deactivated) == (1, 0, 2)
    assert cursor.executemany_calls

    neondb_mod._sync_facets(cursor, schema="honestroles_api", facets_rows=prepared.facets_rows)
    neondb_mod._sync_feedback(
        cursor,
        schema="honestroles_api",
        payload=neondb_mod._FeedbackSyncPayload(
            events_rows=[("p", "j1", "applied", "{}", "h", None)],
            weights_rows=[("p", "{}", datetime.now(UTC))],
        ),
    )
    neondb_mod._insert_publish_batch_started(
        cursor,
        schema="honestroles_api",
        batch_id="b1",
        require_quality_pass=True,
        quality_gate_status="pass",
        full_refresh=False,
        jobs_parquet_hash="h",
        index_manifest_hash="i",
        policy_hash="p",
    )
    neondb_mod._complete_publish_batch(
        cursor,
        schema="honestroles_api",
        batch_id="b1",
        status="pass",
        inserted_count=1,
        updated_count=2,
        deactivated_count=3,
        active_jobs=4,
        error_message=None,
    )


def test_publish_neondb_sync_and_profile_cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    jobs, index_dir = _jobs_and_index(tmp_path)
    sync_report = tmp_path / "sync_report.json"
    sync_report.write_text(json.dumps({"quality_status": "pass"}), encoding="utf-8")

    monkeypatch.setenv("NEON_DATABASE_URL", "postgres://ok")

    prepared = neondb_mod._prepare_sync_payload(jobs_parquet=jobs, index_dir=index_dir)
    feedback = neondb_mod._FeedbackSyncPayload(events_rows=[], weights_rows=[])

    cursors = [
        _ScriptedCursor(fetchone_values=[(0,), (0,), (0,)], fetchall_values=[[]]),
        _ScriptedCursor(fetchone_values=[(0,), (0,), (0,)], fetchall_values=[[]]),
        _ScriptedCursor(fetchone_values=[], fetchall_values=[[]]),
    ]

    def _next_cursor(_url: str):
        assert cursors, "unexpected extra database cursor request"
        return _cursor_ctx(cursors.pop(0))

    monkeypatch.setattr(neondb_mod, "_db_cursor", _next_cursor)
    monkeypatch.setattr(neondb_mod, "_prepare_sync_payload", lambda **_kwargs: prepared)
    monkeypatch.setattr(neondb_mod, "_collect_feedback_sync_payload", lambda: feedback)

    result = neondb_mod.publish_neondb_sync(
        jobs_parquet=jobs,
        index_dir=index_dir,
        sync_report=sync_report,
        batch_id="batch-1",
    )
    assert result.status == "pass"
    assert result.batch_id == "batch-1"

    result2 = neondb_mod.publish_neondb_sync(
        jobs_parquet=jobs,
        index_dir=index_dir,
        require_quality_pass=False,
    )
    assert result2.quality_gate_status == "skipped"

    with pytest.raises(ConfigValidationError, match="jobs parquet"):
        neondb_mod.publish_neondb_sync(
            jobs_parquet=tmp_path / "missing.parquet",
            index_dir=index_dir,
        )

    with pytest.raises(ConfigValidationError, match="index directory"):
        neondb_mod.publish_neondb_sync(
            jobs_parquet=jobs,
            index_dir=tmp_path / "missing_index",
        )

    monkeypatch.setattr(
        neondb_mod,
        "_evaluate_quality_gate",
        lambda **_kwargs: (_ for _ in ()).throw(neondb_mod.NeonRuntimeError("gate failed")),
    )
    with pytest.raises(neondb_mod.NeonRuntimeError, match="gate failed"):
        neondb_mod.publish_neondb_sync(
            jobs_parquet=jobs,
            index_dir=index_dir,
            sync_report=sync_report,
        )

    monkeypatch.setattr(neondb_mod, "_evaluate_quality_gate", lambda **_kwargs: "pass")

    profile_result = neondb_mod.upsert_profile_cache_neondb(
        profile_id="Jane",
        profile_payload={"profile_id": "jane", "skills": ["python"]},
    )
    assert profile_result["status"] == "pass"
    assert profile_result["profile_id"] == "jane"

    with pytest.raises(ConfigValidationError, match="profile_id"):
        neondb_mod.upsert_profile_cache_neondb(profile_id=" ", profile_payload={})
    with pytest.raises(ConfigValidationError, match="must be an object"):
        neondb_mod.upsert_profile_cache_neondb(profile_id="x", profile_payload=[])  # type: ignore[arg-type]
    with pytest.raises(ConfigValidationError, match="ttl_days"):
        neondb_mod.upsert_profile_cache_neondb(profile_id="x", profile_payload={}, ttl_days=0)


def test_migration_sql_and_contract_files(tmp_path: Path) -> None:
    sql = migrations_for_schema("honestroles_api")[0].sql
    assert "match_jobs_v1" in sql
    assert "FILTER_LOCATION" in sql
    assert "ORDER BY (CARDINALITY(r.exclude_reasons) > 0) ASC, score DESC, r.posted_at DESC" in sql

    request_schema = Path("contracts/agent_request.v1.json")
    response_schema = Path("contracts/agent_response.v1.json")
    req = json.loads(request_schema.read_text(encoding="utf-8"))
    resp = json.loads(response_schema.read_text(encoding="utf-8"))
    assert req["title"] == "HonestRoles Agent Request v1"
    assert "candidate" in req["properties"]
    assert resp["title"] == "HonestRoles Agent Response v1"
    assert "results" in resp["properties"]


def test_publish_cli_and_lineage_output(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("NEON_DATABASE_URL", "postgres://ok")
    runs_root = tmp_path / ".honestroles" / "runs"
    monkeypatch.setattr("honestroles.cli.lineage.runs_root", lambda: runs_root)

    # command key + tracking
    assert (
        lineage._command_key(
            {
                "command": "publish",
                "publish_target": "neondb",
                "publish_neondb_command": "sync",
            }
        )
        == "publish.neondb.sync"
    )
    assert lineage.should_track(
        {
            "command": "publish",
            "publish_target": "neondb",
            "publish_neondb_command": "verify",
        }
    )

    args = {
        "command": "publish",
        "publish_target": "neondb",
        "publish_neondb_command": "sync",
        "jobs_parquet": str(tmp_path / "jobs.parquet"),
        "index_dir": str(tmp_path / "idx"),
        "sync_report": str(tmp_path / "sync_report.json"),
    }
    (tmp_path / "jobs.parquet").write_bytes(b"x")
    (tmp_path / "idx").mkdir()
    (tmp_path / "idx" / "manifest.json").write_text("{}", encoding="utf-8")
    (tmp_path / "sync_report.json").write_text("{}", encoding="utf-8")

    input_hash, input_hashes, config_hash = lineage.compute_hashes(args)
    assert input_hash is None
    assert "jobs_parquet" in input_hashes
    assert len(config_hash) == 64

    artifacts = lineage.build_artifact_paths(args, None)
    assert artifacts["jobs_parquet"].endswith("jobs.parquet")

    record = lineage.create_record(
        args=args,
        exit_code=0,
        started_at=datetime(2026, 1, 1, tzinfo=UTC),
        finished_at=datetime(2026, 1, 1, tzinfo=UTC),
        payload={
            "schema": "honestroles_api",
            "batch_id": "b1",
            "inserted_count": 1,
            "updated_count": 2,
            "deactivated_count": 3,
            "active_jobs": 10,
            "quality_gate_status": "pass",
            "check_codes": ["NEON_SYNC_COMPLETED"],
        },
    )
    assert record["publish_metrics"] is not None
    assert record["recommend_metrics"] is None

    output.emit_payload(
        {
            "status": "pass",
            "schema": "honestroles_api",
            "database_url_env": "NEON_DATABASE_URL",
            "migrations_applied": ["0001_neon_agent_api_v1"],
            "migrations_total": 1,
            "checks": [NeonCheck("X", "pass", "ok").to_dict()],
        },
        "table",
    )
    assert "NEON MIGRATE" in capsys.readouterr().out

    output.emit_payload(
        {
            "status": "pass",
            "schema": "honestroles_api",
            "database_url_env": "NEON_DATABASE_URL",
            "batch_id": "b1",
            "active_jobs": 10,
            "inserted_count": 1,
            "updated_count": 2,
            "deactivated_count": 3,
            "quality_gate_status": "pass",
            "checks": [],
        },
        "table",
    )
    assert "NEON SYNC" in capsys.readouterr().out

    output.emit_payload(
        {
            "status": "fail",
            "schema": "honestroles_api",
            "database_url_env": "NEON_DATABASE_URL",
            "checks": [NeonCheck("A", "fail", "missing").to_dict()],
        },
        "table",
    )
    assert "NEON VERIFY" in capsys.readouterr().out


def test_publish_cli_main_commands(tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("NEON_DATABASE_URL", "postgres://ok")

    jobs, index_dir = _jobs_and_index(tmp_path)
    (jobs.with_name("sync_report.json")).write_text(
        json.dumps({"quality_status": "pass"}),
        encoding="utf-8",
    )

    migration_payload = {
        "schema_version": "1.0",
        "status": "pass",
        "schema": "honestroles_api",
        "database_url_env": "NEON_DATABASE_URL",
        "migrations_applied": ["0001_neon_agent_api_v1"],
        "migrations_total": 1,
        "duration_ms": 1,
        "checks": [],
        "check_codes": [],
    }
    verify_payload = {
        "schema_version": "1.0",
        "status": "fail",
        "schema": "honestroles_api",
        "database_url_env": "NEON_DATABASE_URL",
        "duration_ms": 1,
        "checks": [NeonCheck("NEON_TABLE_JOBS_LIVE", "fail", "missing").to_dict()],
        "check_codes": ["NEON_TABLE_JOBS_LIVE"],
    }
    sync_payload = {
        "schema_version": "1.0",
        "status": "pass",
        "schema": "honestroles_api",
        "database_url_env": "NEON_DATABASE_URL",
        "batch_id": "b1",
        "jobs_parquet": str(jobs),
        "index_dir": str(index_dir),
        "sync_report": str(jobs.with_name("sync_report.json")),
        "require_quality_pass": True,
        "quality_gate_status": "pass",
        "full_refresh": False,
        "inserted_count": 1,
        "updated_count": 0,
        "deactivated_count": 0,
        "facet_rows": 1,
        "feature_rows": 2,
        "active_jobs": 2,
        "migration_version": "0001_neon_agent_api_v1",
        "duration_ms": 1,
        "checks": [],
        "check_codes": [],
    }

    monkeypatch.setattr("honestroles.cli.handlers.migrate_neondb", lambda **_kwargs: types.SimpleNamespace(to_payload=lambda: migration_payload))
    monkeypatch.setattr("honestroles.cli.handlers.publish_neondb_sync", lambda **_kwargs: types.SimpleNamespace(to_payload=lambda: sync_payload))
    monkeypatch.setattr("honestroles.cli.handlers.verify_neondb_contract", lambda **_kwargs: types.SimpleNamespace(to_payload=lambda: verify_payload, status="fail"))

    code_migrate = main(["publish", "neondb", "migrate"])
    assert code_migrate == 0
    assert json.loads(capsys.readouterr().out)["migrations_total"] == 1

    code_sync = main(
        [
            "publish",
            "neondb",
            "sync",
            "--jobs-parquet",
            str(jobs),
            "--index-dir",
            str(index_dir),
        ]
    )
    assert code_sync == 0
    assert json.loads(capsys.readouterr().out)["batch_id"] == "b1"

    code_verify = main(["publish", "neondb", "verify"])
    assert code_verify == 1
    assert json.loads(capsys.readouterr().out)["status"] == "fail"


def test_neon_models_to_payload() -> None:
    migration = NeonMigrationResult(
        schema_version="1.0",
        status="pass",
        schema="honestroles_api",
        database_url_env="NEON_DATABASE_URL",
        migrations_applied=("0001",),
        migrations_total=1,
        duration_ms=10,
        checks=(NeonCheck("A", "pass", "ok"),),
        check_codes=("A",),
    )
    publish = NeonPublishResult(
        schema_version="1.0",
        status="pass",
        schema="honestroles_api",
        database_url_env="NEON_DATABASE_URL",
        batch_id="b1",
        jobs_parquet="/tmp/jobs.parquet",
        index_dir="/tmp/index",
        sync_report="/tmp/sync_report.json",
        require_quality_pass=True,
        quality_gate_status="pass",
        full_refresh=False,
        inserted_count=1,
        updated_count=2,
        deactivated_count=3,
        facet_rows=4,
        feature_rows=5,
        active_jobs=6,
        migration_version="0001",
        duration_ms=11,
        checks=(NeonCheck("B", "pass", "ok"),),
        check_codes=("B",),
    )
    verify = NeonVerifyResult(
        schema_version="1.0",
        status="pass",
        schema="honestroles_api",
        database_url_env="NEON_DATABASE_URL",
        duration_ms=12,
        checks=(NeonCheck("C", "pass", "ok"),),
        check_codes=("C",),
    )

    assert migration.to_payload()["migrations_total"] == 1
    assert publish.to_payload()["active_jobs"] == 6
    assert verify.to_payload()["duration_ms"] == 12


def test_lineage_publish_additional_branches(tmp_path: Path) -> None:
    assert lineage._command_key({"command": "publish", "publish_target": "neondb"}) == "publish.neondb"
    assert lineage.build_artifact_paths(
        {"command": "publish", "publish_target": "neondb", "publish_neondb_command": "migrate"},
        None,
    ) == {}
    assert lineage.build_artifact_paths(
        {"command": "publish", "publish_target": "neondb", "publish_neondb_command": "verify"},
        None,
    ) == {}
    assert lineage.build_artifact_paths(
        {"command": "publish", "publish_target": "neondb", "publish_neondb_command": "sync"},
        None,
    ) == {}

    partial_artifacts = lineage.build_artifact_paths(
        {
            "command": "publish",
            "publish_target": "neondb",
            "publish_neondb_command": "sync",
            "jobs_parquet": "",
            "index_dir": str(tmp_path / "index"),
            "sync_report": None,
        },
        None,
    )
    assert "jobs_parquet" not in partial_artifacts
    assert partial_artifacts["index_dir"].endswith("/index")


def test_publish_output_verify_branch_with_non_mapping_check(capsys: pytest.CaptureFixture[str]) -> None:
    output.emit_payload(
        {
            "status": "fail",
            "schema": "honestroles_api",
            "database_url_env": "NEON_DATABASE_URL",
            "checks": ["bad", NeonCheck("D", "fail", "missing").to_dict()],
        },
        "table",
    )
    rendered = capsys.readouterr().out
    assert "NEON VERIFY" in rendered
    assert "missing" in rendered

    output.emit_payload(
        {
            "status": "pass",
            "schema": "honestroles_api",
            "database_url_env": "NEON_DATABASE_URL",
            "checks": None,
        },
        "table",
    )
    assert "STATUS" in capsys.readouterr().out


def test_migrate_verify_config_runtime_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NEON_DATABASE_URL", "postgres://ok")

    monkeypatch.setattr(
        neondb_mod,
        "_db_cursor",
        lambda _url: (_ for _ in ()).throw(ConfigValidationError("bad config")),
    )
    with pytest.raises(ConfigValidationError, match="bad config"):
        neondb_mod.migrate_neondb()
    with pytest.raises(ConfigValidationError, match="bad config"):
        neondb_mod.verify_neondb_contract()

    monkeypatch.setattr(
        neondb_mod,
        "_db_cursor",
        lambda _url: (_ for _ in ()).throw(RuntimeError("runtime")),
    )
    with pytest.raises(neondb_mod.NeonRuntimeError, match="verify failed"):
        neondb_mod.verify_neondb_contract()


def test_publish_sync_exception_mapping(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    jobs, index_dir = _jobs_and_index(tmp_path)
    sync_report = tmp_path / "sync_report.json"
    sync_report.write_text(json.dumps({"quality_status": "pass"}), encoding="utf-8")
    monkeypatch.setenv("NEON_DATABASE_URL", "postgres://ok")

    prepared = neondb_mod._prepare_sync_payload(jobs_parquet=jobs, index_dir=index_dir)
    monkeypatch.setattr(neondb_mod, "_prepare_sync_payload", lambda **_kwargs: prepared)
    monkeypatch.setattr(
        neondb_mod,
        "_collect_feedback_sync_payload",
        lambda: neondb_mod._FeedbackSyncPayload(events_rows=[], weights_rows=[]),
    )

    monkeypatch.setattr(
        neondb_mod,
        "_db_cursor",
        lambda _url: (_ for _ in ()).throw(ConfigValidationError("bad db config")),
    )
    with pytest.raises(ConfigValidationError, match="bad db config"):
        neondb_mod.publish_neondb_sync(
            jobs_parquet=jobs,
            index_dir=index_dir,
            sync_report=sync_report,
        )

    monkeypatch.setattr(
        neondb_mod,
        "_db_cursor",
        lambda _url: (_ for _ in ()).throw(neondb_mod.NeonRuntimeError("runtime neon")),
    )
    with pytest.raises(neondb_mod.NeonRuntimeError, match="runtime neon"):
        neondb_mod.publish_neondb_sync(
            jobs_parquet=jobs,
            index_dir=index_dir,
            sync_report=sync_report,
        )

    monkeypatch.setattr(
        neondb_mod,
        "_db_cursor",
        lambda _url: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    with pytest.raises(neondb_mod.NeonRuntimeError, match="sync failed"):
        neondb_mod.publish_neondb_sync(
            jobs_parquet=jobs,
            index_dir=index_dir,
            sync_report=sync_report,
        )


def test_upsert_profile_cache_exception_mapping(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NEON_DATABASE_URL", "postgres://ok")
    monkeypatch.setattr(
        neondb_mod,
        "_db_cursor",
        lambda _url: (_ for _ in ()).throw(ConfigValidationError("config issue")),
    )
    with pytest.raises(ConfigValidationError, match="config issue"):
        neondb_mod.upsert_profile_cache_neondb(profile_id="ok", profile_payload={})

    monkeypatch.setattr(
        neondb_mod,
        "_db_cursor",
        lambda _url: (_ for _ in ()).throw(RuntimeError("db issue")),
    )
    with pytest.raises(neondb_mod.NeonRuntimeError, match="profile cache upsert failed"):
        neondb_mod.upsert_profile_cache_neondb(profile_id="ok", profile_payload={})


def test_db_cursor_config_validation_rollback(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeCursor:
        def execute(self, _q: str, _p: object | None = None) -> None:
            return None

        def executemany(self, _q: str, _rows: list[tuple[object, ...]]) -> None:
            return None

        def fetchone(self) -> tuple[object, ...] | None:
            return None

        def fetchall(self) -> list[tuple[object, ...]]:
            return []

        def close(self) -> None:
            return None

    class _FakeConn:
        def __init__(self) -> None:
            self.rolled_back = False

        def cursor(self) -> _FakeCursor:
            return _FakeCursor()

        def commit(self) -> None:
            return None

        def rollback(self) -> None:
            self.rolled_back = True

        def close(self) -> None:
            return None

    conn = _FakeConn()
    monkeypatch.setitem(sys.modules, "psycopg", types.SimpleNamespace(connect=lambda _url: conn))

    with pytest.raises(ConfigValidationError, match="boom"):
        with neondb_mod._db_cursor("postgres://x"):
            raise ConfigValidationError("boom")
    assert conn.rolled_back is True


def test_db_cursor_connect_failure_branches(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(
        sys.modules,
        "psycopg",
        types.SimpleNamespace(connect=lambda _url: (_ for _ in ()).throw(ConfigValidationError("bad connect"))),
    )
    with pytest.raises(ConfigValidationError, match="bad connect"):
        with neondb_mod._db_cursor("postgres://x"):
            pass

    monkeypatch.setitem(
        sys.modules,
        "psycopg",
        types.SimpleNamespace(connect=lambda _url: (_ for _ in ()).throw(RuntimeError("connect boom"))),
    )
    with pytest.raises(RuntimeError, match="connect boom"):
        with neondb_mod._db_cursor("postgres://x"):
            pass


def test_apply_migrations_existing_checksum_match() -> None:
    expected_checksum = migrations_for_schema("honestroles_api")[0].checksum
    cursor = _ScriptedCursor(
        fetchone_values=[],
        fetchall_values=[[("0001_neon_agent_api_v1", expected_checksum)]],
    )
    applied = neondb_mod._apply_migrations(cursor, "honestroles_api")
    assert applied == []


def test_quality_gate_oserror_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    report = tmp_path / "sync_report.json"
    report.write_text("{}", encoding="utf-8")
    original_read_text = Path.read_text

    def _raise_on_report(self: Path, *args: object, **kwargs: object) -> str:
        if self == report:
            raise OSError("permission denied")
        return original_read_text(self, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", _raise_on_report)
    with pytest.raises(ConfigValidationError, match="cannot read sync report"):
        neondb_mod._evaluate_quality_gate(sync_report_path=report, require_quality_pass=True)


def test_collect_feedback_sync_payload_additional_branches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    empty_payload = neondb_mod._collect_feedback_sync_payload()
    assert empty_payload.events_rows == []
    assert empty_payload.weights_rows == []

    root = tmp_path / ".honestroles" / "recommend" / "feedback"
    root.mkdir(parents=True)
    (root / "events.jsonl").write_text("\n[]\n{\n", encoding="utf-8")
    weights_dir = root / "weights"
    weights_dir.mkdir()
    (weights_dir / "list.json").write_text("[]", encoding="utf-8")
    (weights_dir / " .json").write_text("{}", encoding="utf-8")

    payload = neondb_mod._collect_feedback_sync_payload()
    assert payload.events_rows == []
    assert payload.weights_rows == []


def test_sync_helpers_empty_rows_and_scalar_helpers() -> None:
    prepared = neondb_mod._PreparedSyncPayload(
        jobs_rows=[],
        feature_rows=[],
        facets_rows=[],
        jobs_parquet_hash="h",
        index_manifest_hash=None,
        policy_hash=None,
        active_jobs=0,
    )
    cursor = _ScriptedCursor(fetchone_values=[(0,), (0,), (0,)], fetchall_values=[])
    assert neondb_mod._sync_jobs_and_features(
        cursor,
        schema="honestroles_api",
        prepared=prepared,
        full_refresh=False,
    ) == (0, 0, 0)
    neondb_mod._sync_facets(cursor, schema="honestroles_api", facets_rows=[])

    with pytest.raises(ConfigValidationError, match="batch-id"):
        neondb_mod._resolve_batch_id(" ")

    counter: Counter[str] = Counter()
    neondb_mod._add_facet_value(counter, "Remote")
    neondb_mod._add_facet_value(counter, "  ")
    assert counter["remote"] == 1

    assert neondb_mod._to_text_array(None) == []
    assert neondb_mod._coerce_float(None) is None
    assert neondb_mod._coerce_float(True) is None
    assert neondb_mod._coerce_float("") is None
    assert neondb_mod._coerce_bool("unknown") is None
    assert neondb_mod._coerce_bool(7) is None
    parsed = neondb_mod._parse_timestamp("2026-01-01T00:00:00")
    assert parsed is not None and parsed.tzinfo is not None
