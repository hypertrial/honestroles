from __future__ import annotations

import argparse
from datetime import UTC, datetime, timedelta
import importlib
from pathlib import Path

import polars as pl
import pytest

from honestroles.cli import handlers, lineage, output
from honestroles.errors import ConfigValidationError
from honestroles.reliability import evaluator as evaluator_mod
from honestroles.reliability import policy as policy_mod


def _pipeline_text(*, input_path: Path, output_path: Path | None) -> str:
    parts = [
        "[input]",
        'kind = "parquet"',
        f'path = "{input_path}"',
        "",
    ]
    if output_path is not None:
        parts.extend(["[output]", f'path = "{output_path}"', ""])
    parts.extend(
        [
            "[stages.clean]",
            "enabled = true",
            "",
            "[stages.filter]",
            "enabled = true",
            "remote_only = false",
            "",
            "[stages.label]",
            "enabled = true",
            "",
            "[stages.rate]",
            "enabled = true",
            "",
            "[stages.match]",
            "enabled = true",
            "top_k = 5",
            "",
            "[runtime]",
            "fail_fast = true",
            "random_seed = 1",
        ]
    )
    return "\n".join(parts).strip() + "\n"


def _write_pipeline(
    tmp_path: Path,
    *,
    input_path: Path,
    output_path: Path | None,
    filename: str = "pipeline.toml",
) -> Path:
    path = tmp_path / filename
    path.write_text(_pipeline_text(input_path=input_path, output_path=output_path), encoding="utf-8")
    return path


def _check_by_code(checks: list[dict[str, object]], code: str) -> dict[str, object]:
    for item in checks:
        if str(item.get("code")) == code:
            return item
    raise AssertionError(f"missing check code: {code}")


def test_policy_default_loader_and_hash() -> None:
    loaded = policy_mod.load_reliability_policy()
    assert loaded.source == "builtin:default"
    assert loaded.policy_hash
    assert loaded.policy.to_dict()["min_rows"] == 1


def test_policy_loader_from_file_with_duplicates(tmp_path: Path) -> None:
    policy_file = tmp_path / "reliability.toml"
    policy_file.write_text(
        """
min_rows = 3
required_columns = ["title", "description_text", "title"]

[max_null_pct]
title = 5
description_text = 7.5

[freshness]
column = "posted_at"
max_age_days = 14
""".strip(),
        encoding="utf-8",
    )
    loaded = policy_mod.load_reliability_policy(policy_file)
    assert loaded.source == str(policy_file.resolve())
    assert loaded.policy.required_columns == ("title", "description_text")
    assert loaded.policy.max_null_pct == {"title": 5.0, "description_text": 7.5}
    assert loaded.policy.freshness.column == "posted_at"


def test_policy_loader_path_and_toml_errors(tmp_path: Path) -> None:
    with pytest.raises(ConfigValidationError, match="does not exist"):
        policy_mod.load_reliability_policy(tmp_path / "missing.toml")

    policy_dir = tmp_path / "policy_dir"
    policy_dir.mkdir()
    with pytest.raises(ConfigValidationError, match="is not a file"):
        policy_mod.load_reliability_policy(policy_dir)

    bad = tmp_path / "bad.toml"
    bad.write_text("min_rows = ", encoding="utf-8")
    with pytest.raises(ConfigValidationError, match="invalid reliability policy"):
        policy_mod.load_reliability_policy(bad)


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ("not-a-table", "policy root must be a TOML table"),
        ({"min_rows": True}, "min_rows must be an integer"),
        ({"min_rows": 0}, "min_rows must be >= 1"),
        ({"required_columns": "title"}, "required_columns must be an array of strings"),
        ({"required_columns": [1]}, "required_columns entries must be strings"),
        ({"required_columns": [" "]}, "required_columns entries must be non-empty"),
        ({"max_null_pct": "x"}, "max_null_pct must be a table"),
        ({"max_null_pct": {1: 5}}, "max_null_pct keys must be strings"),
        ({"max_null_pct": {" ": 5}}, "max_null_pct keys must be non-empty"),
        ({"max_null_pct": {"title": True}}, "must be a number between 0 and 100"),
        ({"max_null_pct": {"title": 101}}, "must be between 0 and 100"),
        ({"freshness": "x"}, "freshness must be a table"),
        ({"freshness": {"column": ""}}, "freshness.column must be a non-empty string"),
        ({"freshness": {"max_age_days": "x"}}, "freshness.max_age_days must be an integer"),
        ({"freshness": {"max_age_days": -1}}, "freshness.max_age_days must be >= 0"),
    ],
)
def test_parse_policy_validation_errors(payload: object, message: str) -> None:
    with pytest.raises(ConfigValidationError, match=message):
        policy_mod._parse_policy(payload)  # pyright: ignore[reportArgumentType]


def test_evaluate_reliability_read_parquet_failure(
    monkeypatch: pytest.MonkeyPatch,
    sample_parquet: Path,
    tmp_path: Path,
) -> None:
    pipeline = _write_pipeline(
        tmp_path,
        input_path=sample_parquet,
        output_path=tmp_path / "out.parquet",
        filename="read_fail.toml",
    )

    def fail_read(_path: Path) -> pl.DataFrame:
        raise RuntimeError("boom")

    monkeypatch.setattr(evaluator_mod, "read_parquet", fail_read)
    evaluation = evaluator_mod.evaluate_reliability(
        pipeline_config=str(pipeline),
        plugin_manifest=None,
        sample_rows=10,
        policy_file=None,
    )
    sample_check = _check_by_code(evaluation.checks, "INPUT_SAMPLE_READ")
    assert sample_check["status"] == "fail"


def test_policy_check_variants_cover_warn_and_missing_fields() -> None:
    checks: list[dict[str, object]] = []
    now_text = datetime.now(UTC).isoformat()
    sample = pl.DataFrame({"title": ["x"], "posted_at": [now_text]})
    policy = policy_mod.ReliabilityPolicy(
        min_rows=2,
        required_columns=("title", "description_text"),
        max_null_pct={"title": 0.0, "missing_col": 1.0},
        freshness=policy_mod.FreshnessRule(column="missing_time", max_age_days=5),
    )
    evaluator_mod._evaluate_policy_checks(
        checks=checks,
        sample=sample,
        aliased=sample,
        normalized=sample,
        policy=policy,
    )
    assert _check_by_code(checks, "POLICY_MIN_ROWS")["status"] == "warn"
    assert _check_by_code(checks, "POLICY_REQUIRED_COLUMNS")["status"] == "warn"
    assert _check_by_code(checks, "POLICY_NULL_RATE")["status"] == "pass"
    assert _check_by_code(checks, "POLICY_FRESHNESS")["status"] == "warn"


def test_policy_checks_cover_fail_and_pass_paths() -> None:
    checks: list[dict[str, object]] = []
    recent = datetime.now(UTC).isoformat()
    sample = pl.DataFrame({"title": ["x"], "posted_at": [recent]})
    empty = sample.head(0)

    evaluator_mod._evaluate_policy_checks(
        checks=checks,
        sample=empty,
        aliased=empty,
        normalized=empty,
        policy=policy_mod.ReliabilityPolicy(
            min_rows=1,
            required_columns=("title",),
            max_null_pct={"title": 0.0},
            freshness=policy_mod.FreshnessRule(column="posted_at", max_age_days=10),
        ),
    )
    assert _check_by_code(checks, "POLICY_MIN_ROWS")["status"] == "fail"

    checks2: list[dict[str, object]] = []
    evaluator_mod._evaluate_policy_checks(
        checks=checks2,
        sample=sample,
        aliased=sample,
        normalized=sample,
        policy=policy_mod.ReliabilityPolicy(
            min_rows=1,
            required_columns=("title",),
            max_null_pct={"title": 100.0},
            freshness=policy_mod.FreshnessRule(column="posted_at", max_age_days=365),
        ),
    )
    assert _check_by_code(checks2, "POLICY_FRESHNESS")["status"] == "pass"


def test_evaluator_helper_functions() -> None:
    assert evaluator_mod._parse_datetime(None) is None
    assert evaluator_mod._parse_datetime("   ") is None
    assert evaluator_mod._parse_datetime("not-a-date") is None
    assert evaluator_mod._parse_datetime("2026-01-01T00:00:00Z") is not None
    aware = evaluator_mod._parse_datetime("2026-01-01T00:00:00+02:00")
    assert aware is not None and aware.tzinfo is not None

    latest = evaluator_mod._latest_timestamp(pl.Series([" ", "2026-01-01T00:00:00Z"]))
    assert latest is not None
    latest_two = evaluator_mod._latest_timestamp(
        pl.Series(["2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z"])
    )
    assert latest_two is not None and latest_two.isoformat().startswith("2026-01-02")
    latest_desc = evaluator_mod._latest_timestamp(
        pl.Series(["2026-01-02T00:00:00Z", "2026-01-01T00:00:00Z"])
    )
    assert latest_desc is not None and latest_desc.isoformat().startswith("2026-01-02")

    status, summary = evaluator_mod._status_summary([{"status": "pass"}])
    assert status == "pass"
    assert summary == {"pass": 1, "warn": 0, "fail": 0}

    codes = evaluator_mod._warn_fail_codes(
        [
            {"status": "warn", "code": "A"},
            {"status": "warn", "code": "A"},
            {"status": "warn", "code": " "},
            {"status": "pass", "code": "B"},
        ]
    )
    assert codes == ["A"]


def test_handle_reliability_check_writes_artifact_and_strict_escalates(
    sample_parquet: Path,
    tmp_path: Path,
) -> None:
    pipeline = _write_pipeline(
        tmp_path,
        input_path=sample_parquet,
        output_path=None,
        filename="strict_warn.toml",
    )
    output_file = tmp_path / "gate_result.json"
    result = handlers.handle_reliability_check(
        argparse.Namespace(
            pipeline_config=str(pipeline),
            plugin_manifest=None,
            sample_rows=50,
            policy_file=None,
            output_file=str(output_file),
            strict=True,
        )
    )
    assert result.exit_code == 1
    assert result.payload["strict_escalated"] is True
    assert output_file.exists()
    assert result.payload["reliability_artifact"] == str(output_file.resolve())


def test_handle_reliability_check_writes_failure_artifact_on_config_error(
    sample_parquet: Path,
    tmp_path: Path,
) -> None:
    pipeline = _write_pipeline(
        tmp_path,
        input_path=sample_parquet,
        output_path=tmp_path / "out.parquet",
        filename="policy_error.toml",
    )
    output_file = tmp_path / "gate_failure.json"
    with pytest.raises(ConfigValidationError):
        handlers.handle_reliability_check(
            argparse.Namespace(
                pipeline_config=str(pipeline),
                plugin_manifest=None,
                sample_rows=50,
                policy_file=str(tmp_path / "missing_policy.toml"),
                output_file=str(output_file),
                strict=False,
            )
        )
    payload = output_file.read_text(encoding="utf-8")
    assert '"status": "fail"' in payload


def test_handler_reliability_helpers_cover_branches() -> None:
    status, summary = handlers._doctor_status_summary([{"status": "pass"}])
    assert status == "pass"
    assert summary["pass"] == 1

    status2, _ = handlers._doctor_status_summary([{"status": "fail"}, {"status": "weird"}])
    assert status2 == "fail"

    assert handlers._reliability_exit_code(
        status="warn",
        strict=True,
        has_config_input_error=False,
    ) == 1


def test_handle_runs_list_since_and_invalid_since(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def fake_list_records(
        limit: int,
        status: str | None,
        command: str | None = None,
        since_utc: datetime | None = None,
        contains_code: str | None = None,
    ) -> list[dict[str, object]]:
        captured["limit"] = limit
        captured["status"] = status
        captured["command"] = command
        captured["since_utc"] = since_utc
        captured["contains_code"] = contains_code
        return []

    monkeypatch.setattr(handlers, "list_records", fake_list_records)
    result = handlers.handle_runs_list(
        argparse.Namespace(
            limit=5,
            status="pass",
            command_filter="reliability.check",
            since="2026-01-01T00:00:00Z",
            contains_code="POLICY_NULL_RATE",
        )
    )
    assert result.payload["count"] == 0
    since = captured["since_utc"]
    assert isinstance(since, datetime) and since.tzinfo is not None
    handlers.handle_runs_list(
        argparse.Namespace(
            limit=5,
            status="pass",
            command_filter="reliability.check",
            since="2026-01-01T00:00:00",
            contains_code=None,
        )
    )
    naive_since = captured["since_utc"]
    assert isinstance(naive_since, datetime) and naive_since.tzinfo == UTC

    with pytest.raises(ConfigValidationError, match="since must be ISO-8601 datetime"):
        handlers.handle_runs_list(
            argparse.Namespace(
                limit=5,
                status=None,
                command_filter=None,
                since="not-a-date",
                contains_code=None,
            )
        )


def test_lineage_reliability_hashing_and_filters(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    input_path = tmp_path / "jobs.parquet"
    pl.DataFrame({"id": ["1"], "title": ["x"], "company": ["y"], "description_text": ["z"]}).write_parquet(
        input_path
    )
    pipeline = _write_pipeline(tmp_path, input_path=input_path, output_path=tmp_path / "out.parquet")
    policy_file = tmp_path / "reliability.toml"
    policy_file.write_text("min_rows = 1\n", encoding="utf-8")

    assert lineage.should_track({"command": "reliability", "reliability_command": "check"})

    input_hash, input_hashes, config_hash = lineage.compute_hashes(
        {
            "command": "reliability",
            "reliability_command": "check",
            "pipeline_config": str(pipeline),
            "plugin_manifest": None,
            "policy_file": str(policy_file),
        }
    )
    assert input_hash
    assert input_hashes["input"] == input_hash
    assert config_hash

    started = datetime.now(UTC)
    record = lineage.create_record(
        args={"command": "reliability", "reliability_command": "check"},
        exit_code=1,
        started_at=started,
        finished_at=started + timedelta(milliseconds=1),
        payload={"check_codes": ["POLICY_NULL_RATE", " ", "POLICY_NULL_RATE"]},
    )
    assert record["check_codes"] == ["POLICY_NULL_RATE", "POLICY_NULL_RATE"]

    root = tmp_path / ".honestroles" / "runs"
    older = root / "older"
    newer = root / "newer"
    skipped = root / "skipped"
    older.mkdir(parents=True)
    newer.mkdir(parents=True)
    skipped.mkdir(parents=True)
    (older / "run.json").write_text(
        '{"run_id":"older","status":"pass","command":"reliability.check","started_at_utc":"2026-01-01T00:00:00","check_codes":["POLICY_NULL_RATE"]}',
        encoding="utf-8",
    )
    (newer / "run.json").write_text(
        '{"run_id":"newer","status":"pass","command":"reliability.check","started_at_utc":"2026-03-01T00:00:00+00:00","check_codes":["POLICY_FRESHNESS"]}',
        encoding="utf-8",
    )
    (skipped / "run.json").write_text(
        '{"run_id":"skipped","status":"pass","command":"run","started_at_utc":123,"check_codes":"x"}',
        encoding="utf-8",
    )
    invalid_date = root / "invalid_date"
    invalid_date.mkdir(parents=True)
    (invalid_date / "run.json").write_text(
        '{"run_id":"invalid_date","status":"pass","command":"reliability.check","started_at_utc":"not-a-date","check_codes":["POLICY_FRESHNESS"]}',
        encoding="utf-8",
    )

    rows = lineage.list_records(
        limit=10,
        status="pass",
        command="reliability.check",
        since_utc=datetime(2026, 2, 1, tzinfo=UTC),
        contains_code="POLICY_FRESHNESS",
    )
    assert [row["run_id"] for row in rows] == ["newer"]
    rows_unmatched = lineage.list_records(
        limit=10,
        status="pass",
        command=None,
        since_utc=datetime(2025, 1, 1, tzinfo=UTC),
        contains_code="DOES_NOT_EXIST",
    )
    assert rows_unmatched == []
    fallback_artifacts = lineage.build_artifact_paths(
        {"command": "reliability", "reliability_command": "check", "output_file": "x.json"},
        payload=None,
    )
    assert "reliability_artifact" in fallback_artifacts


def test_main_dispatch_and_output_table_branches(capsys: pytest.CaptureFixture[str]) -> None:
    cli_main = importlib.import_module("honestroles.cli.main")
    ns = argparse.Namespace(command="reliability", reliability_command="check")
    original_impl = cli_main.handle_reliability_check

    def fake_handler(_args: argparse.Namespace):
        return handlers.CommandResult(payload={"status": "pass"})

    cli_main.handle_reliability_check = fake_handler
    try:
        wrapped = cli_main._handle_reliability_check(argparse.Namespace())
        assert isinstance(wrapped, handlers.CommandResult)
        assert wrapped.payload["status"] == "pass"
        out = cli_main._dispatch(ns)
        assert isinstance(out, handlers.CommandResult)
        assert out.payload["status"] == "pass"
    finally:
        cli_main.handle_reliability_check = original_impl

    output.emit_payload(
        {
            "status": "warn",
            "summary": {"pass": 0, "warn": 1, "fail": 0},
            "checks": [
                {
                    "id": "x",
                    "code": "Y",
                    "severity": "warn",
                    "message": "m",
                    "fix": "f",
                    "fix_snippet": "[x]",
                }
            ],
            "reliability_artifact": "/tmp/r.json",
        },
        "table",
    )
    rendered = capsys.readouterr().out
    assert "snippet" in rendered
    assert "ARTIFACT" in rendered
