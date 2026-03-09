from __future__ import annotations

import argparse
import builtins
from datetime import datetime, timedelta, timezone
import importlib
import json
from pathlib import Path

import polars as pl
import pytest

from honestroles.cli import handlers, lineage, output
from honestroles.errors import ConfigValidationError


def _pipeline_text(input_path: Path, output_path: Path | None = None) -> str:
    sections = [
        "[input]",
        'kind = "parquet"',
        f'path = "{input_path}"',
        "",
    ]
    if output_path is not None:
        sections.extend(["[output]", f'path = "{output_path}"', ""])

    sections.extend(
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
            "top_k = 10",
            "",
            "[runtime]",
            "fail_fast = true",
            "random_seed = 1",
        ]
    )
    return "\n".join(sections).strip() + "\n"


def _write_pipeline(
    tmp_path: Path,
    *,
    input_path: Path,
    output_path: Path | None,
    filename: str = "pipeline.toml",
) -> Path:
    path = tmp_path / filename
    path.write_text(_pipeline_text(input_path, output_path), encoding="utf-8")
    return path


def _check_by_id(payload: dict[str, object], check_id: str) -> dict[str, str]:
    checks = payload["checks"]
    assert isinstance(checks, list)
    for item in checks:
        assert isinstance(item, dict)
        if item.get("id") == check_id:
            return item
    raise AssertionError(f"check not found: {check_id}")


def test_build_pipeline_toml_includes_adapter_fragment(tmp_path: Path) -> None:
    text = handlers._build_pipeline_toml(
        input_path=tmp_path / "in.parquet",
        output_path=tmp_path / "out.parquet",
        adapter_fragment="[input.adapter]\nsource = \"title_text\"",
    )
    assert "[input.adapter]" in text
    assert "source = \"title_text\"" in text


def test_build_pipeline_toml_without_adapter_fragment(tmp_path: Path) -> None:
    text = handlers._build_pipeline_toml(
        input_path=tmp_path / "in.parquet",
        output_path=tmp_path / "out.parquet",
        adapter_fragment=None,
    )
    assert "[input.adapter]" not in text


def test_handle_init_missing_input_raises(tmp_path: Path) -> None:
    args = argparse.Namespace(
        input_parquet=str(tmp_path / "missing.parquet"),
        pipeline_config=str(tmp_path / "pipeline.toml"),
        plugins_manifest=str(tmp_path / "plugins.toml"),
        output_parquet=str(tmp_path / "out.parquet"),
        sample_rows=1,
        force=False,
    )
    with pytest.raises(ConfigValidationError, match="input parquet does not exist"):
        handlers.handle_init(args)


def test_handle_init_rejects_non_positive_sample_rows(sample_parquet: Path, tmp_path: Path) -> None:
    args = argparse.Namespace(
        input_parquet=str(sample_parquet),
        pipeline_config=str(tmp_path / "pipeline.toml"),
        plugins_manifest=str(tmp_path / "plugins.toml"),
        output_parquet=str(tmp_path / "out.parquet"),
        sample_rows=0,
        force=False,
    )
    with pytest.raises(ConfigValidationError, match="sample-rows must be >= 1"):
        handlers.handle_init(args)


def test_nearest_existing_parent_walks_up(tmp_path: Path) -> None:
    missing = tmp_path / "a" / "b" / "c"
    assert handlers._nearest_existing_parent(missing) == tmp_path


def test_doctor_status_summary_warn() -> None:
    status, summary = handlers._doctor_status_summary(
        [{"status": "warn"}, {"status": "pass"}]
    )
    assert status == "warn"
    assert summary == {"pass": 1, "warn": 1, "fail": 0}


def test_handle_doctor_reports_missing_imports(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    pipeline = _write_pipeline(
        tmp_path,
        input_path=tmp_path / "missing.parquet",
        output_path=tmp_path / "out.parquet",
        filename="doctor_missing_imports.toml",
    )

    original_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name in {"polars", "pydantic"}:
            raise ImportError(name)
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    result = handlers.handle_doctor(
        argparse.Namespace(
            pipeline_config=str(pipeline),
            plugin_manifest=None,
            sample_rows=10,
        )
    )
    required_imports = _check_by_id(result.payload, "required_imports")
    assert required_imports["status"] == "fail"
    assert result.exit_code == 1


def test_handle_doctor_plugin_manifest_validation_failure(
    sample_parquet: Path,
    tmp_path: Path,
) -> None:
    pipeline = _write_pipeline(
        tmp_path,
        input_path=sample_parquet,
        output_path=tmp_path / "out.parquet",
        filename="doctor_plugin.toml",
    )
    plugin_manifest = tmp_path / "plugins_bad.toml"
    plugin_manifest.write_text("[[plugins]\n", encoding="utf-8")

    result = handlers.handle_doctor(
        argparse.Namespace(
            pipeline_config=str(pipeline),
            plugin_manifest=str(plugin_manifest),
            sample_rows=10,
        )
    )

    plugin_check = _check_by_id(result.payload, "plugin_manifest")
    assert plugin_check["status"] == "fail"
    assert result.exit_code == 2


def test_handle_doctor_missing_input_and_no_output_warn(tmp_path: Path) -> None:
    missing_input = tmp_path / "missing.parquet"
    pipeline = _write_pipeline(
        tmp_path,
        input_path=missing_input,
        output_path=None,
        filename="doctor_missing_input.toml",
    )

    result = handlers.handle_doctor(
        argparse.Namespace(
            pipeline_config=str(pipeline),
            plugin_manifest=None,
            sample_rows=10,
        )
    )

    input_check = _check_by_id(result.payload, "input_exists")
    assert input_check["status"] == "fail"
    output_check = _check_by_id(result.payload, "output_path")
    assert output_check["status"] == "warn"


def test_handle_doctor_canonical_contract_failure(tmp_path: Path) -> None:
    input_parquet = tmp_path / "jobs_bad_schema.parquet"
    pl.DataFrame({"id": ["1"], "title": ["x"], "company": ["y"], "description_text": ["z"]}).write_parquet(
        input_parquet
    )
    pipeline = _write_pipeline(
        tmp_path,
        input_path=input_parquet,
        output_path=tmp_path / "out.parquet",
        filename="doctor_bad_contract.toml",
    )
    original_validate = handlers.validate_source_data_contract

    def fail_validate(_df: pl.DataFrame) -> pl.DataFrame:
        raise ConfigValidationError("bad canonical")

    handlers.validate_source_data_contract = fail_validate
    try:
        result = handlers.handle_doctor(
            argparse.Namespace(
                pipeline_config=str(pipeline),
                plugin_manifest=None,
                sample_rows=10,
            )
        )
    finally:
        handlers.validate_source_data_contract = original_validate

    canonical = _check_by_id(result.payload, "canonical_contract")
    assert canonical["status"] == "fail"


def test_handle_doctor_content_readiness_empty_sample(tmp_path: Path) -> None:
    input_parquet = tmp_path / "jobs_empty.parquet"
    df = pl.DataFrame(
        {
            "id": ["1"],
            "title": ["x"],
            "company": ["y"],
            "description_text": ["z"],
        }
    ).head(0)
    df.write_parquet(input_parquet)
    pipeline = _write_pipeline(
        tmp_path,
        input_path=input_parquet,
        output_path=tmp_path / "out.parquet",
        filename="doctor_empty.toml",
    )

    result = handlers.handle_doctor(
        argparse.Namespace(
            pipeline_config=str(pipeline),
            plugin_manifest=None,
            sample_rows=10,
        )
    )
    readiness = _check_by_id(result.payload, "content_readiness")
    assert readiness["status"] == "warn"
    assert "empty" in readiness["message"]


def test_handle_doctor_content_readiness_null_title(tmp_path: Path) -> None:
    input_parquet = tmp_path / "jobs_null_title.parquet"
    pl.DataFrame(
        {
            "id": ["1", "2"],
            "title": [None, None],
            "company": ["A", "B"],
            "description_text": ["d1", "d2"],
        }
    ).write_parquet(input_parquet)
    pipeline = _write_pipeline(
        tmp_path,
        input_path=input_parquet,
        output_path=tmp_path / "out.parquet",
        filename="doctor_null_title.toml",
    )

    result = handlers.handle_doctor(
        argparse.Namespace(
            pipeline_config=str(pipeline),
            plugin_manifest=None,
            sample_rows=10,
        )
    )
    readiness = _check_by_id(result.payload, "content_readiness")
    assert readiness["status"] == "warn"
    assert "null title" in readiness["message"]


def test_handle_doctor_output_parent_missing_warn(sample_parquet: Path, tmp_path: Path) -> None:
    output_path = tmp_path / "nested" / "dir" / "out.parquet"
    pipeline = _write_pipeline(
        tmp_path,
        input_path=sample_parquet,
        output_path=output_path,
        filename="doctor_missing_parent.toml",
    )

    result = handlers.handle_doctor(
        argparse.Namespace(
            pipeline_config=str(pipeline),
            plugin_manifest=None,
            sample_rows=10,
        )
    )
    output_check = _check_by_id(result.payload, "output_path")
    assert output_check["status"] == "warn"
    assert "does not exist" in output_check["message"]


def test_handle_runs_list_limit_validation() -> None:
    with pytest.raises(ConfigValidationError, match="limit must be >= 1"):
        handlers.handle_runs_list(argparse.Namespace(limit=0, status=None))


def test_lineage_hash_input_path_directory_without_manifest(tmp_path: Path) -> None:
    digest = lineage._hash_input_path(tmp_path)
    assert digest == lineage._hash_path_reference(tmp_path)


def test_lineage_compute_hashes_run_invalid_pipeline(tmp_path: Path) -> None:
    bad_pipeline = tmp_path / "bad.toml"
    bad_pipeline.write_text("[input\n", encoding="utf-8")
    input_hash, input_hashes, config_hash = lineage.compute_hashes(
        {"command": "run", "pipeline_config": str(bad_pipeline)}
    )
    assert input_hash is None
    assert input_hashes == {}
    assert config_hash


def test_lineage_compute_hashes_run_with_missing_input_path(tmp_path: Path) -> None:
    pipeline = _write_pipeline(
        tmp_path,
        input_path=tmp_path / "missing.parquet",
        output_path=tmp_path / "out.parquet",
        filename="lineage_missing_input.toml",
    )
    input_hash, input_hashes, config_hash = lineage.compute_hashes(
        {"command": "run", "pipeline_config": str(pipeline)}
    )
    assert input_hash is None
    assert input_hashes == {}
    assert config_hash


def test_lineage_compute_hashes_eda_generate_without_existing_input() -> None:
    input_hash, input_hashes, config_hash = lineage.compute_hashes(
        {
            "command": "eda",
            "eda_command": "generate",
            "input_parquet": "missing.parquet",
            "rules_file": None,
        }
    )
    assert input_hash is None
    assert input_hashes == {}
    assert config_hash


def test_lineage_compute_hashes_eda_diff_baseline_only(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline"
    baseline.mkdir()
    input_hash, input_hashes, config_hash = lineage.compute_hashes(
        {
            "command": "eda",
            "eda_command": "diff",
            "candidate_dir": "missing-candidate",
            "baseline_dir": str(baseline),
            "rules_file": None,
        }
    )
    assert input_hash is None
    assert "baseline_dir" in input_hashes
    assert config_hash


def test_lineage_compute_hashes_unknown_command_uses_fingerprint() -> None:
    input_hash, input_hashes, config_hash = lineage.compute_hashes({"command": "doctor"})
    assert input_hash is None
    assert input_hashes == {}
    assert config_hash


def test_lineage_compute_hashes_unknown_eda_subcommand_uses_fingerprint() -> None:
    input_hash, input_hashes, config_hash = lineage.compute_hashes(
        {"command": "eda", "eda_command": "dashboard"}
    )
    assert input_hash is None
    assert input_hashes == {}
    assert config_hash


def test_lineage_list_records_empty_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    assert lineage.list_records(limit=5, status=None) == []


def test_lineage_list_records_status_filter_and_limit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    root = tmp_path / ".honestroles" / "runs"
    older = root / "older"
    newer = root / "newer"
    older.mkdir(parents=True)
    newer.mkdir(parents=True)
    older_payload = {
        "run_id": "older",
        "status": "fail",
        "command": "run",
        "started_at_utc": "2026-01-01T00:00:00+00:00",
    }
    newer_payload = {
        "run_id": "newer",
        "status": "pass",
        "command": "run",
        "started_at_utc": "2026-02-01T00:00:00+00:00",
    }
    (older / "run.json").write_text(json.dumps(older_payload), encoding="utf-8")
    (newer / "run.json").write_text(json.dumps(newer_payload), encoding="utf-8")

    filtered = lineage.list_records(limit=5, status="pass")
    assert [row["run_id"] for row in filtered] == ["newer"]

    all_records = lineage.list_records(limit=1, status=None)
    assert [row["run_id"] for row in all_records] == ["newer"]


def test_output_helpers_and_emit_payload_table(capsys: pytest.CaptureFixture[str]) -> None:
    assert output._stringify(None) == "-"
    assert output._stringify(12) == "12"
    assert output._stringify({"b": 2, "a": 1}) == '{"a": 1, "b": 2}'

    output.emit_payload(
        {
            "status": "warn",
            "summary": {"pass": 1, "warn": 1, "fail": 0},
            "checks": [
                {"id": "alpha", "status": "pass", "message": "ok", "fix": "-"},
                "skip-me",
                {"id": "beta", "status": "warn", "message": "bad", "fix": "do this"},
            ],
        },
        "table",
    )
    rendered = capsys.readouterr().out
    assert "SUMMARY pass=1 warn=1 fail=0" in rendered
    assert "beta" in rendered
    assert "do this" in rendered

    output.emit_payload({"status": "ok", "meta": {"x": 1}, "value": True}, "table")
    generic = capsys.readouterr().out
    assert "meta" in generic
    assert "{\"x\": 1}" in generic


def test_output_doctor_table_handles_non_list_and_empty_fix(capsys: pytest.CaptureFixture[str]) -> None:
    output._print_doctor_table({"summary": {"pass": 0, "warn": 0, "fail": 0}, "checks": "not-a-list"})
    rendered = capsys.readouterr().out
    assert "SUMMARY" in rendered

    output._print_doctor_table(
        {
            "summary": {"pass": 1, "warn": 0, "fail": 0},
            "checks": [{"id": "alpha", "status": "pass", "message": "ok", "fix": ""}],
        }
    )
    rendered = capsys.readouterr().out
    assert "alpha" in rendered
    assert "fix" not in rendered


def test_output_runs_table_variants_and_errors(capsys: pytest.CaptureFixture[str]) -> None:
    output._print_runs_table({"runs": {}})
    header_only = capsys.readouterr().out
    assert "RUN_ID" in header_only

    output.emit_payload(
        {
            "status": "ok",
            "runs": [
                {"run_id": "abc", "status": "pass", "command": "run", "started_at_utc": "x"},
                "skip",
            ],
        },
        "table",
    )
    runs_table = capsys.readouterr().out
    assert "abc" in runs_table

    output.emit_error(ConfigValidationError("boom"), "json")
    assert "boom" in capsys.readouterr().err


def test_main_ignores_lineage_write_failures(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    module = importlib.import_module("honestroles.cli.main")

    def fake_dispatch(_args: argparse.Namespace) -> handlers.CommandResult:
        return handlers.CommandResult(payload={"status": "pass"})

    def fail_write(_record: dict[str, object]) -> None:
        raise RuntimeError("no write")

    monkeypatch.setattr(module, "_dispatch", fake_dispatch)
    monkeypatch.setattr(module, "write_record", fail_write)
    monkeypatch.chdir(tmp_path)

    code = module.main(["run", "--pipeline-config", str(tmp_path / "missing.toml")])
    assert code == 0


def test_lineage_create_record_preserves_error_payload() -> None:
    started = datetime.now(timezone.utc)
    finished = started + timedelta(milliseconds=25)
    record = lineage.create_record(
        args={"command": "run"},
        exit_code=2,
        started_at=started,
        finished_at=finished,
        payload=None,
        error={"type": "ConfigValidationError", "message": "bad"},
    )
    assert record["status"] == "fail"
    assert record["error"] == {"type": "ConfigValidationError", "message": "bad"}
