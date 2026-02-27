from __future__ import annotations

import json
import importlib
from pathlib import Path

import pytest

from honestroles.errors import ConfigValidationError, HonestRolesError, StageExecutionError
from honestroles.cli.main import main


def _cli_main_module():
    return importlib.import_module("honestroles.cli.main")


def test_cli_run(pipeline_config_path: Path, plugin_manifest_path: Path) -> None:
    code = main(
        [
            "run",
            "--pipeline-config",
            str(pipeline_config_path),
            "--plugins",
            str(plugin_manifest_path),
        ]
    )
    assert code == 0


def test_cli_plugins_validate(plugin_manifest_path: Path) -> None:
    code = main(["plugins", "validate", "--manifest", str(plugin_manifest_path)])
    assert code == 0


def test_cli_config_validate(pipeline_config_path: Path) -> None:
    code = main(["config", "validate", "--pipeline", str(pipeline_config_path)])
    assert code == 0


def test_cli_report_quality(pipeline_config_path: Path, plugin_manifest_path: Path) -> None:
    code = main(
        [
            "report-quality",
            "--pipeline-config",
            str(pipeline_config_path),
            "--plugins",
            str(plugin_manifest_path),
        ]
    )
    assert code == 0


def test_cli_report_quality_includes_profile_metadata(
    pipeline_config_path: Path,
    plugin_manifest_path: Path,
    capsys,
) -> None:
    code = main(
        [
            "report-quality",
            "--pipeline-config",
            str(pipeline_config_path),
            "--plugins",
            str(plugin_manifest_path),
        ]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert "profile" in payload
    assert "weighted_null_percent" in payload
    assert "effective_weights" in payload
    assert payload["profile"] == "core_fields_weighted"


def test_cli_report_quality_score_uses_weighted_profile(tmp_path: Path, capsys) -> None:
    parquet_path = tmp_path / "jobs.parquet"
    import polars as pl

    pl.DataFrame({"a": [1, None], "b": [None, None], "title": ["x", "y"], "description_text": ["d", "d"]}).write_parquet(parquet_path)

    pipeline_path = tmp_path / "pipeline.toml"
    pipeline_path.write_text(
        f"""
[input]
kind = "parquet"
path = "{parquet_path}"

[stages.clean]
enabled = false

[stages.filter]
enabled = false

[stages.label]
enabled = false

[stages.rate]
enabled = false

[stages.match]
enabled = false

[runtime.quality]
profile = "equal_weight_all"
""".strip(),
        encoding="utf-8",
    )

    code = main(["report-quality", "--pipeline-config", str(pipeline_path)])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["profile"] == "equal_weight_all"
    # equal weight across output columns includes a (50 null) and b (100 null), score must be < 100
    assert payload["score_percent"] < 100.0


def test_cli_exit_code_for_bad_config(tmp_path: Path) -> None:
    bad_path = tmp_path / "missing.toml"
    code = main(["config", "validate", "--pipeline", str(bad_path)])
    assert code == 2


def test_cli_run_bad_config_returns_config_exit(tmp_path: Path) -> None:
    bad_path = tmp_path / "missing_pipeline.toml"
    code = main(["run", "--pipeline-config", str(bad_path)])
    assert code == 2


def test_cli_run_bad_plugin_manifest_returns_plugin_exit(
    pipeline_config_path: Path,
    tmp_path: Path,
) -> None:
    manifest = tmp_path / "bad_plugins.toml"
    manifest.write_text(
        """
[[plugins]]
name = "bad"
kind = "label"
callable = "nope.module:missing"
""".strip(),
        encoding="utf-8",
    )
    code = main(
        [
            "run",
            "--pipeline-config",
            str(pipeline_config_path),
            "--plugins",
            str(manifest),
        ]
    )
    assert code == 3


def test_cli_scaffold_plugin(tmp_path: Path) -> None:
    code = main(
        [
            "scaffold-plugin",
            "--name",
            "my-demo-plugin",
            "--output-dir",
            str(tmp_path),
        ]
    )
    assert code == 0
    assert (tmp_path / "my-demo-plugin").exists()


def test_cli_scaffold_plugin_target_exists_returns_config_error(tmp_path: Path) -> None:
    target = tmp_path / "my-demo-plugin"
    target.mkdir()
    code = main(
        [
            "scaffold-plugin",
            "--name",
            "my-demo-plugin",
            "--output-dir",
            str(tmp_path),
        ]
    )
    assert code == 2


def test_cli_scaffold_plugin_invalid_name_returns_config_error(tmp_path: Path) -> None:
    code = main(
        [
            "scaffold-plugin",
            "--name",
            "bad!name",
            "--output-dir",
            str(tmp_path),
        ]
    )
    assert code == 2


def test_cli_scaffold_plugin_numeric_name_returns_config_error(tmp_path: Path) -> None:
    code = main(
        [
            "scaffold-plugin",
            "--name",
            "1plugin",
            "--output-dir",
            str(tmp_path),
        ]
    )
    assert code == 2


def test_cli_template_resolution_packaged_fallback(monkeypatch) -> None:
    cli_main = _cli_main_module()

    repo_template = Path(cli_main.__file__).resolve().parents[3] / "plugin_template"
    packaged_template = (
        Path(cli_main.__file__).resolve().parents[1] / "_templates" / "plugin_template"
    )
    original_exists = Path.exists

    def fake_exists(self: Path) -> bool:
        if self == repo_template:
            return False
        if self == packaged_template:
            return True
        return original_exists(self)

    monkeypatch.setattr(Path, "exists", fake_exists)
    resolved = cli_main._resolve_plugin_template_root()
    assert resolved == packaged_template


def test_cli_template_resolution_hard_failure(monkeypatch) -> None:
    cli_main = _cli_main_module()

    repo_template = Path(cli_main.__file__).resolve().parents[3] / "plugin_template"
    packaged_template = (
        Path(cli_main.__file__).resolve().parents[1] / "_templates" / "plugin_template"
    )
    original_exists = Path.exists

    def fake_exists(self: Path) -> bool:
        if self in {repo_template, packaged_template}:
            return False
        return original_exists(self)

    monkeypatch.setattr(Path, "exists", fake_exists)
    with pytest.raises(ConfigValidationError):
        cli_main._resolve_plugin_template_root()


def test_cli_stage_execution_error_mapping(monkeypatch) -> None:
    cli_main = _cli_main_module()

    def fail(_args):
        raise StageExecutionError("filter", "boom")

    monkeypatch.setattr(cli_main, "_handle_run", fail)
    code = cli_main.main(["run", "--pipeline-config", "x.toml"])
    assert code == 4


def test_cli_generic_honestroles_error_mapping(monkeypatch) -> None:
    cli_main = _cli_main_module()

    def fail(_args):
        raise HonestRolesError("oops")

    monkeypatch.setattr(cli_main, "_handle_run", fail)
    code = cli_main.main(["run", "--pipeline-config", "x.toml"])
    assert code == 1


def test_cli_parser_fallback_unhandled_command(monkeypatch) -> None:
    import argparse
    cli_main = _cli_main_module()

    class FakeParser:
        def parse_args(self, _argv):
            return argparse.Namespace(command="unknown")

        def error(self, _msg):
            return None

    monkeypatch.setattr(cli_main, "build_parser", lambda: FakeParser())
    code = cli_main.main([])
    assert code == 1
