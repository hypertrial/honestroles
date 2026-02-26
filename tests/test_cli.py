from __future__ import annotations

from pathlib import Path

from honestroles.cli.main import main


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
