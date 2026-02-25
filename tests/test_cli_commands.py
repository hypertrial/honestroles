from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import duckdb


def _env_with_pythonpath(repo_root: Path) -> dict[str, str]:
    env = dict(os.environ)
    env["PYTHONPATH"] = str(repo_root / "src")
    return env


def test_cli_entrypoints_declared_in_pyproject() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    pyproject_text = (repo_root / "pyproject.toml").read_text(encoding="utf-8")
    assert "honestroles-scaffold-plugin = \"honestroles.cli.scaffold_plugin:main\"" in pyproject_text
    assert "honestroles-report-quality = \"honestroles.cli.report_data_quality:main\"" in pyproject_text


def test_scaffold_cli_help_module() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [sys.executable, "-m", "honestroles.cli.scaffold_plugin", "--help"],
        capture_output=True,
        text=True,
        check=False,
        env=_env_with_pythonpath(repo_root),
    )
    assert result.returncode == 0
    assert "Scaffold a new HonestRoles plugin package" in result.stdout


def test_report_quality_cli_help_module() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [sys.executable, "-m", "honestroles.cli.report_data_quality", "--help"],
        capture_output=True,
        text=True,
        check=False,
        env=_env_with_pythonpath(repo_root),
    )
    assert result.returncode == 0
    assert "Build a data quality report" in result.stdout


def test_scaffold_cli_requires_name() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [sys.executable, "-m", "honestroles.cli.scaffold_plugin"],
        capture_output=True,
        text=True,
        check=False,
        env=_env_with_pythonpath(repo_root),
    )
    assert result.returncode != 0
    assert "--name" in result.stderr


def test_report_quality_cli_requires_input() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [sys.executable, "-m", "honestroles.cli.report_data_quality"],
        capture_output=True,
        text=True,
        check=False,
        env=_env_with_pythonpath(repo_root),
    )
    assert result.returncode != 0
    assert "input" in result.stderr


def test_report_quality_cli_duckdb_requires_table_without_query(tmp_path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    db_path = tmp_path / "jobs.duckdb"
    with duckdb.connect(str(db_path)):
        pass

    result = subprocess.run(
        [sys.executable, "-m", "honestroles.cli.report_data_quality", str(db_path)],
        capture_output=True,
        text=True,
        check=False,
        env=_env_with_pythonpath(repo_root),
    )
    assert result.returncode != 0
    assert "--table is required" in result.stderr
