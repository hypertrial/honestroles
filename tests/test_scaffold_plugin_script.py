from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def test_scaffold_plugin_script_generates_package(tmp_path) -> None:
    script = Path(__file__).resolve().parents[1] / "scripts" / "scaffold_plugin.py"
    output_dir = tmp_path / "out"
    output_dir.mkdir()

    result = subprocess.run(
        [
            sys.executable,
            str(script),
            "--name",
            "honestroles-plugin-acme",
            "--output-dir",
            str(output_dir),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr

    dest = output_dir / "honestroles-plugin-acme"
    assert dest.exists()
    assert (dest / "src" / "honestroles_plugin_acme").exists()

    pyproject = (dest / "pyproject.toml").read_text(encoding="utf-8")
    assert "name = \"honestroles-plugin-acme\"" in pyproject
    assert "honestroles_plugin_acme" in pyproject

    plugins_py = (dest / "src" / "honestroles_plugin_acme" / "plugins.py").read_text(
        encoding="utf-8"
    )
    assert "acme_only_remote" in plugins_py
    assert "acme_add_source_group" in plugins_py
    assert "acme_add_priority_rating" in plugins_py


def test_scaffold_plugin_script_refuses_overwrite_without_force(tmp_path) -> None:
    script = Path(__file__).resolve().parents[1] / "scripts" / "scaffold_plugin.py"
    output_dir = tmp_path / "out"
    output_dir.mkdir()
    target = output_dir / "honestroles-plugin-existing"
    target.mkdir()

    result = subprocess.run(
        [
            sys.executable,
            str(script),
            "--name",
            "honestroles-plugin-existing",
            "--output-dir",
            str(output_dir),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode != 0
    assert "Destination already exists" in result.stderr
