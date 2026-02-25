from __future__ import annotations

import os
import subprocess
import sys
import tarfile
from pathlib import Path
import zipfile


def _run(cmd: list[str], *, cwd: Path) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        cmd,
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, (
        f"Command failed: {' '.join(cmd)}\n"
        f"stdout:\n{result.stdout}\n"
        f"stderr:\n{result.stderr}"
    )
    return result


def test_built_wheel_contains_packaging_assets_and_cli_works_installed(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    dist_dir = tmp_path / "dist"
    dist_dir.mkdir()

    _run(
        [
            sys.executable,
            "-m",
            "build",
            "--sdist",
            "--wheel",
            "--no-isolation",
            "--outdir",
            str(dist_dir),
        ],
        cwd=repo_root,
    )

    artifacts = sorted(dist_dir.glob("*"))
    assert any(path.suffix == ".whl" for path in artifacts)
    assert any(path.suffixes[-2:] == [".tar", ".gz"] for path in artifacts)

    _run([sys.executable, "-m", "twine", "check", *(str(path) for path in artifacts)], cwd=repo_root)

    wheel_path = next(path for path in artifacts if path.suffix == ".whl")
    sdist_path = next(path for path in artifacts if path.suffixes[-2:] == [".tar", ".gz"])

    with tarfile.open(sdist_path) as sdist:
        names = set(sdist.getnames())
        assert any(name.endswith("/plugin_template/README.md") for name in names)
        assert any(name.endswith("/plugin_template/pyproject.toml") for name in names)
        assert any(name.endswith("/src/honestroles/py.typed") for name in names)

    with zipfile.ZipFile(wheel_path) as wheel:
        names = set(wheel.namelist())
        assert "honestroles/py.typed" in names
        assert "honestroles/_templates/plugin_template/pyproject.toml" in names
        assert "honestroles/_templates/plugin_template/README.md" in names

        metadata_name = next(name for name in names if name.endswith(".dist-info/METADATA"))
        entrypoints_name = next(name for name in names if name.endswith(".dist-info/entry_points.txt"))

        metadata = wheel.read(metadata_name).decode("utf-8")
        assert "Classifier: Typing :: Typed" in metadata
        assert "Classifier: Programming Language :: Python :: 3.12" in metadata
        assert "Project-URL: Changelog" in metadata
        assert "Keywords: " in metadata

        entrypoints = wheel.read(entrypoints_name).decode("utf-8")
        assert "honestroles-scaffold-plugin = honestroles.cli.scaffold_plugin:main" in entrypoints
        assert "honestroles-report-quality = honestroles.cli.report_data_quality:main" in entrypoints

    venv_dir = tmp_path / "venv"
    _run([sys.executable, "-m", "venv", str(venv_dir)], cwd=repo_root)

    bin_dir = "Scripts" if os.name == "nt" else "bin"
    python_bin = venv_dir / bin_dir / "python"
    pip_bin = venv_dir / bin_dir / "pip"
    scaffold_bin = venv_dir / bin_dir / "honestroles-scaffold-plugin"
    report_bin = venv_dir / bin_dir / "honestroles-report-quality"

    _run([str(pip_bin), "install", "--no-deps", str(wheel_path)], cwd=repo_root)

    isolated_cwd = tmp_path / "isolated_cwd"
    isolated_cwd.mkdir()

    _run([str(scaffold_bin), "--help"], cwd=isolated_cwd)
    _run([str(report_bin), "--help"], cwd=isolated_cwd)

    out_dir = tmp_path / "scaffold_out"
    _run(
        [
            str(scaffold_bin),
            "--name",
            "honestroles-plugin-acme",
            "--output-dir",
            str(out_dir),
        ],
        cwd=isolated_cwd,
    )

    generated = out_dir / "honestroles-plugin-acme"
    assert (generated / "pyproject.toml").exists()
    assert (generated / "src" / "honestroles_plugin_acme").exists()

    _run([str(python_bin), "-c", "import honestroles; print(honestroles.__version__)"], cwd=isolated_cwd)
