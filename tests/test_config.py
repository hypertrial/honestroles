from __future__ import annotations

from pathlib import Path

import pytest

from honestroles.config import load_pipeline_config, load_plugin_manifest
from honestroles.errors import ConfigValidationError


def test_load_pipeline_config_resolves_paths(tmp_path: Path) -> None:
    parquet_path = tmp_path / "jobs.parquet"
    parquet_path.write_bytes(b"PAR1")
    cfg_text = """
[input]
kind = "parquet"
path = "jobs.parquet"

[output]
path = "out/result.parquet"
""".strip()
    path = tmp_path / "pipeline.toml"
    path.write_text(cfg_text, encoding="utf-8")

    loaded = load_pipeline_config(path)
    assert loaded.input.path == parquet_path.resolve()
    assert loaded.output is not None
    assert loaded.output.path == (tmp_path / "out/result.parquet").resolve()


def test_load_pipeline_config_invalid_toml(tmp_path: Path) -> None:
    path = tmp_path / "broken.toml"
    path.write_text("[input\npath='x'", encoding="utf-8")
    with pytest.raises(ConfigValidationError):
        load_pipeline_config(path)


def test_load_plugin_manifest_requires_array_table(tmp_path: Path) -> None:
    path = tmp_path / "plugins.toml"
    path.write_text("[plugins]\nname='x'", encoding="utf-8")
    with pytest.raises(ConfigValidationError):
        load_plugin_manifest(path)


def test_load_pipeline_config_schema_validation_error_wrapped(tmp_path: Path) -> None:
    path = tmp_path / "pipeline_invalid.toml"
    path.write_text(
        """
[input]
kind = "parquet"
path = "jobs.parquet"

[stages.match]
top_k = 0
""".strip(),
        encoding="utf-8",
    )
    with pytest.raises(ConfigValidationError):
        load_pipeline_config(path)


def test_load_plugin_manifest_schema_validation_error_wrapped(tmp_path: Path) -> None:
    path = tmp_path / "plugins_invalid.toml"
    path.write_text(
        """
[[plugins]]
name = "x"
kind = "invalid_kind"
callable = "mod:fn"
""".strip(),
        encoding="utf-8",
    )
    with pytest.raises(ConfigValidationError):
        load_plugin_manifest(path)
