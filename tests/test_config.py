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


def test_load_pipeline_config_with_aliases_and_quality(tmp_path: Path) -> None:
    parquet_path = tmp_path / "jobs.parquet"
    parquet_path.write_bytes(b"PAR1")
    path = tmp_path / "pipeline_alias_quality.toml"
    path.write_text(
        f"""
[input]
kind = "parquet"
path = "{parquet_path}"

[input.aliases]
location = ["location_raw"]
remote = ["remote_flag"]

[runtime]
fail_fast = true
random_seed = 0

[runtime.quality]
profile = "strict_recruiting"

[runtime.quality.field_weights]
posted_at = 0.6
salary_min = 0.2
""".strip(),
        encoding="utf-8",
    )
    cfg = load_pipeline_config(path)
    assert cfg.input.aliases.location == ("location_raw",)
    assert cfg.input.aliases.remote == ("remote_flag",)
    assert cfg.runtime.quality.profile == "strict_recruiting"
    assert cfg.runtime.quality.field_weights["posted_at"] == 0.6


def test_load_pipeline_config_rejects_invalid_alias_canonical_key(tmp_path: Path) -> None:
    parquet_path = tmp_path / "jobs.parquet"
    parquet_path.write_bytes(b"PAR1")
    path = tmp_path / "pipeline_invalid_alias_key.toml"
    path.write_text(
        f"""
[input]
kind = "parquet"
path = "{parquet_path}"

[input.aliases]
bad_field = ["some_source_column"]
""".strip(),
        encoding="utf-8",
    )
    with pytest.raises(ConfigValidationError):
        load_pipeline_config(path)


def test_load_pipeline_config_rejects_non_positive_custom_quality_weights(
    tmp_path: Path,
) -> None:
    parquet_path = tmp_path / "jobs.parquet"
    parquet_path.write_bytes(b"PAR1")
    path = tmp_path / "pipeline_invalid_quality_weights.toml"
    path.write_text(
        f"""
[input]
kind = "parquet"
path = "{parquet_path}"

[runtime.quality]
profile = "core_fields_weighted"

[runtime.quality.field_weights]
posted_at = 0.0
salary_min = 0.0
""".strip(),
        encoding="utf-8",
    )
    with pytest.raises(ConfigValidationError):
        load_pipeline_config(path)


def test_load_pipeline_config_with_adapter_config(tmp_path: Path) -> None:
    parquet_path = tmp_path / "jobs.parquet"
    parquet_path.write_bytes(b"PAR1")
    path = tmp_path / "pipeline_with_adapter.toml"
    path.write_text(
        f"""
[input]
kind = "parquet"
path = "{parquet_path}"

[input.adapter]
enabled = true
on_error = "null_warn"

[input.adapter.fields.remote]
from = ["remote_flag"]
cast = "bool"
true_values = ["true", "1", "yes"]
false_values = ["false", "0", "no"]
""".strip(),
        encoding="utf-8",
    )
    cfg = load_pipeline_config(path)
    assert cfg.input.adapter.enabled is True
    assert cfg.input.adapter.fields["remote"].from_ == ("remote_flag",)
    assert cfg.input.adapter.fields["remote"].cast == "bool"


def test_load_pipeline_config_rejects_adapter_empty_from_list(tmp_path: Path) -> None:
    parquet_path = tmp_path / "jobs.parquet"
    parquet_path.write_bytes(b"PAR1")
    path = tmp_path / "pipeline_adapter_empty_from.toml"
    path.write_text(
        f"""
[input]
kind = "parquet"
path = "{parquet_path}"

[input.adapter.fields.location]
from = []
cast = "string"
""".strip(),
        encoding="utf-8",
    )
    with pytest.raises(ConfigValidationError):
        load_pipeline_config(path)


def test_load_pipeline_config_rejects_adapter_cast_specific_fields(tmp_path: Path) -> None:
    parquet_path = tmp_path / "jobs.parquet"
    parquet_path.write_bytes(b"PAR1")
    path = tmp_path / "pipeline_adapter_bad_cast_fields.toml"
    path.write_text(
        f"""
[input]
kind = "parquet"
path = "{parquet_path}"

[input.adapter.fields.salary_min]
from = ["salary_text"]
cast = "float"
true_values = ["true"]
""".strip(),
        encoding="utf-8",
    )
    with pytest.raises(ConfigValidationError):
        load_pipeline_config(path)


def test_load_pipeline_config_rejects_invalid_adapter_canonical_key(tmp_path: Path) -> None:
    parquet_path = tmp_path / "jobs.parquet"
    parquet_path.write_bytes(b"PAR1")
    path = tmp_path / "pipeline_adapter_bad_key.toml"
    path.write_text(
        f"""
[input]
kind = "parquet"
path = "{parquet_path}"

[input.adapter.fields.bad_field]
from = ["whatever"]
cast = "string"
""".strip(),
        encoding="utf-8",
    )
    with pytest.raises(ConfigValidationError):
        load_pipeline_config(path)
