from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from honestroles.config.models import (
    FilterStageOptions,
    InputConfig,
    OutputConfig,
    PluginManifestConfig,
    PluginManifestItem,
    PluginSpecConfig,
    RateStageOptions,
)


def test_input_config_path_passthrough_and_type_error() -> None:
    cfg = InputConfig(path=Path("jobs.parquet"))
    assert cfg.path == Path("jobs.parquet")

    with pytest.raises(TypeError, match="input.path must be a path-like string"):
        InputConfig(path=123)  # type: ignore[arg-type]


def test_output_config_path_passthrough_and_type_error() -> None:
    cfg = OutputConfig(path=Path("out.parquet"))
    assert cfg.path == Path("out.parquet")

    with pytest.raises(TypeError, match="output.path must be a path-like string"):
        OutputConfig(path=123)  # type: ignore[arg-type]


def test_filter_required_keywords_passthrough_tuple() -> None:
    options = FilterStageOptions(required_keywords=("python", "sql"))
    assert options.required_keywords == ("python", "sql")


def test_rate_stage_negative_weights_raise() -> None:
    with pytest.raises(ValidationError):
        RateStageOptions(completeness_weight=-1.0)


def test_plugin_spec_capabilities_coercion_paths() -> None:
    list_cfg = PluginSpecConfig(capabilities=["a", "b"])
    assert list_cfg.capabilities == ("a", "b")

    tuple_cfg = PluginSpecConfig(capabilities=("x",))
    assert tuple_cfg.capabilities == ("x",)


def test_plugin_manifest_item_blank_name_raises() -> None:
    with pytest.raises(ValidationError):
        PluginManifestItem(name="   ", kind="filter", callable="x:y")


def test_plugin_manifest_plugins_tuple_passthrough_and_duplicate_rejected() -> None:
    item = PluginManifestItem(name="a", kind="filter", callable="x:y")
    cfg = PluginManifestConfig(plugins=(item,))
    assert cfg.plugins == (item,)

    with pytest.raises(ValidationError):
        PluginManifestConfig(
            plugins=(
                PluginManifestItem(name="dup", kind="filter", callable="x:y"),
                PluginManifestItem(name="dup", kind="filter", callable="z:w"),
            )
        )
