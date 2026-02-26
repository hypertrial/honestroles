from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from honestroles.plugins.errors import PluginExecutionError, PluginValidationError
from honestroles.plugins.loader import load_plugin_item
from honestroles.plugins.registry import PluginRegistry
from honestroles.plugins.types import FilterPluginContext, LoadedPlugin


def test_registry_from_manifest(plugin_manifest_path: Path) -> None:
    registry = PluginRegistry.from_manifest(plugin_manifest_path)
    assert registry.list("filter") == ("high_quality_gate",)
    assert registry.list("label") == ("label_note",)
    assert registry.list("rate") == ("rate_bonus",)


def test_plugin_signature_validation_error() -> None:
    from honestroles.config.models import PluginManifestItem

    item = PluginManifestItem(
        name="bad",
        kind="filter",
        callable="tests.plugins.fixture_plugins:bad_signature",
    )
    with pytest.raises(PluginValidationError):
        load_plugin_item(item)


def test_plugin_requires_explicit_annotations() -> None:
    from honestroles.config.models import PluginManifestItem

    item = PluginManifestItem(
        name="untyped",
        kind="filter",
        callable="tests.plugins.fixture_plugins:untyped_plugin",
    )
    with pytest.raises(PluginValidationError):
        load_plugin_item(item)


def test_plugin_bad_type_hints_raise_validation_error() -> None:
    from honestroles.config.models import PluginManifestItem

    item = PluginManifestItem(
        name="bad_hints",
        kind="filter",
        callable="tests.plugins.fixture_plugins:bad_annotation_plugin",
    )
    with pytest.raises(PluginValidationError):
        load_plugin_item(item)


def test_two_registries_are_isolated() -> None:
    plugin_one = LoadedPlugin(
        name="a",
        kind="filter",
        callable_ref="tests.plugins.fixture_plugins:filter_min_quality",
        func=lambda df, ctx: df,
    )
    plugin_two = LoadedPlugin(
        name="b",
        kind="filter",
        callable_ref="tests.plugins.fixture_plugins:filter_min_quality",
        func=lambda df, ctx: df,
    )
    reg_a = PluginRegistry.from_plugins((plugin_one,))
    reg_b = PluginRegistry.from_plugins((plugin_two,))
    assert reg_a.list("filter") == ("a",)
    assert reg_b.list("filter") == ("b",)


def test_registry_settings_are_immutable(plugin_manifest_path: Path) -> None:
    registry = PluginRegistry.from_manifest(plugin_manifest_path)
    plugin = registry.plugins_for_kind("filter")[0]
    with pytest.raises(TypeError):
        plugin.settings["min_quality"] = 0.9  # type: ignore[index]


def test_plugin_execution_type_failure() -> None:
    def wrong_type(df: pl.DataFrame, ctx: FilterPluginContext) -> pl.DataFrame:  # type: ignore[return-value]
        _ = (df, ctx)
        return "not-a-dataframe"

    plugin = LoadedPlugin(
        name="bad_exec",
        kind="filter",
        callable_ref="x:y",
        func=wrong_type,
    )
    from honestroles.config.models import FilterStageOptions
    from honestroles.plugins.types import RuntimeExecutionContext
    from honestroles.stages import filter_stage

    df = pl.DataFrame({"title": ["x"], "remote": [True], "description_text": ["x"]})
    with pytest.raises(PluginExecutionError):
        filter_stage(
            df,
            FilterStageOptions(),
            RuntimeExecutionContext(
                pipeline_config_path=Path("pipeline.toml"),
                plugin_manifest_path=None,
                stage_options={},
            ),
            plugins=(plugin,),
        )
