from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from honestroles.config.models import FilterStageOptions, PluginManifestItem
from honestroles.domain import JobDataset
from honestroles.plugins.errors import PluginExecutionError, PluginValidationError
from honestroles.plugins.loader import load_plugin_item
from honestroles.plugins.registry import PluginRegistry
from honestroles.plugins.types import FilterStageContext, PluginDefinition, RuntimeExecutionContext
from honestroles.stages import filter_stage


def _dataset() -> JobDataset:
    return JobDataset.from_polars(
        pl.DataFrame(
            {
                "id": ["1"],
                "title": ["x"],
                "company": ["c"],
                "location": ["Remote"],
                "remote": [True],
                "description_text": ["x"],
                "description_html": [None],
                "skills": [["python"]],
                "salary_min": [None],
                "salary_max": [None],
                "apply_url": ["https://x"],
                "posted_at": ["2026-01-01"],
            }
        )
    )


def test_registry_from_manifest(plugin_manifest_path: Path) -> None:
    registry = PluginRegistry.from_manifest(plugin_manifest_path)
    assert registry.list("filter") == ("high_quality_gate",)
    assert registry.list("label") == ("label_note",)
    assert registry.list("rate") == ("rate_bonus",)
    assert "high_quality_gate" in registry.list()


def test_plugin_signature_validation_error() -> None:
    item = PluginManifestItem(
        name="bad",
        kind="filter",
        callable="tests.plugins.fixture_plugins:bad_signature",
    )
    with pytest.raises(PluginValidationError):
        load_plugin_item(item)


def test_plugin_requires_explicit_annotations() -> None:
    item = PluginManifestItem(
        name="untyped",
        kind="filter",
        callable="tests.plugins.fixture_plugins:untyped_plugin",
    )
    with pytest.raises(PluginValidationError):
        load_plugin_item(item)


def test_plugin_bad_type_hints_raise_validation_error() -> None:
    item = PluginManifestItem(
        name="bad_hints",
        kind="filter",
        callable="tests.plugins.fixture_plugins:bad_annotation_plugin",
    )
    with pytest.raises(PluginValidationError):
        load_plugin_item(item)


def test_two_registries_are_isolated() -> None:
    plugin_one = PluginDefinition(
        name="a",
        kind="filter",
        callable_ref="tests.plugins.fixture_plugins:filter_min_quality",
        func=lambda dataset, ctx: dataset,
    )
    plugin_two = PluginDefinition(
        name="b",
        kind="filter",
        callable_ref="tests.plugins.fixture_plugins:filter_min_quality",
        func=lambda dataset, ctx: dataset,
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
    def wrong_type(dataset: JobDataset, ctx: FilterStageContext) -> JobDataset:  # type: ignore[return-value]
        _ = (dataset, ctx)
        return "not-a-dataset"

    plugin = PluginDefinition(
        name="bad_exec",
        kind="filter",
        callable_ref="x:y",
        func=wrong_type,
    )

    with pytest.raises(PluginExecutionError):
        filter_stage(
            _dataset(),
            FilterStageOptions(),
            RuntimeExecutionContext(
                pipeline_config_path=Path("pipeline.toml"),
                plugin_manifest_path=None,
                stage_options={},
            ),
            plugins=(plugin,),
        )


def test_plugin_execution_rejects_non_canonical_dataset() -> None:
    def wrong_shape(dataset: JobDataset, ctx: FilterStageContext) -> JobDataset:
        _ = (dataset, ctx)
        return JobDataset.from_polars(pl.DataFrame({"x": [1]}))

    plugin = PluginDefinition(
        name="bad_shape",
        kind="filter",
        callable_ref="x:y",
        func=wrong_shape,
    )

    with pytest.raises(PluginExecutionError, match="missing canonical fields"):
        filter_stage(
            _dataset(),
            FilterStageOptions(),
            RuntimeExecutionContext(
                pipeline_config_path=Path("pipeline.toml"),
                plugin_manifest_path=None,
                stage_options={},
            ),
            plugins=(plugin,),
        )


@pytest.mark.parametrize("ref", ["x:", ":y"])
def test_loader_rejects_invalid_callable_refs(ref: str) -> None:
    from honestroles.plugins.errors import PluginLoadError

    item = PluginManifestItem(name="bad_ref", kind="filter", callable=ref)
    with pytest.raises(PluginLoadError):
        load_plugin_item(item)


def test_loader_rejects_missing_separator_in_callable_ref() -> None:
    from honestroles.plugins.errors import PluginLoadError

    item = PluginManifestItem(name="bad_ref", kind="filter", callable="invalid")
    with pytest.raises(PluginLoadError):
        load_plugin_item(item)


def test_loader_rejects_missing_attribute() -> None:
    from honestroles.plugins.errors import PluginLoadError

    item = PluginManifestItem(
        name="missing_attr",
        kind="filter",
        callable="tests.plugins.fixture_plugins:not_there",
    )
    with pytest.raises(PluginLoadError):
        load_plugin_item(item)


def test_loader_rejects_non_callable_attribute() -> None:
    from honestroles.plugins.errors import PluginLoadError

    item = PluginManifestItem(
        name="non_callable",
        kind="filter",
        callable="tests.plugins.fixture_plugins:NOT_CALLABLE",
    )
    with pytest.raises(PluginLoadError):
        load_plugin_item(item)


def test_loader_rejects_keyword_only_signature() -> None:
    item = PluginManifestItem(
        name="kw_only",
        kind="filter",
        callable="tests.plugins.fixture_plugins:kw_only_filter",
    )
    with pytest.raises(PluginValidationError):
        load_plugin_item(item)


def test_loader_rejects_wrong_return_annotation() -> None:
    item = PluginManifestItem(
        name="wrong_return",
        kind="filter",
        callable="tests.plugins.fixture_plugins:wrong_return_annotation",
    )
    with pytest.raises(PluginValidationError):
        load_plugin_item(item)


def test_loader_rejects_wrong_context_annotation() -> None:
    item = PluginManifestItem(
        name="wrong_context",
        kind="filter",
        callable="tests.plugins.fixture_plugins:wrong_context_annotation",
    )
    with pytest.raises(PluginValidationError):
        load_plugin_item(item)


def test_loader_freezes_list_and_set_settings() -> None:
    item = PluginManifestItem(
        name="frozen",
        kind="filter",
        callable="tests.plugins.fixture_plugins:filter_min_quality",
        settings={"values": [1, 2], "flags": {3, 4}},
    )
    loaded = load_plugin_item(item)
    assert loaded.settings["values"] == (1, 2)
    assert loaded.settings["flags"] == frozenset({3, 4})
