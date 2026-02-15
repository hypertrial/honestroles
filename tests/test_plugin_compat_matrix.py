from __future__ import annotations

import pandas as pd
import pytest

import honestroles.plugins as plugins_module
from honestroles.plugins import (
    apply_filter_plugins,
    PluginExport,
    PluginSpec,
    list_filter_plugins,
    list_label_plugins,
    list_rate_plugins,
    load_plugins_from_entrypoints,
    register_filter_plugin,
)


def test_register_filter_plugin_rejects_major_mismatch() -> None:
    with pytest.raises(ValueError, match="unsupported api_version"):
        register_filter_plugin(
            "bad_major",
            lambda df: pd.Series([True] * len(df), index=df.index),
            spec=PluginSpec(api_version="2.0"),
        )


def test_register_filter_plugin_rejects_newer_minor() -> None:
    with pytest.raises(ValueError, match="unsupported api_version"):
        register_filter_plugin(
            "bad_minor",
            lambda df: pd.Series([True] * len(df), index=df.index),
            spec=PluginSpec(api_version="1.1"),
        )


def test_register_filter_plugin_accepts_equivalent_single_part_version() -> None:
    register_filter_plugin(
        "ok_single_part",
        lambda df: pd.Series([True] * len(df), index=df.index),
        spec={"api_version": "1", "plugin_version": "0.1.0", "capabilities": "filter"},
    )
    assert "ok_single_part" in list_filter_plugins()


class _FakeEntryPoint:
    def __init__(self, name: str, loaded: object) -> None:
        self.name = name
        self._loaded = loaded

    def load(self) -> object:
        if isinstance(self._loaded, Exception):
            raise self._loaded
        return self._loaded


class _FakeEntryPoints:
    def __init__(self, mapping: dict[str, list[_FakeEntryPoint]]) -> None:
        self._mapping = mapping

    def select(self, *, group: str):  # type: ignore[no-untyped-def]
        return self._mapping.get(group, [])


def test_load_plugins_from_entrypoints_registers_callable(monkeypatch) -> None:
    def _filter(df: pd.DataFrame) -> pd.Series:
        return pd.Series([True] * len(df), index=df.index)

    fake = _FakeEntryPoints({"honestroles.filter_plugins": [_FakeEntryPoint("ep_filter", _filter)]})
    monkeypatch.setattr(plugins_module, "entry_points", lambda: fake)
    loaded = load_plugins_from_entrypoints()
    assert loaded["filter"] == ["ep_filter"]
    assert "ep_filter" in list_filter_plugins()


def test_load_plugins_from_entrypoints_supports_legacy_mapping_api(monkeypatch) -> None:
    def _filter(df: pd.DataFrame) -> pd.Series:
        return pd.Series([True] * len(df), index=df.index)

    monkeypatch.setattr(
        plugins_module,
        "entry_points",
        lambda: {"honestroles.filter_plugins": [_FakeEntryPoint("legacy_filter", _filter)]},
    )
    loaded = load_plugins_from_entrypoints()
    assert loaded["filter"] == ["legacy_filter"]


def test_load_plugins_from_entrypoints_registers_label_and_rate_exports(monkeypatch) -> None:
    def _label(df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(tag="x")

    def _rate(df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(score=1.0)

    fake = _FakeEntryPoints(
        {
            "honestroles.label_plugins": [
                _FakeEntryPoint(
                    "ep_label",
                    PluginExport(kind="label", plugin=_label, name="ep_label"),
                )
            ],
            "honestroles.rate_plugins": [
                _FakeEntryPoint(
                    "ep_rate",
                    PluginExport(kind="rate", plugin=_rate, name="ep_rate"),
                )
            ],
        }
    )
    monkeypatch.setattr(plugins_module, "entry_points", lambda: fake)
    loaded = load_plugins_from_entrypoints()
    assert loaded["label"] == ["ep_label"]
    assert loaded["rate"] == ["ep_rate"]
    assert "ep_label" in list_label_plugins()
    assert "ep_rate" in list_rate_plugins()


def test_load_plugins_from_entrypoints_strict_mode_raises_on_incompatible_export(monkeypatch) -> None:
    def _filter(df: pd.DataFrame) -> pd.Series:
        return pd.Series([True] * len(df), index=df.index)

    export = PluginExport(
        kind="filter",
        name="bad_export",
        plugin=_filter,
        spec=PluginSpec(api_version="2.0"),
    )
    fake = _FakeEntryPoints({"honestroles.filter_plugins": [_FakeEntryPoint("bad_export", export)]})
    monkeypatch.setattr(plugins_module, "entry_points", lambda: fake)
    with pytest.raises(RuntimeError, match="Failed to load plugin entrypoint"):
        load_plugins_from_entrypoints(strict=True)


def test_load_plugins_from_entrypoints_non_strict_logs_warning_on_invalid_object(
    monkeypatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    fake = _FakeEntryPoints({"honestroles.filter_plugins": [_FakeEntryPoint("bad_object", object())]})
    monkeypatch.setattr(plugins_module, "entry_points", lambda: fake)
    loaded = load_plugins_from_entrypoints(strict=False)
    assert loaded["filter"] == []
    assert any("Failed to load plugin entrypoint" in record.message for record in caplog.records)


def test_load_plugins_from_entrypoints_does_not_overwrite_by_default(
    monkeypatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    register_filter_plugin(
        "shared_name",
        lambda df: pd.Series([True] * len(df), index=df.index),
    )

    fake = _FakeEntryPoints(
        {
            "honestroles.filter_plugins": [
                _FakeEntryPoint(
                    "shared_name",
                    lambda df: pd.Series([False] * len(df), index=df.index),
                )
            ]
        }
    )
    monkeypatch.setattr(plugins_module, "entry_points", lambda: fake)
    loaded = load_plugins_from_entrypoints(overwrite=False, strict=False)
    assert loaded["filter"] == []
    assert any("Failed to load plugin entrypoint" in record.message for record in caplog.records)

    df = pd.DataFrame({"x": [1, 2]})
    filtered = apply_filter_plugins(df, ["shared_name"])
    assert len(filtered) == 2


def test_load_plugins_from_entrypoints_overwrite_true_replaces_existing(monkeypatch) -> None:
    register_filter_plugin(
        "shared_name",
        lambda df: pd.Series([True] * len(df), index=df.index),
    )

    fake = _FakeEntryPoints(
        {
            "honestroles.filter_plugins": [
                _FakeEntryPoint(
                    "shared_name",
                    lambda df: pd.Series([False] * len(df), index=df.index),
                )
            ]
        }
    )
    monkeypatch.setattr(plugins_module, "entry_points", lambda: fake)
    loaded = load_plugins_from_entrypoints(overwrite=True)
    assert loaded["filter"] == ["shared_name"]

    df = pd.DataFrame({"x": [1, 2]})
    filtered = apply_filter_plugins(df, ["shared_name"])
    assert filtered.empty
