from __future__ import annotations

from collections.abc import Callable

import pandas as pd

FilterPlugin = Callable[..., pd.Series]
LabelPlugin = Callable[..., pd.DataFrame]

_FILTER_PLUGINS: dict[str, FilterPlugin] = {}
_LABEL_PLUGINS: dict[str, LabelPlugin] = {}


def _validate_plugin_name(name: str) -> str:
    plugin_name = name.strip()
    if not plugin_name:
        raise ValueError("Plugin name must be a non-empty string.")
    return plugin_name


def register_filter_plugin(
    name: str, predicate: FilterPlugin, *, overwrite: bool = False
) -> None:
    plugin_name = _validate_plugin_name(name)
    if plugin_name in _FILTER_PLUGINS and not overwrite:
        raise ValueError(f"Filter plugin '{plugin_name}' is already registered.")
    _FILTER_PLUGINS[plugin_name] = predicate


def unregister_filter_plugin(name: str) -> None:
    plugin_name = _validate_plugin_name(name)
    _FILTER_PLUGINS.pop(plugin_name, None)


def list_filter_plugins() -> list[str]:
    return sorted(_FILTER_PLUGINS.keys())


def apply_filter_plugins(
    df: pd.DataFrame,
    plugin_names: list[str],
    *,
    mode: str = "and",
    plugin_kwargs: dict[str, dict[str, object]] | None = None,
) -> pd.DataFrame:
    if mode not in {"and", "or"}:
        raise ValueError("mode must be 'and' or 'or'")

    kwargs_by_plugin = plugin_kwargs or {}
    masks: list[pd.Series] = []
    for name in plugin_names:
        plugin_name = _validate_plugin_name(name)
        if plugin_name not in _FILTER_PLUGINS:
            raise KeyError(f"Unknown filter plugin: {plugin_name}")
        mask = _FILTER_PLUGINS[plugin_name](df, **kwargs_by_plugin.get(plugin_name, {}))
        if not isinstance(mask, pd.Series):
            raise TypeError(
                f"Filter plugin '{plugin_name}' must return a pandas Series mask."
            )
        masks.append(mask)

    if not masks:
        return df

    combined = masks[0]
    for next_mask in masks[1:]:
        if mode == "and":
            combined &= next_mask
        else:
            combined |= next_mask
    return df[combined].reset_index(drop=True)


def register_label_plugin(name: str, transform: LabelPlugin, *, overwrite: bool = False) -> None:
    plugin_name = _validate_plugin_name(name)
    if plugin_name in _LABEL_PLUGINS and not overwrite:
        raise ValueError(f"Label plugin '{plugin_name}' is already registered.")
    _LABEL_PLUGINS[plugin_name] = transform


def unregister_label_plugin(name: str) -> None:
    plugin_name = _validate_plugin_name(name)
    _LABEL_PLUGINS.pop(plugin_name, None)


def list_label_plugins() -> list[str]:
    return sorted(_LABEL_PLUGINS.keys())


def apply_label_plugins(
    df: pd.DataFrame,
    plugin_names: list[str],
    *,
    plugin_kwargs: dict[str, dict[str, object]] | None = None,
) -> pd.DataFrame:
    kwargs_by_plugin = plugin_kwargs or {}
    result = df
    for name in plugin_names:
        plugin_name = _validate_plugin_name(name)
        if plugin_name not in _LABEL_PLUGINS:
            raise KeyError(f"Unknown label plugin: {plugin_name}")
        result_candidate = _LABEL_PLUGINS[plugin_name](
            result, **kwargs_by_plugin.get(plugin_name, {})
        )
        if not isinstance(result_candidate, pd.DataFrame):
            raise TypeError(
                f"Label plugin '{plugin_name}' must return a pandas DataFrame."
            )
        result = result_candidate
    return result
