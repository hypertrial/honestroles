from __future__ import annotations

import importlib
import importlib.util
import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from importlib.metadata import entry_points
from pathlib import Path
from types import ModuleType
from typing import Any, Literal

import pandas as pd

LOGGER = logging.getLogger(__name__)

SUPPORTED_PLUGIN_API_VERSION = "1.0"

FilterPlugin = Callable[..., pd.Series]
LabelPlugin = Callable[..., pd.DataFrame]
RatePlugin = Callable[..., pd.DataFrame]

PluginKind = Literal["filter", "label", "rate"]


@dataclass(frozen=True)
class PluginSpec:
    """Metadata for plugin compatibility and capability discovery."""

    api_version: str = SUPPORTED_PLUGIN_API_VERSION
    plugin_version: str = "0.1.0"
    capabilities: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class PluginExport:
    """Optional structured export used by entrypoint-based plugins."""

    kind: PluginKind
    plugin: Callable[..., Any]
    name: str | None = None
    spec: PluginSpec = field(default_factory=PluginSpec)

_FILTER_PLUGINS: dict[str, FilterPlugin] = {}
_LABEL_PLUGINS: dict[str, LabelPlugin] = {}
_RATE_PLUGINS: dict[str, RatePlugin] = {}

_FILTER_PLUGIN_SPECS: dict[str, PluginSpec] = {}
_LABEL_PLUGIN_SPECS: dict[str, PluginSpec] = {}
_RATE_PLUGIN_SPECS: dict[str, PluginSpec] = {}


def _validate_plugin_name(name: str) -> str:
    plugin_name = name.strip()
    if not plugin_name:
        raise ValueError("Plugin name must be a non-empty string.")
    return plugin_name


def _parse_api_version(version: str) -> tuple[int, int]:
    text = version.strip()
    parts = text.split(".")
    if len(parts) == 1:
        parts = [parts[0], "0"]
    if len(parts) != 2 or not all(part.isdigit() for part in parts):
        raise ValueError(
            f"Invalid plugin api_version '{version}'. Expected '<major>.<minor>' format."
        )
    return int(parts[0]), int(parts[1])


def _ensure_api_compatible(*, plugin_name: str, spec: PluginSpec) -> None:
    plugin_major, plugin_minor = _parse_api_version(spec.api_version)
    supported_major, supported_minor = _parse_api_version(SUPPORTED_PLUGIN_API_VERSION)
    if plugin_major != supported_major or plugin_minor > supported_minor:
        raise ValueError(
            f"Plugin '{plugin_name}' declares unsupported api_version '{spec.api_version}'. "
            f"Supported plugin API is '{SUPPORTED_PLUGIN_API_VERSION}'."
        )


def _normalize_plugin_spec(
    name: str,
    kind: PluginKind,
    *,
    spec: PluginSpec | Mapping[str, object] | None = None,
) -> PluginSpec:
    if spec is None:
        resolved = PluginSpec(capabilities=(kind,))
    elif isinstance(spec, PluginSpec):
        resolved = spec
    elif isinstance(spec, Mapping):
        api_version = str(spec.get("api_version", SUPPORTED_PLUGIN_API_VERSION))
        plugin_version = str(spec.get("plugin_version", "0.1.0"))
        raw_capabilities = spec.get("capabilities", (kind,))
        if isinstance(raw_capabilities, str):
            capabilities = (raw_capabilities,)
        else:
            capabilities = tuple(str(item) for item in raw_capabilities)  # type: ignore[arg-type]
        resolved = PluginSpec(
            api_version=api_version,
            plugin_version=plugin_version,
            capabilities=capabilities,
        )
    else:
        raise TypeError("spec must be PluginSpec, mapping, or None.")
    _ensure_api_compatible(plugin_name=name, spec=resolved)
    return resolved


def register_filter_plugin(
    name: str,
    predicate: FilterPlugin,
    *,
    overwrite: bool = False,
    spec: PluginSpec | Mapping[str, object] | None = None,
) -> None:
    plugin_name = _validate_plugin_name(name)
    if plugin_name in _FILTER_PLUGINS and not overwrite:
        raise ValueError(f"Filter plugin '{plugin_name}' is already registered.")
    _FILTER_PLUGINS[plugin_name] = predicate
    _FILTER_PLUGIN_SPECS[plugin_name] = _normalize_plugin_spec(
        plugin_name, "filter", spec=spec
    )


def unregister_filter_plugin(name: str) -> None:
    plugin_name = _validate_plugin_name(name)
    _FILTER_PLUGINS.pop(plugin_name, None)
    _FILTER_PLUGIN_SPECS.pop(plugin_name, None)


def list_filter_plugins() -> list[str]:
    return sorted(_FILTER_PLUGINS.keys())


def list_filter_plugin_specs() -> dict[str, PluginSpec]:
    return dict(_FILTER_PLUGIN_SPECS)


def get_filter_plugin_spec(name: str) -> PluginSpec:
    plugin_name = _validate_plugin_name(name)
    if plugin_name not in _FILTER_PLUGIN_SPECS:
        raise KeyError(f"Unknown filter plugin: {plugin_name}")
    return _FILTER_PLUGIN_SPECS[plugin_name]


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


def register_label_plugin(
    name: str,
    transform: LabelPlugin,
    *,
    overwrite: bool = False,
    spec: PluginSpec | Mapping[str, object] | None = None,
) -> None:
    plugin_name = _validate_plugin_name(name)
    if plugin_name in _LABEL_PLUGINS and not overwrite:
        raise ValueError(f"Label plugin '{plugin_name}' is already registered.")
    _LABEL_PLUGINS[plugin_name] = transform
    _LABEL_PLUGIN_SPECS[plugin_name] = _normalize_plugin_spec(
        plugin_name, "label", spec=spec
    )


def unregister_label_plugin(name: str) -> None:
    plugin_name = _validate_plugin_name(name)
    _LABEL_PLUGINS.pop(plugin_name, None)
    _LABEL_PLUGIN_SPECS.pop(plugin_name, None)


def list_label_plugins() -> list[str]:
    return sorted(_LABEL_PLUGINS.keys())


def list_label_plugin_specs() -> dict[str, PluginSpec]:
    return dict(_LABEL_PLUGIN_SPECS)


def get_label_plugin_spec(name: str) -> PluginSpec:
    plugin_name = _validate_plugin_name(name)
    if plugin_name not in _LABEL_PLUGIN_SPECS:
        raise KeyError(f"Unknown label plugin: {plugin_name}")
    return _LABEL_PLUGIN_SPECS[plugin_name]


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


def register_rate_plugin(
    name: str,
    transform: RatePlugin,
    *,
    overwrite: bool = False,
    spec: PluginSpec | Mapping[str, object] | None = None,
) -> None:
    plugin_name = _validate_plugin_name(name)
    if plugin_name in _RATE_PLUGINS and not overwrite:
        raise ValueError(f"Rate plugin '{plugin_name}' is already registered.")
    _RATE_PLUGINS[plugin_name] = transform
    _RATE_PLUGIN_SPECS[plugin_name] = _normalize_plugin_spec(
        plugin_name, "rate", spec=spec
    )


def unregister_rate_plugin(name: str) -> None:
    plugin_name = _validate_plugin_name(name)
    _RATE_PLUGINS.pop(plugin_name, None)
    _RATE_PLUGIN_SPECS.pop(plugin_name, None)


def list_rate_plugins() -> list[str]:
    return sorted(_RATE_PLUGINS.keys())


def list_rate_plugin_specs() -> dict[str, PluginSpec]:
    return dict(_RATE_PLUGIN_SPECS)


def get_rate_plugin_spec(name: str) -> PluginSpec:
    plugin_name = _validate_plugin_name(name)
    if plugin_name not in _RATE_PLUGIN_SPECS:
        raise KeyError(f"Unknown rate plugin: {plugin_name}")
    return _RATE_PLUGIN_SPECS[plugin_name]


def apply_rate_plugins(
    df: pd.DataFrame,
    plugin_names: list[str],
    *,
    plugin_kwargs: dict[str, dict[str, object]] | None = None,
) -> pd.DataFrame:
    kwargs_by_plugin = plugin_kwargs or {}
    result = df
    for name in plugin_names:
        plugin_name = _validate_plugin_name(name)
        if plugin_name not in _RATE_PLUGINS:
            raise KeyError(f"Unknown rate plugin: {plugin_name}")
        result_candidate = _RATE_PLUGINS[plugin_name](
            result, **kwargs_by_plugin.get(plugin_name, {})
        )
        if not isinstance(result_candidate, pd.DataFrame):
            raise TypeError(
                f"Rate plugin '{plugin_name}' must return a pandas DataFrame."
            )
        result = result_candidate
    return result


def _get_entry_points_for_group(group: str) -> list[Any]:
    discovered = entry_points()
    if hasattr(discovered, "select"):
        return list(discovered.select(group=group))
    return list(discovered.get(group, []))  # type: ignore[no-any-return]


def _register_loaded_plugin(
    loaded: object,
    *,
    entrypoint_name: str,
    kind: PluginKind,
    overwrite: bool,
) -> str:
    name = entrypoint_name
    spec: PluginSpec | Mapping[str, object] | None = None
    plugin_kind = kind

    if isinstance(loaded, PluginExport):
        plugin = loaded.plugin
        plugin_kind = loaded.kind
        name = loaded.name or entrypoint_name
        spec = loaded.spec
    elif callable(loaded):
        plugin = loaded
    else:
        raise TypeError(
            f"Entrypoint '{entrypoint_name}' must resolve to a callable or PluginExport."
        )

    if plugin_kind == "filter":
        register_filter_plugin(name, plugin, overwrite=overwrite, spec=spec)  # type: ignore[arg-type]
    elif plugin_kind == "label":
        register_label_plugin(name, plugin, overwrite=overwrite, spec=spec)  # type: ignore[arg-type]
    else:
        register_rate_plugin(name, plugin, overwrite=overwrite, spec=spec)  # type: ignore[arg-type]
    return name


def load_plugins_from_entrypoints(
    *,
    overwrite: bool = False,
    strict: bool = False,
) -> dict[str, list[str]]:
    """Load plugins discovered from package entrypoints."""

    groups: dict[str, PluginKind] = {
        "honestroles.filter_plugins": "filter",
        "honestroles.label_plugins": "label",
        "honestroles.rate_plugins": "rate",
    }
    loaded_names: dict[str, list[str]] = {"filter": [], "label": [], "rate": []}

    for group, kind in groups.items():
        for entrypoint in _get_entry_points_for_group(group):
            try:
                loaded = entrypoint.load()
                registered_name = _register_loaded_plugin(
                    loaded,
                    entrypoint_name=entrypoint.name,
                    kind=kind,
                    overwrite=overwrite,
                )
                loaded_names[kind].append(registered_name)
            except Exception as exc:  # pragma: no cover - optional environment behavior
                message = f"Failed to load plugin entrypoint '{entrypoint.name}' ({group}): {exc}"
                if strict:
                    raise RuntimeError(message) from exc
                LOGGER.warning(message)
    return loaded_names


def _import_plugin_module(module_ref: str) -> ModuleType:
    ref = module_ref.strip()
    if not ref:
        raise ValueError("module_ref must be a non-empty string.")

    path = Path(ref)
    if path.exists():
        if path.is_dir():
            raise ValueError("module_ref path must be a Python file, not a directory.")
        module_name = f"honestroles_plugin_{path.stem}"
        spec = importlib.util.spec_from_file_location(module_name, path)
        if spec is None or spec.loader is None:
            raise ImportError(f"Cannot import plugin module from path: {path}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    return importlib.import_module(ref)


def load_plugins_from_module(module_ref: str) -> dict[str, list[str]]:
    """Load plugins from a module that exposes `register()` or `register_plugins()`."""

    before = {
        "filter": set(list_filter_plugins()),
        "label": set(list_label_plugins()),
        "rate": set(list_rate_plugins()),
    }
    module = _import_plugin_module(module_ref)
    registrar = getattr(module, "register_plugins", None) or getattr(module, "register", None)
    if registrar is None or not callable(registrar):
        raise AttributeError(
            f"Module '{module.__name__}' must expose callable `register()` or `register_plugins()`."
        )
    registrar()

    return {
        "filter": sorted(set(list_filter_plugins()) - before["filter"]),
        "label": sorted(set(list_label_plugins()) - before["label"]),
        "rate": sorted(set(list_rate_plugins()) - before["rate"]),
    }


def reset_plugins() -> None:
    """Reset all registered plugins. Intended for tests and isolated runs."""
    _FILTER_PLUGINS.clear()
    _LABEL_PLUGINS.clear()
    _RATE_PLUGINS.clear()
    _FILTER_PLUGIN_SPECS.clear()
    _LABEL_PLUGIN_SPECS.clear()
    _RATE_PLUGIN_SPECS.clear()
