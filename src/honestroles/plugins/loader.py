from __future__ import annotations

import importlib
import inspect
from collections.abc import Callable
from types import MappingProxyType
from typing import Any, get_origin, get_type_hints

import polars as pl

from honestroles.config.models import PluginManifestConfig, PluginManifestItem
from honestroles.plugins.errors import PluginLoadError, PluginValidationError
from honestroles.plugins.types import (
    FilterPluginContext,
    LabelPluginContext,
    LoadedPlugin,
    PluginKind,
    PluginSpec,
    RatePluginContext,
)


def _import_callable(callable_ref: str) -> Callable[..., Any]:
    if ":" not in callable_ref:
        raise PluginLoadError(
            f"invalid callable ref '{callable_ref}'. expected 'module:function'"
        )
    module_name, attr = callable_ref.split(":", 1)
    if not module_name or not attr:
        raise PluginLoadError(
            f"invalid callable ref '{callable_ref}'. expected 'module:function'"
        )
    try:
        module = importlib.import_module(module_name)
    except Exception as exc:
        raise PluginLoadError(f"failed importing module '{module_name}': {exc}") from exc
    try:
        loaded = getattr(module, attr)
    except AttributeError as exc:
        raise PluginLoadError(
            f"callable '{attr}' not found in module '{module_name}'"
        ) from exc
    if not callable(loaded):
        raise PluginLoadError(f"reference '{callable_ref}' does not resolve to callable")
    return loaded


def _annotation_matches(annotation: Any, expected: Any) -> bool:
    if annotation is inspect.Signature.empty:
        return False
    if annotation is expected:
        return True
    origin = get_origin(annotation)
    return origin is expected


def _freeze_value(value: Any) -> Any:
    if isinstance(value, dict):
        return MappingProxyType({k: _freeze_value(v) for k, v in value.items()})
    if isinstance(value, list):
        return tuple(_freeze_value(item) for item in value)
    if isinstance(value, set):
        return frozenset(_freeze_value(item) for item in value)
    return value


def _validate_signature(name: str, kind: PluginKind, func: Callable[..., Any]) -> None:
    sig = inspect.signature(func)
    params = list(sig.parameters.values())
    try:
        hints = get_type_hints(func)
    except Exception as exc:
        raise PluginValidationError(
            f"plugin '{name}' ({kind}) has invalid type annotations: {exc}"
        ) from exc
    if len(params) < 2:
        raise PluginValidationError(
            f"plugin '{name}' ({kind}) must accept (dataframe, context)"
        )

    first = params[0]
    second = params[1]
    positional_kinds = {
        inspect.Parameter.POSITIONAL_ONLY,
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
    }
    if first.kind not in positional_kinds or second.kind not in positional_kinds:
        raise PluginValidationError(
            f"plugin '{name}' ({kind}) first two args must be positional"
        )

    expected_ctx = {
        "filter": FilterPluginContext,
        "label": LabelPluginContext,
        "rate": RatePluginContext,
    }[kind]
    first_annotation = hints.get(first.name, first.annotation)
    second_annotation = hints.get(second.name, second.annotation)
    return_annotation = hints.get("return", sig.return_annotation)

    if not _annotation_matches(first_annotation, pl.DataFrame):
        raise PluginValidationError(
            f"plugin '{name}' ({kind}) first arg must be annotated as polars.DataFrame"
        )
    if not _annotation_matches(second_annotation, expected_ctx):
        raise PluginValidationError(
            f"plugin '{name}' ({kind}) second arg must be annotated as {expected_ctx.__name__}"
        )
    if not _annotation_matches(return_annotation, pl.DataFrame):
        raise PluginValidationError(
            f"plugin '{name}' ({kind}) return annotation must be polars.DataFrame"
        )


def load_plugins(manifest: PluginManifestConfig) -> tuple[LoadedPlugin, ...]:
    loaded: list[LoadedPlugin] = []
    for item in manifest.plugins:
        loaded.append(load_plugin_item(item))
    return tuple(sorted(loaded, key=lambda p: (p.kind, p.order, p.name)))


def load_plugin_item(item: PluginManifestItem) -> LoadedPlugin:
    func = _import_callable(item.callable)
    _validate_signature(item.name, item.kind, func)
    return LoadedPlugin(
        name=item.name,
        kind=item.kind,
        callable_ref=item.callable,
        func=func,
        order=item.order,
        enabled=item.enabled,
        settings=_freeze_value(item.settings),
        spec=PluginSpec(
            api_version=item.spec.api_version,
            plugin_version=item.spec.plugin_version,
            capabilities=tuple(item.spec.capabilities),
        ),
    )
