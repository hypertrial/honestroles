from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any, Callable, Literal, Mapping

import polars as pl

PluginKind = Literal["filter", "label", "rate"]


def _empty_mapping() -> Mapping[str, Any]:
    return MappingProxyType({})


@dataclass(frozen=True, slots=True)
class PluginSpec:
    api_version: str = "1.0"
    plugin_version: str = "0.1.0"
    capabilities: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class RuntimeExecutionContext:
    pipeline_config_path: Path
    plugin_manifest_path: Path | None
    stage_options: dict[str, Any]


@dataclass(frozen=True, slots=True)
class BasePluginContext:
    plugin_name: str
    settings: Mapping[str, Any] = field(default_factory=_empty_mapping)
    runtime: RuntimeExecutionContext | None = None


@dataclass(frozen=True, slots=True)
class FilterPluginContext(BasePluginContext):
    pass


@dataclass(frozen=True, slots=True)
class LabelPluginContext(BasePluginContext):
    pass


@dataclass(frozen=True, slots=True)
class RatePluginContext(BasePluginContext):
    pass


FilterPlugin = Callable[[pl.DataFrame, FilterPluginContext], pl.DataFrame]
LabelPlugin = Callable[[pl.DataFrame, LabelPluginContext], pl.DataFrame]
RatePlugin = Callable[[pl.DataFrame, RatePluginContext], pl.DataFrame]
PluginCallable = FilterPlugin | LabelPlugin | RatePlugin


@dataclass(frozen=True, slots=True)
class LoadedPlugin:
    name: str
    kind: PluginKind
    callable_ref: str
    func: PluginCallable
    order: int = 0
    enabled: bool = True
    settings: Mapping[str, Any] = field(default_factory=_empty_mapping)
    spec: PluginSpec = field(default_factory=PluginSpec)
