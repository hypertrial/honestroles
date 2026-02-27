from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from honestroles.config.loaders import load_plugin_manifest
from honestroles.plugins.loader import load_plugins
from honestroles.plugins.types import PluginDefinition, PluginKind


@dataclass(frozen=True, slots=True)
class PluginRegistry:
    """Instance-scoped immutable plugin registry."""

    _plugins: tuple[PluginDefinition, ...] = ()

    @classmethod
    def from_manifest(cls, path: str | Path) -> "PluginRegistry":
        manifest = load_plugin_manifest(path)
        loaded = tuple(plugin for plugin in load_plugins(manifest) if plugin.enabled)
        return cls(_plugins=loaded)

    @classmethod
    def from_plugins(cls, plugins: tuple[PluginDefinition, ...]) -> "PluginRegistry":
        return cls(_plugins=tuple(sorted(plugins, key=lambda p: (p.kind, p.order, p.name))))

    def list(self, kind: PluginKind | None = None) -> tuple[str, ...]:
        if kind is None:
            return tuple(plugin.name for plugin in self._plugins)
        return tuple(plugin.name for plugin in self._plugins if plugin.kind == kind)

    def plugins_for_kind(self, kind: PluginKind) -> tuple[PluginDefinition, ...]:
        return tuple(plugin for plugin in self._plugins if plugin.kind == kind)
