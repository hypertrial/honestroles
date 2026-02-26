from __future__ import annotations

from dataclasses import dataclass

from honestroles.errors import HonestRolesError


class PluginError(HonestRolesError):
    """Base plugin subsystem error."""


class PluginLoadError(PluginError):
    """Raised when plugin references cannot be imported."""


class PluginValidationError(PluginError):
    """Raised when plugin metadata/signature contract is invalid."""


@dataclass(slots=True)
class PluginExecutionError(PluginError):
    """Raised when plugin execution fails."""

    plugin_name: str
    stage_kind: str
    detail: str

    def __str__(self) -> str:
        return (
            f"plugin '{self.plugin_name}' ({self.stage_kind}) execution failed: {self.detail}"
        )
