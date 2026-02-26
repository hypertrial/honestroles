from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


class HonestRolesError(Exception):
    """Base exception for runtime, config, and plugin failures."""


class ConfigValidationError(HonestRolesError):
    """Raised when pipeline or plugin configuration is invalid."""


@dataclass(slots=True)
class StageExecutionError(HonestRolesError):
    """Raised when a stage fails with context."""

    stage: str
    detail: str

    def __str__(self) -> str:
        return f"stage '{self.stage}' failed: {self.detail}"


@dataclass(slots=True)
class RuntimeInitializationError(HonestRolesError):
    """Raised when runtime cannot be constructed from configs."""

    pipeline_config_path: Path
    detail: str

    def __str__(self) -> str:
        return (
            f"runtime initialization failed for '{self.pipeline_config_path}': {self.detail}"
        )
