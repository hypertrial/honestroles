from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class InputConfig(StrictModel):
    kind: Literal["parquet"] = "parquet"
    path: Path

    @field_validator("path", mode="before")
    @classmethod
    def _coerce_path(cls, value: object) -> Path:
        if isinstance(value, Path):
            return value
        if isinstance(value, str):
            return Path(value)
        raise TypeError("input.path must be a path-like string")


class OutputConfig(StrictModel):
    path: Path

    @field_validator("path", mode="before")
    @classmethod
    def _coerce_path(cls, value: object) -> Path:
        if isinstance(value, Path):
            return value
        if isinstance(value, str):
            return Path(value)
        raise TypeError("output.path must be a path-like string")


class CleanStageOptions(StrictModel):
    enabled: bool = True
    drop_null_titles: bool = True
    strip_html: bool = True


class FilterStageOptions(StrictModel):
    enabled: bool = True
    remote_only: bool = False
    min_salary: float | None = None
    required_keywords: tuple[str, ...] = ()

    @field_validator("required_keywords", mode="before")
    @classmethod
    def _coerce_required_keywords(cls, value: object) -> object:
        if isinstance(value, list):
            return tuple(value)
        return value


class LabelStageOptions(StrictModel):
    enabled: bool = True


class RateStageOptions(StrictModel):
    enabled: bool = True
    completeness_weight: float = 0.5
    quality_weight: float = 0.5

    @model_validator(mode="after")
    def _weights_non_negative(self) -> "RateStageOptions":
        if self.completeness_weight < 0 or self.quality_weight < 0:
            raise ValueError("rate weights must be non-negative")
        return self


class MatchStageOptions(StrictModel):
    enabled: bool = True
    top_k: int = Field(default=100, ge=1)


class StageConfig(StrictModel):
    clean: CleanStageOptions = Field(default_factory=CleanStageOptions)
    filter: FilterStageOptions = Field(default_factory=FilterStageOptions)
    label: LabelStageOptions = Field(default_factory=LabelStageOptions)
    rate: RateStageOptions = Field(default_factory=RateStageOptions)
    match: MatchStageOptions = Field(default_factory=MatchStageOptions)


class RuntimeConfig(StrictModel):
    fail_fast: bool = True
    random_seed: int = 0


class PipelineConfig(StrictModel):
    input: InputConfig
    output: OutputConfig | None = None
    stages: StageConfig = Field(default_factory=StageConfig)
    runtime: RuntimeConfig = Field(default_factory=RuntimeConfig)


class PluginSpecConfig(StrictModel):
    api_version: str = "1.0"
    plugin_version: str = "0.1.0"
    capabilities: tuple[str, ...] = ()

    @field_validator("capabilities", mode="before")
    @classmethod
    def _coerce_capabilities(cls, value: object) -> object:
        if isinstance(value, list):
            return tuple(value)
        return value


class PluginManifestItem(StrictModel):
    name: str
    kind: Literal["filter", "label", "rate"]
    callable: str
    enabled: bool = True
    order: int = 0
    settings: dict[str, Any] = Field(default_factory=dict)
    spec: PluginSpecConfig = Field(default_factory=PluginSpecConfig)

    @field_validator("name")
    @classmethod
    def _validate_name(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("plugin name must be non-empty")
        return cleaned


class PluginManifestConfig(StrictModel):
    plugins: tuple[PluginManifestItem, ...] = ()

    @field_validator("plugins", mode="before")
    @classmethod
    def _coerce_plugins(cls, value: object) -> object:
        if isinstance(value, list):
            return tuple(value)
        return value

    @model_validator(mode="after")
    def _unique_by_kind(self) -> "PluginManifestConfig":
        seen: set[tuple[str, str]] = set()
        for plugin in self.plugins:
            key = (plugin.kind, plugin.name)
            if key in seen:
                raise ValueError(
                    f"duplicate plugin entry for kind='{plugin.kind}' name='{plugin.name}'"
                )
            seen.add(key)
        return self
