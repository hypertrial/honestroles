from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import random
from typing import Any

import polars as pl

from honestroles.config import PipelineConfig, load_pipeline_config
from honestroles.errors import HonestRolesError, RuntimeInitializationError
from honestroles.io import (
    resolve_source_aliases,
    normalize_source_data_contract,
    read_parquet,
    validate_source_data_contract,
    write_parquet,
)
from honestroles.plugins import PluginRegistry
from honestroles.plugins.types import RuntimeExecutionContext
from honestroles.stages import (
    StageArtifacts,
    clean_stage,
    filter_stage,
    label_stage,
    match_stage,
    rate_stage,
)


@dataclass(frozen=True, slots=True)
class RuntimeResult:
    dataframe: pl.DataFrame
    diagnostics: dict[str, Any]
    application_plan: list[dict[str, Any]]


@dataclass(frozen=True, slots=True)
class HonestRolesRuntime:
    pipeline_config: PipelineConfig
    plugin_registry: PluginRegistry
    pipeline_config_path: Path
    plugin_manifest_path: Path | None = None

    @classmethod
    def from_configs(
        cls,
        pipeline_config_path: str | Path,
        plugin_manifest_path: str | Path | None = None,
    ) -> "HonestRolesRuntime":
        pipeline_path = Path(pipeline_config_path).expanduser().resolve()
        try:
            pipeline_config = load_pipeline_config(pipeline_path)
            plugin_registry = (
                PluginRegistry.from_manifest(plugin_manifest_path)
                if plugin_manifest_path
                else PluginRegistry()
            )
        except HonestRolesError:
            raise
        except Exception as exc:
            raise RuntimeInitializationError(pipeline_path, str(exc)) from exc

        manifest_path = (
            Path(plugin_manifest_path).expanduser().resolve()
            if plugin_manifest_path is not None
            else None
        )
        return cls(
            pipeline_config=pipeline_config,
            plugin_registry=plugin_registry,
            pipeline_config_path=pipeline_path,
            plugin_manifest_path=manifest_path,
        )

    def run(self) -> RuntimeResult:
        random.seed(self.pipeline_config.runtime.random_seed)
        diagnostics: dict[str, Any] = {
            "input_path": str(self.pipeline_config.input.path),
            "stage_rows": {},
            "plugin_counts": {
                "filter": len(self.plugin_registry.plugins_for_kind("filter")),
                "label": len(self.plugin_registry.plugins_for_kind("label")),
                "rate": len(self.plugin_registry.plugins_for_kind("rate")),
            },
            "runtime": {
                "fail_fast": self.pipeline_config.runtime.fail_fast,
                "random_seed": self.pipeline_config.runtime.random_seed,
            },
        }

        df = read_parquet(self.pipeline_config.input.path)
        df, aliasing = resolve_source_aliases(df, self.pipeline_config.input.aliases)
        diagnostics["input_aliasing"] = aliasing
        df = normalize_source_data_contract(df)
        df = validate_source_data_contract(df)

        runtime_ctx = RuntimeExecutionContext(
            pipeline_config_path=self.pipeline_config_path,
            plugin_manifest_path=self.plugin_manifest_path,
            stage_options=self.pipeline_config.stages.model_dump(mode="python"),
        )

        diagnostics["stage_rows"]["input"] = df.height
        artifacts = StageArtifacts(application_plan=[])
        non_fatal_errors: list[dict[str, str]] = []

        def _record_non_fatal(stage: str, exc: HonestRolesError) -> None:
            non_fatal_errors.append(
                {
                    "stage": stage,
                    "error_type": exc.__class__.__name__,
                    "detail": str(exc),
                }
            )

        if self.pipeline_config.stages.clean.enabled:
            try:
                df = clean_stage(df, self.pipeline_config.stages.clean, runtime_ctx)
            except HonestRolesError as exc:
                if self.pipeline_config.runtime.fail_fast:
                    raise
                _record_non_fatal("clean", exc)
            diagnostics["stage_rows"]["clean"] = df.height

        if self.pipeline_config.stages.filter.enabled:
            try:
                df = filter_stage(
                    df,
                    self.pipeline_config.stages.filter,
                    runtime_ctx,
                    plugins=self.plugin_registry.plugins_for_kind("filter"),
                )
            except HonestRolesError as exc:
                if self.pipeline_config.runtime.fail_fast:
                    raise
                _record_non_fatal("filter", exc)
            diagnostics["stage_rows"]["filter"] = df.height

        if self.pipeline_config.stages.label.enabled:
            try:
                df = label_stage(
                    df,
                    self.pipeline_config.stages.label,
                    runtime_ctx,
                    plugins=self.plugin_registry.plugins_for_kind("label"),
                )
            except HonestRolesError as exc:
                if self.pipeline_config.runtime.fail_fast:
                    raise
                _record_non_fatal("label", exc)
            diagnostics["stage_rows"]["label"] = df.height

        if self.pipeline_config.stages.rate.enabled:
            try:
                df = rate_stage(
                    df,
                    self.pipeline_config.stages.rate,
                    runtime_ctx,
                    plugins=self.plugin_registry.plugins_for_kind("rate"),
                )
            except HonestRolesError as exc:
                if self.pipeline_config.runtime.fail_fast:
                    raise
                _record_non_fatal("rate", exc)
            diagnostics["stage_rows"]["rate"] = df.height

        if self.pipeline_config.stages.match.enabled:
            try:
                df, artifacts = match_stage(df, self.pipeline_config.stages.match, runtime_ctx)
            except HonestRolesError as exc:
                if self.pipeline_config.runtime.fail_fast:
                    raise
                _record_non_fatal("match", exc)
            diagnostics["stage_rows"]["match"] = df.height

        if self.pipeline_config.output is not None:
            write_parquet(df, self.pipeline_config.output.path)
            diagnostics["output_path"] = str(self.pipeline_config.output.path)

        diagnostics["final_rows"] = df.height
        if non_fatal_errors:
            diagnostics["non_fatal_errors"] = non_fatal_errors
        return RuntimeResult(
            dataframe=df,
            diagnostics=diagnostics,
            application_plan=artifacts.application_plan,
        )
