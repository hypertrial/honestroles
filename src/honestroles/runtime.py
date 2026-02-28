from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import random

from honestroles.config import PipelineSpec, load_pipeline_config
from honestroles.diagnostics import (
    InputAdapterDiagnostics,
    InputAliasingDiagnostics,
    NonFatalStageError,
    PluginExecutionCounts,
    RuntimeDiagnostics,
    RuntimeSettingsSnapshot,
    StageRowCounts,
)
from honestroles.domain import JobDataset
from honestroles.errors import HonestRolesError, RuntimeInitializationError
from honestroles.io import (
    apply_source_adapter,
    normalize_source_data_contract,
    read_parquet,
    resolve_source_aliases,
    validate_source_data_contract,
    write_parquet,
)
from honestroles.objects import PipelineRun
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
class HonestRolesRuntime:
    pipeline_spec: PipelineSpec
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
            pipeline_spec = load_pipeline_config(pipeline_path)
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
            pipeline_spec=pipeline_spec,
            plugin_registry=plugin_registry,
            pipeline_config_path=pipeline_path,
            plugin_manifest_path=manifest_path,
        )

    def run(self) -> PipelineRun:
        random.seed(self.pipeline_spec.runtime.random_seed)
        stage_rows = StageRowCounts()
        plugin_counts = PluginExecutionCounts(
            filter=len(self.plugin_registry.plugins_for_kind("filter")),
            label=len(self.plugin_registry.plugins_for_kind("label")),
            rate=len(self.plugin_registry.plugins_for_kind("rate")),
        )
        runtime_snapshot = RuntimeSettingsSnapshot(
            fail_fast=self.pipeline_spec.runtime.fail_fast,
            random_seed=self.pipeline_spec.runtime.random_seed,
        )

        df = read_parquet(self.pipeline_spec.input.path)
        df, adapter_payload = apply_source_adapter(df, self.pipeline_spec.input.adapter)
        df, aliasing_payload = resolve_source_aliases(df, self.pipeline_spec.input.aliases)
        df = normalize_source_data_contract(df)
        df = validate_source_data_contract(df)
        dataset = JobDataset.from_polars(df)
        dataset.validate()

        runtime_ctx = RuntimeExecutionContext(
            pipeline_config_path=self.pipeline_config_path,
            plugin_manifest_path=self.plugin_manifest_path,
            stage_options=self.pipeline_spec.stages.model_dump(mode="python"),
        )

        stage_rows = stage_rows.record("input", dataset.row_count())
        artifacts = StageArtifacts()
        non_fatal_errors: list[NonFatalStageError] = []

        def _record_non_fatal(stage: str, exc: HonestRolesError) -> None:
            non_fatal_errors.append(
                NonFatalStageError(
                    stage=stage,
                    error_type=exc.__class__.__name__,
                    detail=str(exc),
                )
            )

        if self.pipeline_spec.stages.clean.enabled:
            try:
                dataset = clean_stage(dataset, self.pipeline_spec.stages.clean, runtime_ctx)
            except HonestRolesError as exc:
                if self.pipeline_spec.runtime.fail_fast:
                    raise
                _record_non_fatal("clean", exc)
            stage_rows = stage_rows.record("clean", dataset.row_count())

        if self.pipeline_spec.stages.filter.enabled:
            try:
                dataset = filter_stage(
                    dataset,
                    self.pipeline_spec.stages.filter,
                    runtime_ctx,
                    plugins=self.plugin_registry.plugins_for_kind("filter"),
                )
            except HonestRolesError as exc:
                if self.pipeline_spec.runtime.fail_fast:
                    raise
                _record_non_fatal("filter", exc)
            stage_rows = stage_rows.record("filter", dataset.row_count())

        if self.pipeline_spec.stages.label.enabled:
            try:
                dataset = label_stage(
                    dataset,
                    self.pipeline_spec.stages.label,
                    runtime_ctx,
                    plugins=self.plugin_registry.plugins_for_kind("label"),
                )
            except HonestRolesError as exc:
                if self.pipeline_spec.runtime.fail_fast:
                    raise
                _record_non_fatal("label", exc)
            stage_rows = stage_rows.record("label", dataset.row_count())

        if self.pipeline_spec.stages.rate.enabled:
            try:
                dataset = rate_stage(
                    dataset,
                    self.pipeline_spec.stages.rate,
                    runtime_ctx,
                    plugins=self.plugin_registry.plugins_for_kind("rate"),
                )
            except HonestRolesError as exc:
                if self.pipeline_spec.runtime.fail_fast:
                    raise
                _record_non_fatal("rate", exc)
            stage_rows = stage_rows.record("rate", dataset.row_count())

        if self.pipeline_spec.stages.match.enabled:
            try:
                dataset, artifacts = match_stage(
                    dataset,
                    self.pipeline_spec.stages.match,
                    runtime_ctx,
                )
            except HonestRolesError as exc:
                if self.pipeline_spec.runtime.fail_fast:
                    raise
                _record_non_fatal("match", exc)
            stage_rows = stage_rows.record("match", dataset.row_count())

        output_path: str | None = None
        if self.pipeline_spec.output is not None:
            write_parquet(dataset.to_polars(copy=False), self.pipeline_spec.output.path)
            output_path = str(self.pipeline_spec.output.path)

        diagnostics = RuntimeDiagnostics(
            input_path=str(self.pipeline_spec.input.path),
            stage_rows=stage_rows,
            plugin_counts=plugin_counts,
            runtime=runtime_snapshot,
            input_adapter=InputAdapterDiagnostics.from_mapping(adapter_payload),
            input_aliasing=InputAliasingDiagnostics.from_mapping(aliasing_payload),
            output_path=output_path,
            final_rows=dataset.row_count(),
            non_fatal_errors=tuple(non_fatal_errors),
        )
        return PipelineRun(
            dataset=dataset,
            diagnostics=diagnostics,
            application_plan=artifacts.application_plan,
        )
