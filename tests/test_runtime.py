from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import polars as pl
import pytest

from honestroles.errors import RuntimeInitializationError, StageExecutionError
from honestroles.plugins.errors import PluginExecutionError
from honestroles.runtime import HonestRolesRuntime


def test_runtime_run_end_to_end(
    pipeline_config_path: Path, plugin_manifest_path: Path
) -> None:
    runtime = HonestRolesRuntime.from_configs(pipeline_config_path, plugin_manifest_path)
    result = runtime.run()

    assert isinstance(result.dataframe, pl.DataFrame)
    assert result.dataframe.height > 0
    assert "fit_score" in result.dataframe.columns
    assert "plugin_label_note" in result.dataframe.columns
    assert result.diagnostics["final_rows"] == result.dataframe.height
    assert result.application_plan


def test_runtime_deterministic(
    pipeline_config_path: Path, plugin_manifest_path: Path
) -> None:
    runtime = HonestRolesRuntime.from_configs(pipeline_config_path, plugin_manifest_path)
    first = runtime.run().dataframe
    second = runtime.run().dataframe
    assert first.equals(second)


def test_runtime_concurrent_isolated(
    pipeline_config_path: Path, plugin_manifest_path: Path
) -> None:
    runtime_a = HonestRolesRuntime.from_configs(pipeline_config_path, plugin_manifest_path)
    runtime_b = HonestRolesRuntime.from_configs(pipeline_config_path, None)

    def run_a() -> pl.DataFrame:
        return runtime_a.run().dataframe

    def run_b() -> pl.DataFrame:
        return runtime_b.run().dataframe

    with ThreadPoolExecutor(max_workers=2) as pool:
        a_df, b_df = pool.submit(run_a).result(), pool.submit(run_b).result()

    assert "plugin_label_note" in a_df.columns
    assert "plugin_label_note" not in b_df.columns


def test_runtime_non_fail_fast_collects_plugin_errors(
    pipeline_config_non_fail_fast_path: Path,
    fail_plugin_manifest_path: Path,
) -> None:
    runtime = HonestRolesRuntime.from_configs(
        pipeline_config_non_fail_fast_path,
        fail_plugin_manifest_path,
    )
    result = runtime.run()
    assert result.dataframe.height >= 0
    assert "non_fatal_errors" in result.diagnostics
    assert result.diagnostics["non_fatal_errors"][0]["stage"] == "filter"


def test_runtime_from_configs_wraps_generic_init_errors(monkeypatch) -> None:
    import honestroles.runtime as runtime_module

    def boom(_path):
        raise RuntimeError("unexpected")

    monkeypatch.setattr(runtime_module, "load_pipeline_config", boom)
    with pytest.raises(RuntimeInitializationError):
        HonestRolesRuntime.from_configs("pipeline.toml")


def test_runtime_fail_fast_true_re_raises_filter_error(
    pipeline_config_path: Path, fail_plugin_manifest_path: Path
) -> None:
    runtime = HonestRolesRuntime.from_configs(pipeline_config_path, fail_plugin_manifest_path)
    with pytest.raises(PluginExecutionError):
        runtime.run()


@pytest.mark.parametrize("patch_attr", ["clean_stage", "label_stage", "rate_stage", "match_stage"])
def test_runtime_fail_fast_true_re_raises_stage_errors(
    pipeline_config_path: Path, monkeypatch, patch_attr: str
) -> None:
    import honestroles.runtime as runtime_module

    def fail_stage(*_args, **_kwargs):
        raise StageExecutionError("x", "boom")

    monkeypatch.setattr(runtime_module, patch_attr, fail_stage)
    runtime = HonestRolesRuntime.from_configs(pipeline_config_path)
    with pytest.raises(StageExecutionError):
        runtime.run()


@pytest.mark.parametrize(
    ("stage_name", "patch_attr"),
    [
        ("clean", "clean_stage"),
        ("label", "label_stage"),
        ("rate", "rate_stage"),
        ("match", "match_stage"),
    ],
)
def test_runtime_non_fail_fast_collects_stage_errors(
    pipeline_config_non_fail_fast_path: Path,
    monkeypatch,
    stage_name: str,
    patch_attr: str,
) -> None:
    import honestroles.runtime as runtime_module

    def fail_stage(*_args, **_kwargs):
        raise StageExecutionError(stage_name, "boom")

    monkeypatch.setattr(runtime_module, patch_attr, fail_stage)
    runtime = HonestRolesRuntime.from_configs(
        pipeline_config_non_fail_fast_path,
    )
    result = runtime.run()
    assert "non_fatal_errors" in result.diagnostics
    assert any(entry["stage"] == stage_name for entry in result.diagnostics["non_fatal_errors"])
