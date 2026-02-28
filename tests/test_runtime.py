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
    diagnostics = result.diagnostics.to_dict()
    frame = result.dataset.to_polars()

    assert isinstance(frame, pl.DataFrame)
    result.dataset.validate()
    assert frame.height > 0
    assert "fit_score" in frame.columns
    assert "plugin_label_note" in frame.columns
    assert diagnostics["final_rows"] == frame.height
    assert result.application_plan
    assert "input_aliasing" in diagnostics
    assert "input_adapter" in diagnostics
    assert set(diagnostics["input_aliasing"].keys()) == {
        "applied",
        "conflicts",
        "unresolved",
    }
    assert set(diagnostics["input_adapter"].keys()) == {
        "enabled",
        "applied",
        "conflicts",
        "coercion_errors",
        "null_like_hits",
        "unresolved",
        "error_samples",
    }


def test_runtime_deterministic(
    pipeline_config_path: Path, plugin_manifest_path: Path
) -> None:
    runtime = HonestRolesRuntime.from_configs(pipeline_config_path, plugin_manifest_path)
    first = runtime.run().dataset.to_polars()
    second = runtime.run().dataset.to_polars()
    assert first.equals(second)


def test_runtime_concurrent_isolated(
    pipeline_config_path: Path, plugin_manifest_path: Path
) -> None:
    runtime_a = HonestRolesRuntime.from_configs(pipeline_config_path, plugin_manifest_path)
    runtime_b = HonestRolesRuntime.from_configs(pipeline_config_path, None)

    def run_a() -> pl.DataFrame:
        return runtime_a.run().dataset.to_polars()

    def run_b() -> pl.DataFrame:
        return runtime_b.run().dataset.to_polars()

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
    assert result.dataset.to_polars().height >= 0
    diagnostics = result.diagnostics.to_dict()
    assert "non_fatal_errors" in diagnostics
    assert diagnostics["non_fatal_errors"][0]["stage"] == "filter"


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
    diagnostics = result.diagnostics.to_dict()
    assert "non_fatal_errors" in diagnostics
    assert any(entry["stage"] == stage_name for entry in diagnostics["non_fatal_errors"])


def test_runtime_alias_mapping_affects_remote_filtering(tmp_path: Path) -> None:
    parquet_path = tmp_path / "jobs.parquet"
    pl.DataFrame(
        {
            "id": ["1", "2"],
            "title": ["A", "B"],
            "company": ["X", "Y"],
            "location_raw": ["Remote", "NYC"],
            "remote_flag": [True, False],
            "description_text": ["desc", "desc"],
            "description_html": [None, None],
            "apply_url": ["https://x/1", "https://x/2"],
            "posted_at": ["2026-01-01", "2026-01-02"],
        }
    ).write_parquet(parquet_path)

    pipeline_path = tmp_path / "pipeline.toml"
    pipeline_path.write_text(
        f"""
[input]
kind = "parquet"
path = "{parquet_path}"

[input.aliases]
remote = ["remote_flag"]
location = ["location_raw"]

[stages.clean]
enabled = true

[stages.filter]
enabled = true
remote_only = true

[stages.label]
enabled = false

[stages.rate]
enabled = false

[stages.match]
enabled = false

[runtime]
fail_fast = true
random_seed = 0
""".strip(),
        encoding="utf-8",
    )

    runtime = HonestRolesRuntime.from_configs(pipeline_path)
    result = runtime.run()
    assert result.dataset.to_polars().height == 1
    assert result.dataset.to_polars()["id"].to_list() == ["1"]
    assert result.diagnostics.to_dict()["input_aliasing"]["applied"]["remote"] == "remote_flag"


def test_runtime_adapter_mapping_affects_remote_filtering(tmp_path: Path) -> None:
    parquet_path = tmp_path / "jobs.parquet"
    pl.DataFrame(
        {
            "id": ["1", "2"],
            "title": ["A", "B"],
            "company": ["X", "Y"],
            "location_raw": ["Remote", "NYC"],
            "remote_flag": ["yes", "no"],
            "description_text": ["desc", "desc"],
            "description_html": [None, None],
            "apply_url": ["https://x/1", "https://x/2"],
            "posted_at": ["2026-01-01", "2026-01-02"],
        }
    ).write_parquet(parquet_path)

    pipeline_path = tmp_path / "pipeline.toml"
    pipeline_path.write_text(
        f"""
[input]
kind = "parquet"
path = "{parquet_path}"

[input.adapter]
enabled = true

[input.adapter.fields.remote]
from = ["remote_flag"]
cast = "bool"

[stages.clean]
enabled = true

[stages.filter]
enabled = true
remote_only = true

[stages.label]
enabled = false

[stages.rate]
enabled = false

[stages.match]
enabled = false
""".strip(),
        encoding="utf-8",
    )

    runtime = HonestRolesRuntime.from_configs(pipeline_path)
    result = runtime.run()
    assert result.dataset.to_polars().height == 1
    assert result.dataset.to_polars()["id"].to_list() == ["1"]
    assert result.diagnostics.to_dict()["input_adapter"]["applied"]["remote"] == "remote_flag"


def test_runtime_adapter_then_alias_precedence(tmp_path: Path) -> None:
    parquet_path = tmp_path / "jobs.parquet"
    pl.DataFrame(
        {
            "id": ["1", "2"],
            "title": ["A", "B"],
            "company": ["X", "Y"],
            "location_raw": ["Remote", "NYC"],
            "location_alias": ["Austin", "Seattle"],
            "description_text": ["desc", "desc"],
            "description_html": [None, None],
            "apply_url": ["https://x/1", "https://x/2"],
            "posted_at": ["2026-01-01", "2026-01-02"],
        }
    ).write_parquet(parquet_path)

    pipeline_path = tmp_path / "pipeline.toml"
    pipeline_path.write_text(
        f"""
[input]
kind = "parquet"
path = "{parquet_path}"

[input.aliases]
location = ["location_alias"]

[input.adapter]
enabled = true

[input.adapter.fields.location]
from = ["location_raw"]
cast = "string"

[stages.clean]
enabled = false

[stages.filter]
enabled = false

[stages.label]
enabled = false

[stages.rate]
enabled = false

[stages.match]
enabled = false
""".strip(),
        encoding="utf-8",
    )

    runtime = HonestRolesRuntime.from_configs(pipeline_path)
    result = runtime.run()
    assert result.dataset.to_polars()["location"].to_list() == ["Remote", "NYC"]
    diagnostics = result.diagnostics.to_dict()
    assert diagnostics["input_adapter"]["applied"]["location"] == "location_raw"
    assert diagnostics["input_aliasing"]["applied"] == {}


def test_runtime_normalizes_canonical_types_when_clean_disabled(tmp_path: Path) -> None:
    parquet_path = tmp_path / "jobs.parquet"
    pl.DataFrame(
        {
            "id": [1],
            "title": ["A"],
            "company": ["X"],
            "location": ["Remote"],
            "remote": ["yes"],
            "description_text": ["desc"],
            "description_html": [None],
            "skills": ["python, sql"],
            "salary_min": ["100000"],
            "salary_max": ["120000"],
            "apply_url": ["https://x/1"],
            "posted_at": ["2026-01-01"],
        }
    ).write_parquet(parquet_path)

    pipeline_path = tmp_path / "pipeline.toml"
    pipeline_path.write_text(
        f"""
[input]
kind = "parquet"
path = "{parquet_path}"

[stages.clean]
enabled = false

[stages.filter]
enabled = false

[stages.label]
enabled = false

[stages.rate]
enabled = false

[stages.match]
enabled = false
""".strip(),
        encoding="utf-8",
    )

    result = HonestRolesRuntime.from_configs(pipeline_path).run()
    result.dataset.validate()
    frame = result.dataset.to_polars()
    assert frame.schema["id"] == pl.String
    assert frame.schema["remote"] == pl.Boolean
    assert isinstance(frame.schema["skills"], pl.List)
    assert frame["skills"].to_list() == [["python", "sql"]]
