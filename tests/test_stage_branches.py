from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from honestroles.config.models import (
    CleanStageOptions,
    FilterStageOptions,
    LabelStageOptions,
    MatchStageOptions,
    RateStageOptions,
)
from honestroles.errors import StageExecutionError
from honestroles.plugins.errors import PluginExecutionError
from honestroles.plugins.types import LoadedPlugin, RuntimeExecutionContext
from honestroles.stages import (
    _apply_filter_options,
    clean_stage,
    ensure_schema,
    filter_stage,
    label_stage,
    match_stage,
    rate_stage,
)


def _ctx() -> RuntimeExecutionContext:
    return RuntimeExecutionContext(
        pipeline_config_path=Path("pipeline.toml"),
        plugin_manifest_path=None,
        stage_options={},
    )


def _base_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "id": ["1", "2"],
            "title": ["Role", "Senior Role"],
            "company": ["A", "B"],
            "location": ["Remote", "NYC"],
            "remote": [True, False],
            "description_text": ["python sql", "backend"],
            "description_html": ["<p>python</p>", "<p>backend</p>"],
            "skills": ["python,sql", "go"],
            "salary_min": [100.0, 0.0],
            "salary_max": [200.0, 0.0],
            "apply_url": ["https://a", "https://b"],
            "posted_at": ["2026-01-01", "2026-01-02"],
        }
    )


def test_ensure_schema_adds_missing_columns() -> None:
    out = ensure_schema(pl.DataFrame({"title": ["x"]}))
    assert "company" in out.columns


def test_clean_stage_strip_html_false_branch() -> None:
    frame = _base_df()
    out = clean_stage(frame, CleanStageOptions(strip_html=False), _ctx())
    assert out["description_text"].to_list()[0] == "python sql"


def test_clean_stage_wraps_generic_exception(monkeypatch) -> None:
    import honestroles.stages as stages_module

    def fail_ensure_schema(_df: pl.DataFrame) -> pl.DataFrame:
        raise RuntimeError("x")

    monkeypatch.setattr(stages_module, "ensure_schema", fail_ensure_schema)
    with pytest.raises(StageExecutionError):
        clean_stage(_base_df(), CleanStageOptions(), _ctx())


def test_apply_filter_options_remote_only_branch() -> None:
    out = _apply_filter_options(_base_df(), FilterStageOptions(remote_only=True))
    assert out.height == 1


def test_filter_stage_wraps_generic_exception(monkeypatch) -> None:
    import honestroles.stages as stages_module

    def fail_apply_filter_options(
        _df: pl.DataFrame, _opts: FilterStageOptions
    ) -> pl.DataFrame:
        raise RuntimeError("x")

    monkeypatch.setattr(stages_module, "_apply_filter_options", fail_apply_filter_options)
    with pytest.raises(StageExecutionError):
        filter_stage(_base_df(), FilterStageOptions(), _ctx())


def test_label_stage_plugin_exception_reraised() -> None:
    def explode(_df, _ctx):
        raise RuntimeError("boom")

    plugin = LoadedPlugin(name="bad", kind="label", callable_ref="x:y", func=explode)
    with pytest.raises(PluginExecutionError):
        label_stage(_base_df(), LabelStageOptions(), _ctx(), plugins=(plugin,))


def test_label_stage_invalid_plugin_return_reraised() -> None:
    def wrong(_df, _ctx):
        return "not-df"

    plugin = LoadedPlugin(name="bad", kind="label", callable_ref="x:y", func=wrong)
    with pytest.raises(PluginExecutionError):
        label_stage(_base_df(), LabelStageOptions(), _ctx(), plugins=(plugin,))


def test_label_stage_wraps_generic_exception() -> None:
    with pytest.raises(StageExecutionError):
        label_stage(pl.DataFrame({"company": ["x"]}), LabelStageOptions(), _ctx())


def test_rate_stage_zero_weight_sum_branch() -> None:
    out = rate_stage(
        _base_df(),
        RateStageOptions(completeness_weight=0.0, quality_weight=0.0),
        _ctx(),
    )
    assert out["rate_composite"].to_list() == [0.0, 0.0]


def test_rate_stage_plugin_exception_reraised() -> None:
    def explode(_df, _ctx):
        raise RuntimeError("boom")

    plugin = LoadedPlugin(name="bad", kind="rate", callable_ref="x:y", func=explode)
    with pytest.raises(PluginExecutionError):
        rate_stage(_base_df(), RateStageOptions(), _ctx(), plugins=(plugin,))


def test_rate_stage_invalid_plugin_return_reraised() -> None:
    def wrong(_df, _ctx):
        return "not-df"

    plugin = LoadedPlugin(name="bad", kind="rate", callable_ref="x:y", func=wrong)
    with pytest.raises(PluginExecutionError):
        rate_stage(_base_df(), RateStageOptions(), _ctx(), plugins=(plugin,))


def test_rate_stage_wraps_generic_exception() -> None:
    with pytest.raises(StageExecutionError):
        rate_stage(pl.DataFrame({"x": [1]}), RateStageOptions(), _ctx())


def test_match_stage_adds_missing_rate_and_label_columns() -> None:
    ranked, plan = match_stage(
        pl.DataFrame({"title": ["x"], "company": ["c"], "apply_url": ["u"]}),
        MatchStageOptions(top_k=5),
        _ctx(),
    )
    assert "fit_score" in ranked.columns
    assert ranked.height == 1
    assert isinstance(plan.application_plan, list)


def test_match_stage_adds_missing_label_when_rate_exists() -> None:
    ranked, _ = match_stage(
        pl.DataFrame(
            {
                "title": ["x"],
                "company": ["c"],
                "apply_url": ["u"],
                "rate_composite": [0.2],
            }
        ),
        MatchStageOptions(top_k=5),
        _ctx(),
    )
    assert "label_seniority" in ranked.columns


def test_match_stage_wraps_generic_exception() -> None:
    class BadOptions:
        top_k = "oops"

    with pytest.raises(StageExecutionError):
        match_stage(_base_df(), BadOptions(), _ctx())
