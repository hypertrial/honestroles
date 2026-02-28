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
from honestroles.domain import JobDataset
from honestroles.errors import StageExecutionError
from honestroles.plugins.errors import PluginExecutionError
from honestroles.plugins.types import PluginDefinition, RuntimeExecutionContext
from honestroles.stages import (
    _apply_filter_options,
    clean_stage,
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
            "skills": [["python", "sql"], ["go"]],
            "salary_min": [100.0, 0.0],
            "salary_max": [200.0, 0.0],
            "apply_url": ["https://a", "https://b"],
            "posted_at": ["2026-01-01", "2026-01-02"],
        }
    )


def _dataset() -> JobDataset:
    return JobDataset.from_polars(_base_df())


def test_clean_stage_strip_html_false_branch() -> None:
    out = clean_stage(_dataset(), CleanStageOptions(strip_html=False), _ctx())
    assert out.to_polars()["description_text"].to_list()[0] == "python sql"


def test_clean_stage_drop_null_titles_false_branch() -> None:
    frame = _base_df().with_columns(pl.lit("").alias("title"))
    out = clean_stage(
        JobDataset.from_polars(frame),
        CleanStageOptions(strip_html=False, drop_null_titles=False),
        _ctx(),
    )
    assert out.row_count() == frame.height


def test_clean_stage_wraps_generic_exception(monkeypatch) -> None:
    import honestroles.stages as stages_module

    def fail_clean_expr(_column: str) -> pl.Expr:
        raise RuntimeError("x")

    monkeypatch.setattr(stages_module, "_clean_text_expr", fail_clean_expr)
    with pytest.raises(StageExecutionError):
        clean_stage(_dataset(), CleanStageOptions(), _ctx())


def test_apply_filter_options_remote_only_branch() -> None:
    out = _apply_filter_options(_base_df(), FilterStageOptions(remote_only=True))
    assert out.height == 1


def test_apply_filter_options_ignores_blank_keyword() -> None:
    out = _apply_filter_options(
        _base_df(),
        FilterStageOptions(required_keywords=("   ",)),
    )
    assert out.height == _base_df().height


def test_filter_stage_wraps_generic_exception(monkeypatch) -> None:
    import honestroles.stages as stages_module

    def fail_apply_filter_options(
        _df: pl.DataFrame, _opts: FilterStageOptions
    ) -> pl.DataFrame:
        raise RuntimeError("x")

    monkeypatch.setattr(stages_module, "_apply_filter_options", fail_apply_filter_options)
    with pytest.raises(StageExecutionError):
        filter_stage(_dataset(), FilterStageOptions(), _ctx())


def test_label_stage_plugin_exception_reraised() -> None:
    def explode(_dataset, _ctx):
        raise RuntimeError("boom")

    plugin = PluginDefinition(name="bad", kind="label", callable_ref="x:y", func=explode)
    with pytest.raises(PluginExecutionError):
        label_stage(_dataset(), LabelStageOptions(), _ctx(), plugins=(plugin,))


def test_label_stage_invalid_plugin_return_reraised() -> None:
    def wrong(_dataset, _ctx):
        return "not-dataset"

    plugin = PluginDefinition(name="bad", kind="label", callable_ref="x:y", func=wrong)
    with pytest.raises(PluginExecutionError):
        label_stage(_dataset(), LabelStageOptions(), _ctx(), plugins=(plugin,))


def test_label_stage_rejects_non_canonical_plugin_dataset() -> None:
    def wrong_shape(_dataset, _ctx):
        return JobDataset._from_polars_unchecked(pl.DataFrame({"x": [1]}))

    plugin = PluginDefinition(name="bad", kind="label", callable_ref="x:y", func=wrong_shape)
    with pytest.raises(PluginExecutionError, match="returned invalid JobDataset: dataset is missing canonical fields"):
        label_stage(_dataset(), LabelStageOptions(), _ctx(), plugins=(plugin,))


def test_label_stage_wraps_invalid_dataset_entry() -> None:
    with pytest.raises(StageExecutionError):
        label_stage(JobDataset._from_polars_unchecked(pl.DataFrame({"company": ["x"]})), LabelStageOptions(), _ctx())


def test_rate_stage_zero_weight_sum_branch() -> None:
    out = rate_stage(
        _dataset(),
        RateStageOptions(completeness_weight=0.0, quality_weight=0.0),
        _ctx(),
    )
    assert out.to_polars()["rate_composite"].to_list() == [0.0, 0.0]


def test_rate_stage_plugin_exception_reraised() -> None:
    def explode(_dataset, _ctx):
        raise RuntimeError("boom")

    plugin = PluginDefinition(name="bad", kind="rate", callable_ref="x:y", func=explode)
    with pytest.raises(PluginExecutionError):
        rate_stage(_dataset(), RateStageOptions(), _ctx(), plugins=(plugin,))


def test_rate_stage_invalid_plugin_return_reraised() -> None:
    def wrong(_dataset, _ctx):
        return "not-dataset"

    plugin = PluginDefinition(name="bad", kind="rate", callable_ref="x:y", func=wrong)
    with pytest.raises(PluginExecutionError):
        rate_stage(_dataset(), RateStageOptions(), _ctx(), plugins=(plugin,))


def test_rate_stage_rejects_non_canonical_plugin_dataset() -> None:
    def wrong_shape(_dataset, _ctx):
        return JobDataset._from_polars_unchecked(pl.DataFrame({"x": [1]}))

    plugin = PluginDefinition(name="bad", kind="rate", callable_ref="x:y", func=wrong_shape)
    with pytest.raises(PluginExecutionError, match="returned invalid JobDataset: dataset is missing canonical fields"):
        rate_stage(_dataset(), RateStageOptions(), _ctx(), plugins=(plugin,))


def test_rate_stage_rejects_wrong_dtype_plugin_dataset() -> None:
    def wrong_dtype(_dataset, _ctx):
        return JobDataset._from_polars_unchecked(_base_df().with_columns(pl.lit("yes").alias("remote")))

    plugin = PluginDefinition(name="bad", kind="rate", callable_ref="x:y", func=wrong_dtype)
    with pytest.raises(PluginExecutionError, match="invalid dtype"):
        rate_stage(_dataset(), RateStageOptions(), _ctx(), plugins=(plugin,))


def test_rate_stage_wraps_invalid_dataset_entry() -> None:
    with pytest.raises(StageExecutionError):
        rate_stage(JobDataset._from_polars_unchecked(pl.DataFrame({"x": [1]})), RateStageOptions(), _ctx())


def test_match_stage_adds_missing_rate_and_label_columns() -> None:
    ranked, plan = match_stage(
        JobDataset.from_polars(
            pl.DataFrame(
                {
                    "id": ["1"],
                    "title": ["x"],
                    "company": ["c"],
                    "location": [None],
                    "remote": [None],
                    "description_text": [None],
                    "description_html": [None],
                    "skills": [[]],
                    "salary_min": [None],
                    "salary_max": [None],
                    "apply_url": ["u"],
                    "posted_at": [None],
                }
            )
        ),
        MatchStageOptions(top_k=5),
        _ctx(),
    )
    assert "fit_score" in ranked.to_polars().columns
    assert ranked.row_count() == 1
    assert isinstance(plan.application_plan, tuple)


def test_match_stage_adds_missing_label_when_rate_exists() -> None:
    ranked, _ = match_stage(
        JobDataset.from_polars(
            pl.DataFrame(
                {
                    "id": ["1"],
                    "title": ["x"],
                    "company": ["c"],
                    "location": [None],
                    "remote": [None],
                    "description_text": [None],
                    "description_html": [None],
                    "skills": [[]],
                    "salary_min": [None],
                    "salary_max": [None],
                    "apply_url": ["u"],
                    "posted_at": [None],
                    "rate_composite": [0.2],
                }
            )
        ),
        MatchStageOptions(top_k=5),
        _ctx(),
    )
    assert "label_seniority" in ranked.to_polars().columns


def test_match_stage_wraps_generic_exception() -> None:
    class BadOptions:
        top_k = "oops"

    with pytest.raises(StageExecutionError):
        match_stage(_dataset(), BadOptions(), _ctx())
