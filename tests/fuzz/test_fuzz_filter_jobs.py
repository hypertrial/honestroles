from __future__ import annotations

import pandas as pd
import pytest
from hypothesis import given
from hypothesis import strategies as st

from honestroles.filter import filter_jobs
from honestroles.plugins import register_filter_plugin, reset_plugins

from .strategies import ARRAY_LIKE_VALUES, MIXED_SCALARS, TEXT_VALUES, TIMESTAMP_LIKE_VALUES, dataframe_for_columns


@pytest.mark.fuzz
@given(
    df=dataframe_for_columns(
        {
            "title": MIXED_SCALARS,
            "description_text": MIXED_SCALARS,
            "location_raw": MIXED_SCALARS,
            "city": TEXT_VALUES,
            "region": TEXT_VALUES,
            "country": TEXT_VALUES,
            "remote_flag": MIXED_SCALARS,
            "remote_type": MIXED_SCALARS,
            "salary_min": MIXED_SCALARS,
            "salary_max": MIXED_SCALARS,
            "salary_currency": TEXT_VALUES,
            "skills": ARRAY_LIKE_VALUES,
            "posted_at": TIMESTAMP_LIKE_VALUES,
            "last_seen": TIMESTAMP_LIKE_VALUES,
            "ingested_at": TIMESTAMP_LIKE_VALUES,
        },
        max_rows=12,
    ),
    remote_only=st.booleans(),
    min_salary=st.one_of(st.none(), st.floats(min_value=0, max_value=1_000_000, allow_nan=False)),
    max_salary=st.one_of(st.none(), st.floats(min_value=0, max_value=1_000_000, allow_nan=False)),
    include_keywords=st.lists(TEXT_VALUES, min_size=0, max_size=2),
    required_skills=st.lists(TEXT_VALUES, min_size=0, max_size=2),
)
def test_fuzz_filter_jobs_no_crash_and_shape(
    df: pd.DataFrame,
    remote_only: bool,
    min_salary: float | None,
    max_salary: float | None,
    include_keywords: list[str],
    required_skills: list[str],
) -> None:
    result = filter_jobs(
        df,
        remote_only=remote_only,
        min_salary=min_salary,
        max_salary=max_salary,
        include_keywords=include_keywords,
        required_skills=required_skills,
    )
    assert len(result) <= len(df)
    assert list(result.index) == list(range(len(result)))


@pytest.mark.fuzz
@given(
    df=dataframe_for_columns(
        {
            "title": MIXED_SCALARS,
            "description_text": MIXED_SCALARS,
        },
        max_rows=12,
    ),
    threshold=st.floats(min_value=0.0, max_value=1.0, allow_nan=False),
)
def test_fuzz_filter_jobs_plugin_path_no_crash(df: pd.DataFrame, threshold: float) -> None:
    reset_plugins()
    def score_filter(frame: pd.DataFrame, *, threshold: float = 0.5) -> pd.Series:
        mask = frame["title"].astype("string").fillna("").str.len() / 100.0
        return mask.ge(threshold).astype("bool")

    register_filter_plugin("score_filter", score_filter, overwrite=True)
    result = filter_jobs(
        df,
        plugin_filters=["score_filter"],
        plugin_filter_kwargs={"score_filter": {"threshold": threshold}},
    )
    assert len(result) <= len(df)


@pytest.mark.fuzz
def test_filter_jobs_plugin_bad_mask_type_raises() -> None:
    reset_plugins()
    df = pd.DataFrame({"title": ["a", "b"]})

    def bad_filter(frame: pd.DataFrame) -> list[bool]:
        return [True] * len(frame)

    register_filter_plugin("bad_filter", bad_filter)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        filter_jobs(df, plugin_filters=["bad_filter"])
