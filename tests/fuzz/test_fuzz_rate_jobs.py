from __future__ import annotations

import pandas as pd
import pytest
from hypothesis import given

from honestroles.plugins import register_rate_plugin, reset_plugins
from honestroles.rate import rate_jobs

from .strategies import ARRAY_LIKE_VALUES, MIXED_SCALARS, TEXT_VALUES, URL_LIKE_VALUES, dataframe_for_columns


@pytest.mark.fuzz
@given(
    df=dataframe_for_columns(
        {
            "company": MIXED_SCALARS,
            "title": MIXED_SCALARS,
            "location_raw": MIXED_SCALARS,
            "apply_url": URL_LIKE_VALUES,
            "description_text": TEXT_VALUES,
            "salary_min": MIXED_SCALARS,
            "skills": ARRAY_LIKE_VALUES,
            "benefits": ARRAY_LIKE_VALUES,
        },
        max_rows=12,
    )
)
def test_fuzz_rate_jobs_orchestration_no_crash(df: pd.DataFrame) -> None:
    result = rate_jobs(df, use_llm=False)
    assert len(result) == len(df)
    for column in ["completeness_score", "quality_score"]:
        assert column in result.columns


@pytest.mark.fuzz
@given(
    df=dataframe_for_columns({"description_text": TEXT_VALUES}, max_rows=12),
)
def test_fuzz_rate_jobs_with_plugin_rater(df: pd.DataFrame) -> None:
    reset_plugins()
    def add_priority(frame: pd.DataFrame, *, cutoff: float = 0.5) -> pd.DataFrame:
        result = frame.copy()
        result["priority"] = pd.to_numeric(result.get("rating"), errors="coerce").fillna(0.0).ge(cutoff)
        return result

    register_rate_plugin("priority", add_priority, overwrite=True)
    result = rate_jobs(
        df,
        use_llm=False,
        plugin_raters=["priority"],
        plugin_rater_kwargs={"priority": {"cutoff": 0.25}},
    )
    assert "priority" in result.columns
    assert len(result) == len(df)
