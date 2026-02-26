from __future__ import annotations

import pandas as pd
import pytest
from hypothesis import given

from honestroles.label import label_jobs
from honestroles.plugins import register_label_plugin, reset_plugins

from .strategies import ARRAY_LIKE_VALUES, MIXED_SCALARS, TEXT_VALUES, dataframe_for_columns


@pytest.mark.fuzz
@given(
    df=dataframe_for_columns(
        {
            "title": TEXT_VALUES,
            "description_text": TEXT_VALUES,
            "skills": ARRAY_LIKE_VALUES,
            "source": MIXED_SCALARS,
        },
        max_rows=12,
    )
)
def test_fuzz_label_jobs_heuristic_orchestration(df: pd.DataFrame) -> None:
    result = label_jobs(df, use_llm=False)
    assert len(result) == len(df)
    assert result.index.equals(df.index)
    for column in ["seniority", "role_category", "tech_stack"]:
        assert column in result.columns


@pytest.mark.fuzz
@given(
    df=dataframe_for_columns(
        {
            "title": TEXT_VALUES,
            "description_text": TEXT_VALUES,
            "skills": ARRAY_LIKE_VALUES,
            "source": TEXT_VALUES,
        },
        max_rows=12,
    )
)
def test_fuzz_label_jobs_plugin_orchestration(df: pd.DataFrame) -> None:
    reset_plugins()
    def add_source_group(frame: pd.DataFrame, *, suffix: str = "_group") -> pd.DataFrame:
        result = frame.copy()
        result["source_group"] = result["source"].astype("string").fillna("") + suffix
        return result

    register_label_plugin("source_group", add_source_group, overwrite=True)
    result = label_jobs(
        df,
        use_llm=False,
        plugin_labelers=["source_group"],
        plugin_labeler_kwargs={"source_group": {"suffix": "_x"}},
    )
    assert "source_group" in result.columns
    assert len(result) == len(df)
