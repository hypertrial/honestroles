from __future__ import annotations

import pandas as pd
import pytest
from hypothesis import given

from honestroles.rate.completeness import rate_completeness

from .invariants import assert_numeric_between
from .strategies import ARRAY_LIKE_VALUES, MIXED_SCALARS, URL_LIKE_VALUES, dataframe_for_columns


@pytest.mark.fuzz
@given(
    df=dataframe_for_columns(
        {
            "company": MIXED_SCALARS,
            "title": MIXED_SCALARS,
            "location_raw": MIXED_SCALARS,
            "apply_url": URL_LIKE_VALUES,
            "description_text": MIXED_SCALARS,
            "salary_min": MIXED_SCALARS,
            "skills": ARRAY_LIKE_VALUES,
            "benefits": ARRAY_LIKE_VALUES,
        },
        max_rows=16,
    )
)
def test_fuzz_rate_completeness_no_crash_and_bounds(df: pd.DataFrame) -> None:
    result = rate_completeness(df)
    assert len(result) == len(df)
    if "completeness_score" in result.columns:
        assert_numeric_between(result["completeness_score"], minimum=0.0, maximum=1.0)
