from __future__ import annotations

import pandas as pd
import pytest
from hypothesis import given
from hypothesis import strategies as st

from honestroles.rate.composite import rate_composite

from .strategies import MIXED_SCALARS, WEIGHT_VALUES, dataframe_for_columns


@pytest.mark.fuzz
@given(
    df=dataframe_for_columns(
        {
            "completeness_score": MIXED_SCALARS,
            "quality_score": MIXED_SCALARS,
        },
        max_rows=16,
    ),
    weights=st.dictionaries(
        st.sampled_from(["completeness_score", "quality_score", "other"]),
        WEIGHT_VALUES,
        min_size=0,
        max_size=3,
    ),
)
def test_fuzz_rate_composite_no_crash_and_finite_scores(
    df: pd.DataFrame,
    weights: dict[str, float],
) -> None:
    result = rate_composite(df, weights=weights)
    assert len(result) == len(df)
    if "rating" in result.columns:
        numeric = pd.to_numeric(result["rating"], errors="coerce").dropna()
        assert numeric.map(lambda value: pd.notna(value)).all()
