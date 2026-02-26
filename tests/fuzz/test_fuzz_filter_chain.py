from __future__ import annotations

import pandas as pd
import pytest
from hypothesis import given
from hypothesis import strategies as st

from honestroles.filter.chain import FilterChain
from honestroles.filter.predicates import by_keywords, by_location

from .strategies import MIXED_SCALARS, TEXT_VALUES, dataframe_for_columns


@pytest.mark.fuzz
@given(
    df=dataframe_for_columns(
        {
            "title": MIXED_SCALARS,
            "description_text": MIXED_SCALARS,
            "location_raw": MIXED_SCALARS,
            "remote_flag": MIXED_SCALARS,
            "remote_type": MIXED_SCALARS,
        },
        max_rows=12,
    ),
    mode=st.sampled_from(["and", "or"]),
    include=st.lists(TEXT_VALUES, min_size=0, max_size=2),
)
def test_fuzz_filter_chain_apply_no_crash(df: pd.DataFrame, mode: str, include: list[str]) -> None:
    chain = FilterChain(mode=mode)
    chain.add(by_keywords, include=include)
    chain.add(by_location, remote_only=False)

    result = chain.apply(df)
    assert len(result) <= len(df)
    assert list(result.index) == list(range(len(result)))


@pytest.mark.fuzz
def test_filter_chain_invalid_mask_index_fails_deterministically() -> None:
    df = pd.DataFrame({"title": ["a", "b"]}, index=[10, 11])

    def bad_mask(frame: pd.DataFrame) -> pd.Series:
        return pd.Series([True] * len(frame), index=range(len(frame)))

    chain = FilterChain().add(bad_mask)
    with pytest.raises(Exception):
        chain.apply(df)
