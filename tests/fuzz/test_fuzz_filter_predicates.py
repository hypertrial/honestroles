from __future__ import annotations

import pandas as pd
import pytest
from hypothesis import given
from hypothesis import strategies as st

from honestroles.filter.predicates import (
    by_keywords,
    by_location,
    by_recency,
    by_salary,
)

from .invariants import assert_mask_shape
from .strategies import (
    MIXED_SCALARS,
    TEXT_VALUES,
    TIMESTAMP_LIKE_VALUES,
    dataframe_for_columns,
)

NUMERIC_SALARY = st.one_of(
    st.none(),
    st.integers(min_value=0, max_value=2_000_000),
    st.floats(min_value=0, max_value=2_000_000, allow_nan=False, allow_infinity=False),
)


@pytest.mark.fuzz
@given(
    df=dataframe_for_columns(
        {
            "location_raw": MIXED_SCALARS,
            "city": TEXT_VALUES,
            "region": TEXT_VALUES,
            "country": TEXT_VALUES,
            "remote_flag": MIXED_SCALARS,
            "remote_type": MIXED_SCALARS,
        },
        min_rows=1,
        max_rows=14,
    )
)
def test_fuzz_by_location_remote_only_returns_mask(df: pd.DataFrame) -> None:
    mask = by_location(df, remote_only=True, cities=["new york"], countries=["US"])
    assert_mask_shape(df, mask)


@pytest.mark.fuzz
@given(
    df=dataframe_for_columns(
        {
            "salary_min": NUMERIC_SALARY,
            "salary_max": NUMERIC_SALARY,
            "salary_currency": TEXT_VALUES,
        },
        min_rows=1,
        max_rows=14,
    ),
    min_salary=st.one_of(st.none(), st.floats(min_value=0, max_value=1_000_000, allow_nan=False)),
    max_salary=st.one_of(st.none(), st.floats(min_value=0, max_value=1_000_000, allow_nan=False)),
)
def test_fuzz_by_salary_numeric_inputs_return_mask(
    df: pd.DataFrame,
    min_salary: float | None,
    max_salary: float | None,
) -> None:
    mask = by_salary(df, min_salary=min_salary, max_salary=max_salary, currency="USD")
    assert_mask_shape(df, mask)


@pytest.mark.fuzz
def test_by_salary_mixed_type_cells_do_not_raise() -> None:
    df = pd.DataFrame(
        {
            "salary_min": ["100000", None],
            "salary_max": ["200000", "bad"],
            "salary_currency": ["USD", "USD"],
        }
    )
    mask = by_salary(df, min_salary=150000)
    assert_mask_shape(df, mask)


@pytest.mark.fuzz
@given(
    df=dataframe_for_columns(
        {"title": MIXED_SCALARS, "description_text": MIXED_SCALARS},
        min_rows=1,
        max_rows=14,
    ),
    include=st.lists(TEXT_VALUES, min_size=0, max_size=3),
    exclude=st.lists(TEXT_VALUES, min_size=0, max_size=3),
)
def test_fuzz_by_keywords_no_exception_and_shape(
    df: pd.DataFrame,
    include: list[str],
    exclude: list[str],
) -> None:
    mask = by_keywords(df, include=include, exclude=exclude)
    assert_mask_shape(df, mask)


@pytest.mark.fuzz
@pytest.mark.filterwarnings("ignore:invalid value encountered in cast:RuntimeWarning")
@given(
    df=dataframe_for_columns(
        {
            "posted_at": TIMESTAMP_LIKE_VALUES,
            "last_seen": TIMESTAMP_LIKE_VALUES,
            "ingested_at": TIMESTAMP_LIKE_VALUES,
        },
        min_rows=1,
        max_rows=14,
    ),
    posted_within_days=st.one_of(st.none(), st.integers(min_value=0, max_value=365)),
    seen_within_days=st.one_of(st.none(), st.integers(min_value=0, max_value=365)),
    as_of=TIMESTAMP_LIKE_VALUES,
)
def test_fuzz_by_recency_no_exception_and_shape(
    df: pd.DataFrame,
    posted_within_days: int | None,
    seen_within_days: int | None,
    as_of: object,
) -> None:
    mask = by_recency(
        df,
        posted_within_days=posted_within_days,
        seen_within_days=seen_within_days,
        as_of=as_of,  # type: ignore[arg-type]
    )
    assert_mask_shape(df, mask)
