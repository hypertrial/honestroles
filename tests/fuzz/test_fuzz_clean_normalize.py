from __future__ import annotations

import pandas as pd
import pytest
from hypothesis import given

from honestroles.clean.normalize import (
    normalize_locations,
    normalize_salaries,
    normalize_skills,
)

from .strategies import (
    ARRAY_LIKE_VALUES,
    MIXED_SCALARS,
    TEXT_VALUES,
    dataframe_for_columns,
)


@pytest.mark.fuzz
@given(
    df=dataframe_for_columns(
        {
            "location_raw": MIXED_SCALARS,
            "remote_flag": MIXED_SCALARS,
            "title": TEXT_VALUES,
            "description_text": TEXT_VALUES,
            "remote_allowed": MIXED_SCALARS,
        },
        max_rows=12,
    )
)
def test_fuzz_normalize_locations_no_crash_and_row_count(df: pd.DataFrame) -> None:
    result = normalize_locations(df)
    assert len(result) == len(df)
    assert "city" in result.columns
    assert "region" in result.columns
    assert "country" in result.columns
    assert "remote_type" in result.columns


@pytest.mark.fuzz
@given(
    df=dataframe_for_columns(
        {
            "salary_text": MIXED_SCALARS,
            "description_text": TEXT_VALUES,
            "salary_min": MIXED_SCALARS,
            "salary_max": MIXED_SCALARS,
            "salary_currency": MIXED_SCALARS,
            "salary_interval": MIXED_SCALARS,
        },
        max_rows=12,
    )
)
def test_fuzz_normalize_salaries_no_crash_and_shape(df: pd.DataFrame) -> None:
    result = normalize_salaries(df)
    assert len(result) == len(df)
    for column in [
        "salary_min",
        "salary_max",
        "salary_currency",
        "salary_interval",
        "salary_annual_min",
        "salary_annual_max",
        "salary_confidence",
        "salary_source",
    ]:
        assert column in result.columns


@pytest.mark.fuzz
@given(
    df=dataframe_for_columns(
        {
            "skills": ARRAY_LIKE_VALUES,
            "title": TEXT_VALUES,
            "description_text": TEXT_VALUES,
        },
        max_rows=12,
    )
)
def test_fuzz_normalize_skills_returns_string_lists(df: pd.DataFrame) -> None:
    result = normalize_skills(df)
    assert len(result) == len(df)
    assert "skills" in result.columns
    for value in result["skills"].tolist():
        assert isinstance(value, list)
        assert all(isinstance(item, str) for item in value)


@pytest.mark.fuzz
def test_normalize_locations_preserves_non_default_index() -> None:
    df = pd.DataFrame(
        {
            "location_raw": ["New York, NY", "Toronto, ON"],
            "remote_flag": [False, False],
        },
        index=[10, 11],
    )
    result = normalize_locations(df)
    assert result.index.tolist() == [10, 11]
    assert result.loc[10, "city"] == "New York"
    assert result.loc[11, "city"] == "Toronto"


@pytest.mark.fuzz
def test_normalize_locations_falsey_remote_flag_strings() -> None:
    df = pd.DataFrame(
        {
            "location_raw": ["Austin, TX", "Seattle, WA"],
            "remote_flag": ["False", "0"],
        }
    )
    result = normalize_locations(df)
    assert result["remote_type"].tolist() == [None, None]
