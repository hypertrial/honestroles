from __future__ import annotations

import pandas as pd
import pytest
from hypothesis import given
from hypothesis import strategies as st

from honestroles.clean.historical import (
    HISTORICAL_IS_LISTING_PAGE,
    HistoricalCleanOptions,
    clean_historical_jobs,
    detect_historical_listing_pages,
)

from .strategies import ARRAY_LIKE_VALUES, MIXED_SCALARS, TIMESTAMP_LIKE_VALUES, URL_LIKE_VALUES, dataframe_for_columns


@pytest.mark.fuzz
@given(
    df=dataframe_for_columns(
        {
            "location_raw": MIXED_SCALARS,
            "title": MIXED_SCALARS,
            "job_id": MIXED_SCALARS,
        },
        max_rows=14,
    )
)
def test_fuzz_detect_historical_listing_pages_returns_bool_mask(df: pd.DataFrame) -> None:
    mask = detect_historical_listing_pages(df)
    assert len(mask) == len(df)
    assert mask.index.equals(df.index)
    assert str(mask.dtype) == "bool"


@pytest.mark.fuzz
@given(
    df=dataframe_for_columns(
        {
            "job_key": MIXED_SCALARS,
            "content_hash": MIXED_SCALARS,
            "company": MIXED_SCALARS,
            "source": MIXED_SCALARS,
            "job_id": MIXED_SCALARS,
            "title": MIXED_SCALARS,
            "location_raw": MIXED_SCALARS,
            "apply_url": URL_LIKE_VALUES,
            "description_html": MIXED_SCALARS,
            "description_text": MIXED_SCALARS,
            "ingested_at": TIMESTAMP_LIKE_VALUES,
            "skills": ARRAY_LIKE_VALUES,
            "salary_text": MIXED_SCALARS,
            "remote_flag": MIXED_SCALARS,
        },
        max_rows=12,
    ),
    detect_listing_pages=st.booleans(),
    drop_listing_pages=st.booleans(),
    compact=st.booleans(),
    timestamp_output=st.sampled_from(["iso8601", "datetime"]),
)
def test_fuzz_clean_historical_jobs_no_crash_and_output_contract(
    df: pd.DataFrame,
    detect_listing_pages: bool,
    drop_listing_pages: bool,
    compact: bool,
    timestamp_output: str,
) -> None:
    result = clean_historical_jobs(
        df,
        options=HistoricalCleanOptions(
            detect_listing_pages=detect_listing_pages,
            drop_listing_pages=drop_listing_pages,
            compact_snapshots=compact,
            snapshot_timestamp_output=timestamp_output,  # type: ignore[arg-type]
        ),
    )

    assert HISTORICAL_IS_LISTING_PAGE in result.columns
    assert list(result.index) == list(range(len(result)))

    if compact:
        for column in ["snapshot_count", "first_seen", "last_seen"]:
            assert column in result.columns
