from __future__ import annotations

import pandas as pd
import pytest
from hypothesis import given
from hypothesis import strategies as st

from honestroles.clean.dedup import compact_snapshots, deduplicate

from .strategies import MIXED_SCALARS, TIMESTAMP_LIKE_VALUES, dataframe_for_columns


@pytest.mark.fuzz
@given(
    df=dataframe_for_columns(
        {
            "job_key": MIXED_SCALARS,
            "content_hash": MIXED_SCALARS,
            "ingested_at": TIMESTAMP_LIKE_VALUES,
            "title": MIXED_SCALARS,
        },
        max_rows=16,
    ),
    keep=st.sampled_from(["first", "last", False]),
)
def test_fuzz_deduplicate_no_crash_and_row_bounds(df: pd.DataFrame, keep: str | bool) -> None:
    result = deduplicate(df, keep=keep)
    assert len(result) <= len(df)
    assert list(result.index) == list(range(len(result)))


@pytest.mark.fuzz
@given(
    df=dataframe_for_columns(
        {
            "job_key": MIXED_SCALARS,
            "content_hash": MIXED_SCALARS,
            "ingested_at": TIMESTAMP_LIKE_VALUES,
            "title": MIXED_SCALARS,
        },
        max_rows=16,
    ),
    key_columns=st.sampled_from(
        [
            ("job_key", "content_hash"),
            ("job_key",),
            ("content_hash",),
            (),
            ("missing_key",),
        ]
    ),
    timestamp_output=st.sampled_from(["iso8601", "datetime"]),
)
def test_fuzz_compact_snapshots_no_crash_and_metadata(
    df: pd.DataFrame,
    key_columns: tuple[str, ...],
    timestamp_output: str,
) -> None:
    result = compact_snapshots(
        df,
        key_columns=key_columns,
        timestamp_output=timestamp_output,  # type: ignore[arg-type]
    )

    for column in ["snapshot_count", "first_seen", "last_seen"]:
        assert column in result.columns

    if len(result) > 0:
        counts = pd.to_numeric(result["snapshot_count"], errors="coerce").fillna(0)
        assert (counts >= 1).all()

    if key_columns and all(column in df.columns for column in key_columns):
        deduped = result.drop_duplicates(subset=list(key_columns))
        assert len(deduped) == len(result)
