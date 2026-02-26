from __future__ import annotations

import pandas as pd
import pytest
from hypothesis import given

from honestroles.clean import clean_jobs

from .strategies import ARRAY_LIKE_VALUES, MIXED_SCALARS, TIMESTAMP_LIKE_VALUES, URL_LIKE_VALUES, dataframe_for_columns


@pytest.mark.fuzz
@given(
    df=dataframe_for_columns(
        {
            "job_key": MIXED_SCALARS,
            "content_hash": MIXED_SCALARS,
            "title": MIXED_SCALARS,
            "location_raw": MIXED_SCALARS,
            "description_html": MIXED_SCALARS,
            "description_text": MIXED_SCALARS,
            "salary_text": MIXED_SCALARS,
            "skills": ARRAY_LIKE_VALUES,
            "apply_url": URL_LIKE_VALUES,
            "ingested_at": TIMESTAMP_LIKE_VALUES,
            "remote_flag": MIXED_SCALARS,
        },
        max_rows=14,
    )
)
def test_fuzz_clean_jobs_pipeline_stability(df: pd.DataFrame) -> None:
    result = clean_jobs(df)

    assert len(result) <= len(df)
    assert list(result.index) == list(range(len(result)))

    for column in ["city", "region", "country", "remote_type", "skills"]:
        assert column in result.columns
