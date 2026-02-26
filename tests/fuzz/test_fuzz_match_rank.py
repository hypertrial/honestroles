from __future__ import annotations

import pandas as pd
import pytest
from hypothesis import given
from hypothesis import strategies as st

from honestroles.match import CandidateProfile
from honestroles.match.models import DEFAULT_RESULT_COLUMNS
from honestroles.match.rank import rank_jobs

from .strategies import ARRAY_LIKE_VALUES, MIXED_SCALARS, TIMESTAMP_LIKE_VALUES, URL_LIKE_VALUES, dataframe_for_columns


@pytest.mark.fuzz
@given(
    df=dataframe_for_columns(
        {
            "title": MIXED_SCALARS,
            "description_text": MIXED_SCALARS,
            "skills": ARRAY_LIKE_VALUES,
            "tech_stack": ARRAY_LIKE_VALUES,
            "apply_url": URL_LIKE_VALUES,
            "remote_flag": MIXED_SCALARS,
            "remote_type": MIXED_SCALARS,
            "location_raw": MIXED_SCALARS,
            "city": MIXED_SCALARS,
            "region": MIXED_SCALARS,
            "country": MIXED_SCALARS,
            "salary_max": MIXED_SCALARS,
            "salary_currency": MIXED_SCALARS,
            "ingested_at": TIMESTAMP_LIKE_VALUES,
            "last_seen": TIMESTAMP_LIKE_VALUES,
            "posted_at": TIMESTAMP_LIKE_VALUES,
        },
        max_rows=14,
    ),
    top_n=st.one_of(st.none(), st.integers(min_value=0, max_value=20)),
)
def test_fuzz_rank_jobs_no_crash_and_fit_bounds(df: pd.DataFrame, top_n: int | None) -> None:
    ranked = rank_jobs(
        df,
        profile=CandidateProfile(),
        use_llm_signals=False,
        top_n=top_n,
    )
    columns = DEFAULT_RESULT_COLUMNS

    assert columns.fit_score in ranked.columns
    numeric = pd.to_numeric(ranked[columns.fit_score], errors="coerce").dropna()
    assert ((numeric >= 0.0) & (numeric <= 1.0)).all()
    assert columns.fit_breakdown in ranked.columns
    assert columns.missing_requirements in ranked.columns
