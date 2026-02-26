from __future__ import annotations

import pandas as pd
import pytest
from hypothesis import given

from honestroles.match.models import DEFAULT_RESULT_COLUMNS
from honestroles.match.rank import build_application_plan, rank_jobs
from honestroles.match.signals import extract_job_signals

from .strategies import ARRAY_LIKE_VALUES, MIXED_SCALARS, TIMESTAMP_LIKE_VALUES, URL_LIKE_VALUES, dataframe_for_columns


@pytest.mark.fuzz
@given(
    df=dataframe_for_columns(
        {
            "title": MIXED_SCALARS,
            "description_text": MIXED_SCALARS,
            "skills": ARRAY_LIKE_VALUES,
            "apply_url": URL_LIKE_VALUES,
            "remote_flag": MIXED_SCALARS,
            "remote_type": MIXED_SCALARS,
            "ingested_at": TIMESTAMP_LIKE_VALUES,
            "last_seen": TIMESTAMP_LIKE_VALUES,
            "posted_at": TIMESTAMP_LIKE_VALUES,
        },
        max_rows=10,
    )
)
def test_fuzz_match_pipeline_integration(df: pd.DataFrame) -> None:
    signals = extract_job_signals(df, use_llm=False)
    ranked = rank_jobs(signals, use_llm_signals=False)
    planned = build_application_plan(ranked, top_n=10)

    columns = DEFAULT_RESULT_COLUMNS
    assert len(signals) == len(df)
    assert len(ranked) <= len(df)
    assert len(planned) <= len(ranked)

    assert columns.fit_score in ranked.columns
    assert columns.next_actions in planned.columns
