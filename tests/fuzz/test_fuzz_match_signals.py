from __future__ import annotations

import pandas as pd
import pytest
from hypothesis import given

from honestroles.match.models import DEFAULT_RESULT_COLUMNS
from honestroles.match.signals import extract_job_signals

from .strategies import (
    ARRAY_LIKE_VALUES,
    MIXED_SCALARS,
    TIMESTAMP_LIKE_VALUES,
    URL_LIKE_VALUES,
    dataframe_for_columns,
)


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
            "last_seen": TIMESTAMP_LIKE_VALUES,
            "ingested_at": TIMESTAMP_LIKE_VALUES,
            "posted_at": TIMESTAMP_LIKE_VALUES,
            "snapshot_count": MIXED_SCALARS,
        },
        max_rows=10,
    )
)
def test_fuzz_extract_job_signals_no_crash_and_numeric_bounds(df: pd.DataFrame) -> None:
    result = extract_job_signals(df, use_llm=False)
    columns = DEFAULT_RESULT_COLUMNS

    assert len(result) == len(df)
    assert result.index.equals(df.index)

    for column in [
        columns.required_skills_extracted,
        columns.preferred_skills_extracted,
        columns.experience_years_min,
        columns.experience_years_max,
        columns.entry_level_likely,
        columns.visa_sponsorship_signal,
        columns.application_friction_score,
        columns.role_clarity_score,
        columns.signal_confidence,
        columns.signal_source,
        columns.active_likelihood,
        columns.active_reason,
    ]:
        assert column in result.columns

    for score_column in [
        columns.application_friction_score,
        columns.role_clarity_score,
        columns.signal_confidence,
        columns.active_likelihood,
    ]:
        numeric = pd.to_numeric(result[score_column], errors="coerce").dropna()
        assert ((numeric >= 0.0) & (numeric <= 1.0)).all()

