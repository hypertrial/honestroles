from __future__ import annotations

import pandas as pd
import pytest
from hypothesis import given
from hypothesis import strategies as st

from honestroles.match.models import DEFAULT_RESULT_COLUMNS
from honestroles.match.rank import build_application_plan

from .strategies import ARRAY_LIKE_VALUES, MIXED_SCALARS, dataframe_for_columns


@pytest.mark.fuzz
@given(
    ranked_df=dataframe_for_columns(
        {
            "title": MIXED_SCALARS,
            "description_text": MIXED_SCALARS,
            "fit_score": st.floats(min_value=0.0, max_value=1.0, allow_nan=False),
            "missing_requirements": ARRAY_LIKE_VALUES,
            "role_category": MIXED_SCALARS,
            "application_friction_score": st.one_of(
                st.none(),
                st.floats(min_value=0.0, max_value=1.0, allow_nan=False),
            ),
            "active_likelihood": st.one_of(
                st.none(),
                st.floats(min_value=0.0, max_value=1.0, allow_nan=False),
            ),
            "signal_confidence": st.one_of(
                st.none(),
                st.floats(min_value=0.0, max_value=1.0, allow_nan=False),
            ),
            "visa_sponsorship_signal": MIXED_SCALARS,
            "citizenship_required": MIXED_SCALARS,
            "work_authorization_required": MIXED_SCALARS,
        },
        max_rows=14,
    ),
    top_n=st.integers(min_value=0, max_value=20),
)
def test_fuzz_build_application_plan_no_crash_and_contract(
    ranked_df: pd.DataFrame,
    top_n: int,
) -> None:
    planned = build_application_plan(
        ranked_df,
        top_n=top_n,
        include_diagnostics=True,
    )
    columns = DEFAULT_RESULT_COLUMNS

    assert columns.next_actions in planned.columns
    for actions in planned[columns.next_actions].tolist():
        assert isinstance(actions, list)
        assert all(isinstance(item, str) for item in actions)

    assert columns.application_effort_minutes in planned.columns
    effort = pd.to_numeric(planned[columns.application_effort_minutes], errors="coerce").dropna()
    if not effort.empty:
        assert (effort >= 10).all()
