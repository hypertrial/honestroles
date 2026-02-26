from __future__ import annotations

import pandas as pd
import pytest
from hypothesis import given

from honestroles.label.heuristic import label_role_category, label_seniority, label_tech_stack

from .invariants import assert_index_preserved, assert_list_of_strings_or_empty
from .strategies import ARRAY_LIKE_VALUES, TEXT_VALUES, dataframe_for_columns

_ALLOWED_SENIORITY = {
    None,
    "intern",
    "junior",
    "mid",
    "senior",
    "staff",
    "principal",
    "lead",
    "director",
    "vp",
    "c_level",
}

_ALLOWED_ROLE_CATEGORY = {
    None,
    "engineering",
    "data",
    "design",
    "product",
    "marketing",
    "sales",
    "operations",
    "finance",
    "hr",
    "legal",
    "support",
}


@pytest.mark.fuzz
@given(
    df=dataframe_for_columns(
        {
            "title": TEXT_VALUES,
            "description_text": TEXT_VALUES,
            "skills": ARRAY_LIKE_VALUES,
        },
        max_rows=14,
    )
)
def test_fuzz_label_heuristics_contracts(df: pd.DataFrame) -> None:
    with_seniority = label_seniority(df)
    with_role = label_role_category(with_seniority)
    with_tech = label_tech_stack(with_role)

    assert_index_preserved(df, with_tech)

    for value in with_tech["seniority"].tolist():
        assert pd.isna(value) or value in _ALLOWED_SENIORITY
    for value in with_tech["role_category"].tolist():
        assert pd.isna(value) or value in _ALLOWED_ROLE_CATEGORY

    assert_list_of_strings_or_empty(with_tech["tech_stack"])
