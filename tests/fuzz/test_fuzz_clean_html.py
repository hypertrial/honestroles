from __future__ import annotations

import pandas as pd
import pytest
from hypothesis import given

from honestroles.clean.html import strip_html

from .strategies import MIXED_SCALARS, dataframe_for_columns


@pytest.mark.fuzz
@given(
    df=dataframe_for_columns(
        {
            "description_html": MIXED_SCALARS,
            "description_text": MIXED_SCALARS,
        },
        min_rows=1,
        max_rows=12,
    )
)
def test_fuzz_strip_html_handles_malformed_values(
    df: pd.DataFrame,
) -> None:
    result = strip_html(df, overwrite_existing=True)
    assert len(result) == len(df)
    assert result.index.equals(df.index)
    assert "description_text" in result.columns

    parse_positions = [
        pos
        for pos, value in enumerate(df["description_html"].tolist())
        if isinstance(value, str) and value.strip() != ""
    ]
    for pos in parse_positions:
        value = result.iloc[pos]["description_text"]
        assert value is None or isinstance(value, str)
