import pandas as pd

from honestroles.clean import strip_html


def test_strip_html(sample_df: pd.DataFrame) -> None:
    cleaned = strip_html(sample_df)
    assert "description_text" in cleaned.columns
    assert cleaned.loc[0, "description_text"].startswith("Build")
