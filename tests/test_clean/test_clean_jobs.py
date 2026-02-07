import pandas as pd

from honestroles.clean import clean_jobs


def test_clean_jobs_integration(sample_df: pd.DataFrame) -> None:
    cleaned = clean_jobs(sample_df)
    assert "description_text" in cleaned.columns
    assert "city" in cleaned.columns
    assert "salary_min" in cleaned.columns


def test_clean_jobs_empty_dataframe(empty_df: pd.DataFrame) -> None:
    cleaned = clean_jobs(empty_df)
    assert cleaned.empty
