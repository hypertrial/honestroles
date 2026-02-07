import pandas as pd

from honestroles.rate import rate_jobs


def test_rate_jobs_integration(sample_df: pd.DataFrame) -> None:
    rated = rate_jobs(sample_df, use_llm=False)
    assert "completeness_score" in rated.columns
    assert "quality_score" in rated.columns
    assert "rating" in rated.columns


def test_rate_jobs_empty_dataframe(empty_df: pd.DataFrame) -> None:
    rated = rate_jobs(empty_df, use_llm=False)
    assert rated.empty
