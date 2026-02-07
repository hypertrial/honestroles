import pandas as pd

from honestroles.label import label_jobs


def test_label_jobs_integration(sample_df: pd.DataFrame) -> None:
    labeled = label_jobs(sample_df, use_llm=False)
    assert "seniority" in labeled.columns
    assert "role_category" in labeled.columns
    assert "tech_stack" in labeled.columns


def test_label_jobs_empty_dataframe(empty_df: pd.DataFrame) -> None:
    labeled = label_jobs(empty_df, use_llm=False)
    assert labeled.empty
