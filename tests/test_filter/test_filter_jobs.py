import pandas as pd

from honestroles.filter import filter_jobs


def test_filter_jobs_integration(sample_df: pd.DataFrame) -> None:
    df = sample_df.copy()
    df["salary_min"] = [120000, 80000]
    df["salary_max"] = [150000, 90000]
    df["country"] = ["US", "US"]
    df["city"] = ["New York", "Remote"]
    filtered = filter_jobs(
        df,
        cities=["New York"],
        min_salary=100000,
        include_keywords=["systems"],
    )
    assert filtered["job_id"].tolist() == ["1"]


def test_filter_jobs_no_filters_returns_all(sample_df: pd.DataFrame) -> None:
    filtered = filter_jobs(sample_df)
    assert filtered.equals(sample_df)


def test_filter_jobs_no_filters_with_salary_columns_returns_all(sample_df: pd.DataFrame) -> None:
    df = sample_df.copy()
    df["salary_min"] = [120000, 80000]
    df["salary_max"] = [150000, 90000]
    df["salary_currency"] = ["CAD", None]
    filtered = filter_jobs(df)
    assert filtered.equals(df)


def test_filter_jobs_empty_dataframe(empty_df: pd.DataFrame) -> None:
    filtered = filter_jobs(empty_df)
    assert filtered.empty
