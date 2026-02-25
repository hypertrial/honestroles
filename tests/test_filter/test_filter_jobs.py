import pandas as pd

import honestroles.filter as filter_module
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


def test_filter_jobs_no_active_filters_skip_predicates(sample_df: pd.DataFrame, monkeypatch) -> None:
    def _raise(*args, **kwargs):
        raise AssertionError("Predicate should not be invoked")

    monkeypatch.setattr(filter_module, "by_location", _raise)
    monkeypatch.setattr(filter_module, "by_salary", _raise)
    monkeypatch.setattr(filter_module, "by_skills", _raise)
    monkeypatch.setattr(filter_module, "by_keywords", _raise)
    monkeypatch.setattr(filter_module, "by_completeness", _raise)
    monkeypatch.setattr(filter_module, "by_recency", _raise)

    filtered = filter_module.filter_jobs(sample_df)
    assert filtered.equals(sample_df.reset_index(drop=True))


def test_filter_jobs_no_filters_resets_index() -> None:
    df = pd.DataFrame(
        {
            "job_id": ["a", "b"],
            "title": ["Engineer", "PM"],
        },
        index=[10, 20],
    )
    filtered = filter_jobs(df)
    assert filtered["job_id"].tolist() == ["a", "b"]
    assert filtered.index.tolist() == [0, 1]


def test_filter_jobs_only_skills_filter_active(sample_df: pd.DataFrame) -> None:
    filtered = filter_jobs(sample_df, required_skills=["python"])
    assert filtered["job_id"].tolist() == ["1"]


def test_filter_jobs_required_fields_filter_active(sample_df: pd.DataFrame) -> None:
    df = sample_df.copy()
    df.loc[1, "apply_url"] = None
    filtered = filter_jobs(df, required_fields=["apply_url"])
    assert filtered["job_id"].tolist() == ["1"]


def test_filter_jobs_recency_filter_active() -> None:
    df = pd.DataFrame(
        {
            "job_id": ["1", "2"],
            "title": ["Role 1", "Role 2"],
            "posted_at": ["2025-01-10T00:00:00Z", "2024-11-01T00:00:00Z"],
            "apply_url": ["https://a.example/jobs/1", "https://a.example/jobs/2"],
        }
    )
    filtered = filter_jobs(
        df,
        posted_within_days=7,
        as_of="2025-01-10T00:00:00Z",
    )
    assert filtered["job_id"].tolist() == ["1"]
