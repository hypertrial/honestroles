import pandas as pd

from honestroles.clean.normalize import (
    normalize_employment_types,
    normalize_locations,
    normalize_salaries,
)


def test_normalize_locations(sample_df: pd.DataFrame) -> None:
    normalized = normalize_locations(sample_df)
    assert normalized.loc[0, "city"] == "New York"
    assert normalized.loc[0, "country"] == "USA"


def test_normalize_salaries(sample_df: pd.DataFrame) -> None:
    normalized = normalize_salaries(sample_df)
    assert normalized.loc[0, "salary_min"] == 120000.0
    assert normalized.loc[0, "salary_max"] == 150000.0


def test_normalize_employment_types(sample_df: pd.DataFrame) -> None:
    df = sample_df.copy()
    df["employment_type"] = ["Full Time", "Contract"]
    normalized = normalize_employment_types(df)
    assert normalized.loc[0, "employment_type"] == "full_time"
    assert normalized.loc[1, "employment_type"] == "contract"
