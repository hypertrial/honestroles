import pandas as pd

from honestroles.filter.predicates import by_keywords, by_location, by_salary, by_skills


def test_by_location(sample_df: pd.DataFrame) -> None:
    mask = by_location(sample_df, cities=["New York"])
    assert mask.tolist() == [True, False]


def test_by_salary(sample_df: pd.DataFrame) -> None:
    sample_df = sample_df.copy()
    sample_df["salary_min"] = [120000, 80000]
    sample_df["salary_max"] = [150000, 90000]
    mask = by_salary(sample_df, min_salary=100000)
    assert mask.tolist() == [True, False]


def test_by_skills(sample_df: pd.DataFrame) -> None:
    mask = by_skills(sample_df, required=["python"])
    assert mask.tolist() == [True, False]


def test_by_keywords(sample_df: pd.DataFrame) -> None:
    mask = by_keywords(sample_df, include=["roadmap"])
    assert mask.tolist() == [False, True]
