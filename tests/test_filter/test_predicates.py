import pandas as pd

from honestroles.filter.predicates import (
    by_completeness,
    by_keywords,
    by_location,
    by_salary,
    by_skills,
)


def test_by_location(sample_df: pd.DataFrame) -> None:
    mask = by_location(sample_df, cities=["New York"])
    assert mask.tolist() == [True, False]


def test_by_location_countries(sample_df: pd.DataFrame) -> None:
    df = sample_df.copy()
    df["country"] = ["US", "CA"]
    mask = by_location(df, countries=["CA"])
    assert mask.tolist() == [False, True]


def test_by_location_remote_only(sample_df: pd.DataFrame) -> None:
    df = sample_df.copy()
    df["remote_flag"] = [True, False]
    mask = by_location(df, remote_only=True)
    assert mask.tolist() == [True, False]


def test_by_location_fallback_location_raw(sample_df: pd.DataFrame) -> None:
    df = sample_df.drop(columns=["city"], errors="ignore")
    mask = by_location(df, cities=["new york"])
    assert mask.tolist() == [True, False]


def test_by_location_combined_filters(sample_df: pd.DataFrame) -> None:
    df = sample_df.copy()
    df["city"] = ["New York", "Toronto"]
    df["country"] = ["US", "CA"]
    mask = by_location(df, cities=["New York"], countries=["US"])
    assert mask.tolist() == [True, False]


def test_by_location_empty_lists(sample_df: pd.DataFrame) -> None:
    mask = by_location(sample_df, cities=[], regions=[], countries=[])
    assert mask.tolist() == [True, True]


def test_by_location_region(sample_df: pd.DataFrame) -> None:
    sample_df = sample_df.copy()
    sample_df["region"] = ["New York", "California"]
    mask = by_location(sample_df, regions=["California"])
    assert mask.tolist() == [False, True]


def test_by_salary(sample_df: pd.DataFrame) -> None:
    sample_df = sample_df.copy()
    sample_df["salary_min"] = [120000, 80000]
    sample_df["salary_max"] = [150000, 90000]
    mask = by_salary(sample_df, min_salary=100000)
    assert mask.tolist() == [True, False]


def test_by_salary_max_only(sample_df: pd.DataFrame) -> None:
    df = sample_df.copy()
    df["salary_min"] = [120000, 80000]
    df["salary_max"] = [150000, 90000]
    mask = by_salary(df, max_salary=100000)
    assert mask.tolist() == [False, True]


def test_by_salary_bounds_and_currency(sample_df: pd.DataFrame) -> None:
    df = sample_df.copy()
    df["salary_min"] = [120000, 80000]
    df["salary_max"] = [150000, 90000]
    df["salary_currency"] = ["USD", "CAD"]
    mask = by_salary(df, min_salary=100000, max_salary=160000, currency="USD")
    assert mask.tolist() == [True, False]


def test_by_salary_missing_columns(sample_df: pd.DataFrame) -> None:
    df = sample_df.drop(columns=["salary_min", "salary_max"], errors="ignore")
    mask = by_salary(df, min_salary=100000)
    assert mask.tolist() == [True, True]


def test_by_skills(sample_df: pd.DataFrame) -> None:
    mask = by_skills(sample_df, required=["python"])
    assert mask.tolist() == [True, False]


def test_by_skills_excluded(sample_df: pd.DataFrame) -> None:
    mask = by_skills(sample_df, excluded=["aws"])
    assert mask.tolist() == [False, True]


def test_by_skills_required_and_excluded(sample_df: pd.DataFrame) -> None:
    mask = by_skills(sample_df, required=["python"], excluded=["aws"])
    assert mask.tolist() == [False, False]


def test_by_skills_missing_column(sample_df: pd.DataFrame) -> None:
    df = sample_df.drop(columns=["skills"])
    mask = by_skills(df, required=["python"])
    assert mask.tolist() == [True, True]


def test_by_skills_none_values() -> None:
    df = pd.DataFrame({"skills": [None, float("nan"), ["Python"]]})
    mask = by_skills(df, required=["python"])
    assert mask.tolist() == [False, False, True]


def test_by_skills_scalar_values() -> None:
    df = pd.DataFrame({"skills": ["Python", "Roadmapping"]})
    mask = by_skills(df, required=["python"])
    assert mask.tolist() == [True, False]


def test_by_keywords(sample_df: pd.DataFrame) -> None:
    mask = by_keywords(sample_df, include=["roadmap"])
    assert mask.tolist() == [False, True]


def test_by_keywords_exclude(sample_df: pd.DataFrame) -> None:
    mask = by_keywords(sample_df, exclude=["systems"])
    assert mask.tolist() == [False, True]


def test_by_keywords_custom_columns(sample_df: pd.DataFrame) -> None:
    df = sample_df.copy()
    df["notes"] = ["Important note", "Other"]
    mask = by_keywords(df, include=["important"], columns=["notes"])
    assert mask.tolist() == [True, False]


def test_by_keywords_missing_columns(sample_df: pd.DataFrame) -> None:
    df = sample_df.drop(columns=["title", "description_text"], errors="ignore")
    mask = by_keywords(df, include=["roadmap"])
    assert mask.tolist() == [True, True]


def test_by_keywords_no_terms(sample_df: pd.DataFrame) -> None:
    mask = by_keywords(sample_df, include=[], exclude=[])
    assert mask.tolist() == [True, True]


def test_by_completeness_basic(sample_df: pd.DataFrame) -> None:
    df = sample_df.copy()
    mask = by_completeness(df, required_fields=["title", "apply_url"])
    assert mask.tolist() == [True, True]


def test_by_completeness_missing_fields(sample_df: pd.DataFrame) -> None:
    df = sample_df.copy()
    mask = by_completeness(df, required_fields=["missing_field"])
    assert mask.tolist() == [True, True]


def test_by_completeness_partial_presence(sample_df: pd.DataFrame) -> None:
    df = sample_df.copy()
    df.loc[1, "apply_url"] = None
    mask = by_completeness(df, required_fields=["apply_url"])
    assert mask.tolist() == [True, False]
