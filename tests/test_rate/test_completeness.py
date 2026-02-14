import pandas as pd

from honestroles.rate.completeness import rate_completeness


def test_rate_completeness(sample_df: pd.DataFrame) -> None:
    rated = rate_completeness(sample_df)
    assert (rated["completeness_score"] > 0).all()


def test_rate_completeness_all_fields_present() -> None:
    df = pd.DataFrame(
        [
            {
                "company": "Acme",
                "title": "Engineer",
                "location_raw": "Remote",
                "apply_url": "https://example.com",
                "description_text": "Text",
                "salary_min": 100000,
                "skills": ["Python"],
                "benefits": ["Health"],
            }
        ]
    )
    rated = rate_completeness(df)
    assert rated.loc[0, "completeness_score"] == 1.0


def test_rate_completeness_no_fields_present() -> None:
    df = pd.DataFrame([{"company": None, "title": ""}])
    rated = rate_completeness(df, required_fields=["company", "title"])
    assert rated.loc[0, "completeness_score"] == 0.0


def test_rate_completeness_custom_required_fields() -> None:
    df = pd.DataFrame([{"company": "Acme", "title": None}])
    rated = rate_completeness(df, required_fields=["company", "title"])
    assert rated.loc[0, "completeness_score"] == 0.5


def test_rate_completeness_empty_values_not_counted() -> None:
    df = pd.DataFrame([{"company": "", "skills": [], "benefits": None}])
    rated = rate_completeness(df, required_fields=["company", "skills", "benefits"])
    assert rated.loc[0, "completeness_score"] == 0.0


def test_rate_completeness_no_overlapping_required_fields_returns_input() -> None:
    df = pd.DataFrame([{"company": "Acme"}])
    rated = rate_completeness(df, required_fields=["missing_one", "missing_two"])
    assert rated.equals(df)
