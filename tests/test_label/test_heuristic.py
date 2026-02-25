import pandas as pd
import pytest

import honestroles.label.heuristic as heuristic_module
from honestroles.label.heuristic import label_role_category, label_seniority, label_tech_stack


def test_label_seniority(sample_df: pd.DataFrame) -> None:
    labeled = label_seniority(sample_df)
    assert labeled.loc[0, "seniority"] == "senior"


@pytest.mark.parametrize(
    ("title", "expected"),
    [
        ("Intern Software Engineer", "intern"),
        ("Junior Engineer", "junior"),
        ("Jr Developer", "junior"),
        ("Jr. Developer", "junior"),
        ("Mid-level Developer", "mid"),
        ("Senior Engineer", "senior"),
        ("Sr Software Engineer", "senior"),
        ("Sr. Software Engineer", "senior"),
        ("Staff Engineer", "staff"),
        ("Principal Engineer", "principal"),
        ("Tech Lead", "lead"),
        ("Director of Engineering", "director"),
        ("VP of Engineering", "vp"),
        ("Chief Technology Officer", "c_level"),
    ],
)
def test_label_seniority_levels(title: str, expected: str) -> None:
    df = pd.DataFrame({"title": [title]})
    labeled = label_seniority(df)
    assert labeled.loc[0, "seniority"] == expected


def test_label_seniority_missing_title_column(sample_df: pd.DataFrame) -> None:
    df = sample_df.drop(columns=["title"])
    labeled = label_seniority(df)
    assert labeled.equals(df)


def test_label_seniority_empty_title() -> None:
    df = pd.DataFrame({"title": [None, ""]})
    labeled = label_seniority(df)
    assert labeled["seniority"].tolist() == [None, None]


def test_label_seniority_explicit_none_object_title() -> None:
    df = pd.DataFrame({"title": pd.Series([None], dtype="object")})
    labeled = label_seniority(df)
    assert labeled["seniority"].tolist() == [None]


def test_label_seniority_nan_and_non_string_title() -> None:
    df = pd.DataFrame({"title": [float("nan"), 123]})
    labeled = label_seniority(df)
    assert labeled["seniority"].tolist() == [None, None]


def test_label_seniority_preserves_existing_values() -> None:
    df = pd.DataFrame({"title": ["Engineer"], "seniority": ["staff"]})
    labeled = label_seniority(df)
    assert labeled.loc[0, "seniority"] == "staff"


def test_label_role_category(sample_df: pd.DataFrame) -> None:
    labeled = label_role_category(sample_df)
    assert labeled.loc[0, "role_category"] == "engineering"
    assert labeled.loc[1, "role_category"] == "product"


@pytest.mark.parametrize(
    ("title", "expected"),
    [
        ("Data Scientist", "data"),
        ("Product Designer", "design"),
        ("Marketing Manager", "marketing"),
        ("Account Executive", "sales"),
        ("DevOps Engineer", "engineering"),
        ("Finance Analyst", "finance"),
        ("Talent Partner", "hr"),
        ("Legal Counsel", "legal"),
        ("Customer Support Specialist", "support"),
    ],
)
def test_label_role_category_multiple(title: str, expected: str) -> None:
    df = pd.DataFrame({"title": [title], "description_text": [""]})
    labeled = label_role_category(df)
    assert labeled.loc[0, "role_category"] == expected


def test_label_role_category_missing_columns(sample_df: pd.DataFrame) -> None:
    df = sample_df.drop(columns=["title", "description_text"])
    labeled = label_role_category(df)
    assert labeled.equals(df)


def test_label_role_category_unmatched_returns_none() -> None:
    df = pd.DataFrame({"title": ["Astronaut"], "description_text": ["Space travel."]})
    labeled = label_role_category(df)
    assert labeled.loc[0, "role_category"] is None


def test_label_role_category_avoids_pm_substring_false_positive() -> None:
    df = pd.DataFrame({"title": ["Development Coordinator"], "description_text": ["Coordinate teams."]})
    labeled = label_role_category(df)
    assert labeled.loc[0, "role_category"] is None


def test_label_role_category_prefers_title_signal_over_description() -> None:
    df = pd.DataFrame(
        {
            "title": ["Data Engineer"],
            "description_text": ["Responsibilities include product management."],
        }
    )
    labeled = label_role_category(df)
    assert labeled.loc[0, "role_category"] == "engineering"


def test_label_role_category_description_loop_breaks_after_resolution() -> None:
    df = pd.DataFrame({"title": ["", ""], "description_text": ["software engineer", "backend engineer"]})
    labeled = label_role_category(df)
    assert labeled["role_category"].tolist() == ["engineering", "engineering"]


def test_label_tech_stack(sample_df: pd.DataFrame) -> None:
    labeled = label_tech_stack(sample_df)
    assert "python" in labeled.loc[0, "tech_stack"]


def test_label_tech_stack_from_description() -> None:
    df = pd.DataFrame({"description_text": ["We use Python and AWS."]})
    labeled = label_tech_stack(df)
    assert labeled.loc[0, "tech_stack"] == ["aws", "python"]


def test_label_tech_stack_empty_values() -> None:
    df = pd.DataFrame({"skills": [[]], "description_text": [None]})
    labeled = label_tech_stack(df)
    assert labeled.loc[0, "tech_stack"] == []


def test_label_tech_stack_skills_value_edge_types() -> None:
    df = pd.DataFrame(
        {
            "skills": [None, float("nan"), "", 123],
            "description_text": ["", "", "", ""],
        }
    )
    labeled = label_tech_stack(df)
    assert labeled["tech_stack"].tolist() == [[], [], [], []]


def test_label_tech_stack_normalizes_skill_aliases() -> None:
    df = pd.DataFrame({"skills": ["Python, Amazon Web Services"], "description_text": ["Use ReactJS"]})
    labeled = label_tech_stack(df)
    assert labeled.loc[0, "tech_stack"] == ["aws", "python", "react"]


def test_extract_tech_helpers_cover_empty_and_canonical_drop(monkeypatch) -> None:
    assert heuristic_module._extract_tech_terms("") == set()
    assert heuristic_module._extract_tech_terms("Use Python and SQL") == {"python", "sql"}
    empty_series = heuristic_module._extract_tech_terms_series(pd.Series([], dtype="string"))
    assert empty_series.empty

    monkeypatch.setattr(heuristic_module, "_normalize_tech_token", lambda _: None)
    dropped = heuristic_module._extract_tech_terms_series(pd.Series(["python"], dtype="string"))
    assert dropped.loc[0] == set()
