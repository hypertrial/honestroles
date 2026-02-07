import pandas as pd

from honestroles.label.heuristic import label_role_category, label_seniority, label_tech_stack


def test_label_seniority(sample_df: pd.DataFrame) -> None:
    labeled = label_seniority(sample_df)
    assert labeled.loc[0, "seniority"] == "senior"


def test_label_role_category(sample_df: pd.DataFrame) -> None:
    labeled = label_role_category(sample_df)
    assert labeled.loc[0, "role_category"] == "engineering"
    assert labeled.loc[1, "role_category"] == "product"


def test_label_tech_stack(sample_df: pd.DataFrame) -> None:
    labeled = label_tech_stack(sample_df)
    assert "python" in labeled.loc[0, "tech_stack"]
