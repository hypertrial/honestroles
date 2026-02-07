import pandas as pd

from honestroles.rate.quality import rate_quality


def test_rate_quality(sample_df: pd.DataFrame) -> None:
    rated = rate_quality(sample_df)
    assert "quality_score" in rated.columns
    assert rated["quality_score"].max() <= 1.0


def test_rate_quality_empty_description() -> None:
    df = pd.DataFrame({"description_text": [None, ""]})
    rated = rate_quality(df)
    assert rated["quality_score"].tolist() == [0.0, 0.0]


def test_rate_quality_long_description() -> None:
    df = pd.DataFrame({"description_text": ["x" * 5000]})
    rated = rate_quality(df)
    assert rated.loc[0, "quality_score"] <= 1.0
    assert rated.loc[0, "quality_score"] > 0.7


def test_rate_quality_bullet_bonus() -> None:
    df = pd.DataFrame({"description_text": ["- item one\n- item two"]})
    rated = rate_quality(df)
    assert rated.loc[0, "quality_score"] > 0.0


def test_rate_quality_missing_column(sample_df: pd.DataFrame) -> None:
    df = sample_df.drop(columns=["description_text"])
    rated = rate_quality(df)
    assert rated.equals(df)
