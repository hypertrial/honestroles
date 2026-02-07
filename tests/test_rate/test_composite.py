import pandas as pd

from honestroles.rate.composite import rate_composite


def test_rate_composite(sample_df: pd.DataFrame) -> None:
    df = sample_df.copy()
    df["completeness_score"] = [0.5, 0.8]
    df["quality_score"] = [0.5, 0.2]
    rated = rate_composite(df)
    assert rated["rating"].tolist() == [0.5, 0.5]


def test_rate_composite_custom_weights(sample_df: pd.DataFrame) -> None:
    df = sample_df.copy()
    df["completeness_score"] = [1.0, 0.0]
    df["quality_score"] = [0.0, 1.0]
    rated = rate_composite(df, weights={"completeness_score": 0.8, "quality_score": 0.2})
    assert rated["rating"].tolist() == [0.8, 0.2]


def test_rate_composite_missing_columns(sample_df: pd.DataFrame) -> None:
    df = sample_df.copy()
    df["completeness_score"] = [0.5, 0.8]
    rated = rate_composite(df)
    assert "rating" in rated.columns


def test_rate_composite_zero_total_weight(sample_df: pd.DataFrame) -> None:
    df = sample_df.copy()
    df["completeness_score"] = [0.5, 0.8]
    rated = rate_composite(df, weights={"completeness_score": 0.0})
    assert rated.equals(df)
