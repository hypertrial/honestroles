import pandas as pd

from honestroles.rate.composite import rate_composite


def test_rate_composite(sample_df: pd.DataFrame) -> None:
    df = sample_df.copy()
    df["completeness_score"] = [0.5, 0.8]
    df["quality_score"] = [0.5, 0.2]
    rated = rate_composite(df)
    assert rated["rating"].tolist() == [0.5, 0.5]
