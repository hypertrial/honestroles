import pandas as pd

from honestroles.rate.quality import rate_quality


def test_rate_quality(sample_df: pd.DataFrame) -> None:
    rated = rate_quality(sample_df)
    assert "quality_score" in rated.columns
    assert rated["quality_score"].max() <= 1.0
