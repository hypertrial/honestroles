import pandas as pd

from honestroles.rate.completeness import rate_completeness


def test_rate_completeness(sample_df: pd.DataFrame) -> None:
    rated = rate_completeness(sample_df)
    assert (rated["completeness_score"] > 0).all()
