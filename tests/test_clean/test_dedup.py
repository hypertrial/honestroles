import pandas as pd

from honestroles.clean.dedup import deduplicate


def test_deduplicate_by_content_hash(sample_df: pd.DataFrame) -> None:
    duplicated = pd.concat([sample_df, sample_df.iloc[[0]]], ignore_index=True)
    result = deduplicate(duplicated)
    assert len(result) == len(sample_df)
