import pandas as pd

from honestroles.io import read_parquet, write_parquet


def test_parquet_roundtrip(tmp_path, sample_df: pd.DataFrame) -> None:
    path = tmp_path / "jobs.parquet"
    write_parquet(sample_df, path)
    loaded = read_parquet(path)
    assert len(loaded) == len(sample_df)
    assert loaded["job_key"].tolist() == sample_df["job_key"].tolist()
