import pandas as pd

from honestroles.io import read_parquet, write_parquet


def test_parquet_roundtrip(tmp_path, sample_df: pd.DataFrame) -> None:
    path = tmp_path / "jobs.parquet"
    write_parquet(sample_df, path)
    loaded = read_parquet(path)
    assert len(loaded) == len(sample_df)
    assert loaded["job_key"].tolist() == sample_df["job_key"].tolist()


def test_read_parquet_missing_file(tmp_path) -> None:
    path = tmp_path / "missing.parquet"
    try:
        read_parquet(path)
    except FileNotFoundError as exc:
        assert "Parquet file not found" in str(exc)
    else:
        raise AssertionError("Expected FileNotFoundError")


def test_read_parquet_validate_false(tmp_path, sample_df: pd.DataFrame) -> None:
    path = tmp_path / "jobs.parquet"
    write_parquet(sample_df, path)
    loaded = read_parquet(path, validate=False)
    assert len(loaded) == len(sample_df)


def test_write_parquet_creates_parent(tmp_path, sample_df: pd.DataFrame) -> None:
    path = tmp_path / "nested" / "jobs.parquet"
    write_parquet(sample_df, path)
    assert path.exists()
