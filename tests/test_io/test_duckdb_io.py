import duckdb
import pandas as pd

from honestroles.io import read_duckdb, write_duckdb


def test_duckdb_roundtrip(sample_df: pd.DataFrame) -> None:
    conn = duckdb.connect()
    write_duckdb(sample_df, conn, "jobs")
    loaded = read_duckdb(conn, "jobs")
    assert len(loaded) == len(sample_df)
    assert set(loaded["job_key"]) == set(sample_df["job_key"])
