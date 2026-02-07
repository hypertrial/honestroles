import duckdb
import pandas as pd

from honestroles.io import read_duckdb, write_duckdb


def test_duckdb_roundtrip(sample_df: pd.DataFrame) -> None:
    conn = duckdb.connect()
    write_duckdb(sample_df, conn, "jobs")
    loaded = read_duckdb(conn, "jobs")
    assert len(loaded) == len(sample_df)
    assert set(loaded["job_key"]) == set(sample_df["job_key"])


def test_read_duckdb_query(sample_df: pd.DataFrame) -> None:
    conn = duckdb.connect()
    write_duckdb(sample_df, conn, "jobs")
    loaded = read_duckdb(conn, "select * from jobs where job_id = '1'")
    assert loaded["job_id"].tolist() == ["1"]


def test_write_duckdb_invalid_table_name(sample_df: pd.DataFrame) -> None:
    conn = duckdb.connect()
    try:
        write_duckdb(sample_df, conn, "jobs;drop")
    except ValueError as exc:
        assert "Invalid table name" in str(exc)
    else:
        raise AssertionError("Expected ValueError for invalid table name")


def test_write_duckdb_overwrite_false(sample_df: pd.DataFrame) -> None:
    conn = duckdb.connect()
    write_duckdb(sample_df, conn, "jobs", overwrite=True)
    write_duckdb(sample_df, conn, "jobs_copy", overwrite=False)
    loaded = read_duckdb(conn, "jobs_copy")
    assert len(loaded) == len(sample_df)


def test_read_duckdb_validate_false(sample_df: pd.DataFrame) -> None:
    conn = duckdb.connect()
    write_duckdb(sample_df, conn, "jobs")
    loaded = read_duckdb(conn, "jobs", validate=False)
    assert len(loaded) == len(sample_df)
