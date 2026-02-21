import duckdb
import pandas as pd
import pytest

import honestroles as hr
import honestroles.io as io_module
from honestroles.io import (
    read_duckdb_query,
    read_duckdb_table,
    validate_source_data_contract,
    write_duckdb,
)
from honestroles.io.duckdb_io import _coerce_duckdb_compatible


def test_duckdb_roundtrip(sample_df: pd.DataFrame) -> None:
    conn = duckdb.connect()
    write_duckdb(sample_df, conn, "jobs")
    loaded = read_duckdb_table(conn, "jobs")
    assert len(loaded) == len(sample_df)
    assert set(loaded["job_key"]) == set(sample_df["job_key"])


def test_read_duckdb_query(sample_df: pd.DataFrame) -> None:
    conn = duckdb.connect()
    write_duckdb(sample_df, conn, "jobs")
    loaded = read_duckdb_query(conn, "select * from jobs where job_id = '1'")
    assert loaded["job_id"].tolist() == ["1"]


def test_read_duckdb_query_trailing_semicolon(sample_df: pd.DataFrame) -> None:
    conn = duckdb.connect()
    write_duckdb(sample_df, conn, "jobs")
    loaded = read_duckdb_query(conn, "select * from jobs where job_id = '1';")
    assert loaded["job_id"].tolist() == ["1"]


def test_read_duckdb_rejects_multi_statement_query(sample_df: pd.DataFrame) -> None:
    conn = duckdb.connect()
    write_duckdb(sample_df, conn, "jobs")
    with pytest.raises(ValueError, match="single SELECT query"):
        read_duckdb_query(conn, "select * from jobs; drop table jobs;")
    loaded = read_duckdb_table(conn, "jobs")
    assert len(loaded) == len(sample_df)


def test_read_duckdb_rejects_non_select_query(sample_df: pd.DataFrame) -> None:
    conn = duckdb.connect()
    write_duckdb(sample_df, conn, "jobs")
    with pytest.raises(ValueError, match="Only SELECT queries are supported"):
        read_duckdb_query(conn, "drop table jobs")


def test_read_duckdb_rejects_empty_query() -> None:
    conn = duckdb.connect()
    with pytest.raises(ValueError, match="non-empty"):
        read_duckdb_query(conn, "   ")


def test_read_duckdb_rejects_select_with_forbidden_tokens(sample_df: pd.DataFrame) -> None:
    conn = duckdb.connect()
    write_duckdb(sample_df, conn, "jobs")
    with pytest.raises(ValueError, match="read-only"):
        read_duckdb_query(conn, "select * from jobs where title = 'ok' drop")


def test_write_duckdb_unregisters_temp_table_on_failure(sample_df: pd.DataFrame) -> None:
    class _FailingConn:
        def __init__(self) -> None:
            self.registered: list[str] = []
            self.unregistered: list[str] = []

        def register(self, name: str, df: pd.DataFrame) -> None:
            self.registered.append(name)

        def execute(self, query: str):  # type: ignore[no-untyped-def]
            raise RuntimeError("simulated create failure")

        def unregister(self, name: str) -> None:
            self.unregistered.append(name)

    conn = _FailingConn()
    with pytest.raises(RuntimeError, match="simulated create failure"):
        write_duckdb(sample_df, conn, "jobs_failure")  # type: ignore[arg-type]
    assert conn.registered == ["__honestroles_df"]
    assert conn.unregistered == ["__honestroles_df"]


def test_removed_deprecated_read_duckdb_api() -> None:
    assert not hasattr(io_module, "read_duckdb")
    assert not hasattr(hr, "read_duckdb")


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
    loaded = read_duckdb_table(conn, "jobs_copy")
    assert len(loaded) == len(sample_df)


def test_read_duckdb_validate_false(sample_df: pd.DataFrame) -> None:
    conn = duckdb.connect()
    write_duckdb(sample_df, conn, "jobs")
    loaded = read_duckdb_table(conn, "jobs", validate=False)
    assert len(loaded) == len(sample_df)


def test_duckdb_contract_validation_with_extra_columns_and_arrays(
    minimal_df: pd.DataFrame,
) -> None:
    conn = duckdb.connect()
    df = minimal_df.copy()
    df["skills"] = [["Python", "SQL"]]
    df["source_data_debug"] = ["trace-1"]

    write_duckdb(df, conn, "jobs_contract")
    loaded = read_duckdb_table(conn, "jobs_contract", validate=False)
    validate_source_data_contract(loaded)

    assert loaded["source_data_debug"].tolist() == ["trace-1"]
    assert list(loaded["skills"].iloc[0]) == ["Python", "SQL"]


def test_coerce_duckdb_compatible_no_copy_when_not_needed() -> None:
    df = pd.DataFrame({"a": ["x", "y"]}).astype({"a": "object"})
    coerced = _coerce_duckdb_compatible(df)
    assert coerced is df


def test_coerce_duckdb_compatible_converts_string_extension_dtype() -> None:
    df = pd.DataFrame({"a": pd.Series(["x", "y"], dtype="string")})
    coerced = _coerce_duckdb_compatible(df)
    assert coerced is not df
    assert str(coerced["a"].dtype) == "object"
