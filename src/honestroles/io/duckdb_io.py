from __future__ import annotations

import re

import duckdb
import pandas as pd

from honestroles.io.dataframe import validate_dataframe

_TABLE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _is_query(table_or_query: str) -> bool:
    lowered = table_or_query.strip().lower()
    return lowered.startswith("select") or " from " in lowered or ";" in lowered


def read_duckdb(
    conn: duckdb.DuckDBPyConnection,
    table_or_query: str,
    validate: bool = True,
) -> pd.DataFrame:
    if _is_query(table_or_query):
        df = conn.execute(table_or_query).fetchdf()
    else:
        df = conn.execute(f"SELECT * FROM {table_or_query}").fetchdf()
    if validate:
        validate_dataframe(df)
    return df


def write_duckdb(
    df: pd.DataFrame,
    conn: duckdb.DuckDBPyConnection,
    table: str,
    *,
    overwrite: bool = True,
) -> None:
    if not _TABLE_RE.match(table):
        raise ValueError(f"Invalid table name: {table}")
    temp_name = "__honestroles_df"
    conn.register(temp_name, df)
    clause = "OR REPLACE " if overwrite else ""
    conn.execute(f"CREATE {clause}TABLE {table} AS SELECT * FROM {temp_name}")
    conn.unregister(temp_name)
