from __future__ import annotations

import re

import duckdb
import pandas as pd

from honestroles.io.dataframe import validate_dataframe

_TABLE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SELECT_QUERY_RE = re.compile(r"^\s*(?:select|with)\b", re.IGNORECASE)
_FORBIDDEN_QUERY_TOKENS_RE = re.compile(
    r"\b(?:insert|update|delete|drop|alter|create|attach|detach|copy|call|pragma|"
    r"replace|truncate|vacuum)\b",
    re.IGNORECASE,
)


def _validate_table_name(table: str) -> str:
    candidate = table.strip()
    if not _TABLE_RE.match(candidate):
        raise ValueError(f"Invalid table name: {table}")
    return candidate


def _validate_read_query(query: str) -> str:
    cleaned = query.strip()
    if not cleaned:
        raise ValueError("Query must be a non-empty string.")
    if cleaned.endswith(";"):
        cleaned = cleaned[:-1].strip()
    if ";" in cleaned:
        raise ValueError("Only a single SELECT query is supported.")
    if not _SELECT_QUERY_RE.match(cleaned):
        raise ValueError("Only SELECT queries are supported.")
    if _FORBIDDEN_QUERY_TOKENS_RE.search(cleaned):
        raise ValueError("Read queries must be read-only.")
    return cleaned


def read_duckdb_table(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    validate: bool = True,
) -> pd.DataFrame:
    table_name = _validate_table_name(table)
    df = conn.execute(f"SELECT * FROM {table_name}").fetchdf()
    if validate:
        validate_dataframe(df)
    return df


def read_duckdb_query(
    conn: duckdb.DuckDBPyConnection,
    query: str,
    validate: bool = True,
) -> pd.DataFrame:
    safe_query = _validate_read_query(query)
    df = conn.execute(safe_query).fetchdf()
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
    table_name = _validate_table_name(table)
    temp_name = "__honestroles_df"
    conn.register(temp_name, df)
    try:
        clause = "OR REPLACE " if overwrite else ""
        conn.execute(f"CREATE {clause}TABLE {table_name} AS SELECT * FROM {temp_name}")
    finally:
        conn.unregister(temp_name)
