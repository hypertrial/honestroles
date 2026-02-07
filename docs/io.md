## IO

`honestroles.io` provides read/write helpers for Parquet and DuckDB, plus a
minimal DataFrame validation utility.

### Modules

- `parquet.py`: Parquet read/write.
- `duckdb_io.py`: DuckDB read/write.
- `dataframe.py`: `validate_dataframe`.

### Public API reference

#### `read_parquet(path: str | Path, validate: bool = True) -> pd.DataFrame`

Reads a Parquet file. Raises `FileNotFoundError` if the path does not exist.
When `validate=True`, checks required columns.

#### `write_parquet(df: pd.DataFrame, path: str | Path) -> None`

Writes a Parquet file, creating parent directories if needed.

#### `read_duckdb(conn: duckdb.DuckDBPyConnection, table_or_query: str, validate: bool = True) -> pd.DataFrame`

Reads from DuckDB by table name or SQL query. When `validate=True`, checks
required columns.

#### `write_duckdb(df: pd.DataFrame, conn: duckdb.DuckDBPyConnection, table: str, *, overwrite: bool = True) -> None`

Writes a DataFrame to a DuckDB table. Validates table name using a regex.
Uses `CREATE OR REPLACE` when `overwrite=True`.

#### `validate_dataframe(df: pd.DataFrame, required_columns: Iterable[str] | None = None) -> pd.DataFrame`

Ensures required columns are present (defaults to `schema.REQUIRED_COLUMNS`).
Raises `ValueError` if any are missing.

### Usage examples

```python
import honestroles as hr

df = hr.read_parquet("jobs_current.parquet")
hr.write_parquet(df, "jobs_clean.parquet")
```

```python
import duckdb
from honestroles.io import read_duckdb, write_duckdb

conn = duckdb.connect()
df = read_duckdb(conn, "jobs_current")
write_duckdb(df, conn, "jobs_scored", overwrite=True)
```

### Design notes

- Validation is intentionally lightweight and focuses on required columns.
- `read_duckdb` treats any string containing `select`, ` from `, or `;` as a query.
- `write_duckdb` uses a temporary registered DataFrame to avoid SQL injection.
