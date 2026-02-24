## IO

`honestroles.io` provides read/write helpers for Parquet and DuckDB, plus a
minimal DataFrame validation utility.

### Modules

- `parquet.py`: Parquet read/write.
- `quality_report.py`: data quality report model + accumulator.
- `duckdb_io.py`: DuckDB read/write.
- `dataframe.py`: `validate_dataframe`.
- `contract.py`: Source-data normalization and contract validation.

### Public API reference

#### `read_parquet(path: str | Path, validate: bool = True) -> pd.DataFrame`

Reads a Parquet file. Raises `FileNotFoundError` if the path does not exist.
When `validate=True`, checks required columns.

#### `write_parquet(df: pd.DataFrame, path: str | Path) -> None`

Writes a Parquet file, creating parent directories if needed.

#### `iter_parquet_row_groups(path: str | Path, *, columns: list[str] | None = None, validate: bool = False) -> Iterator[pd.DataFrame]`

Yields one DataFrame per parquet row-group in deterministic order. Useful for
streaming large datasets without loading the full file in memory.

#### `read_duckdb_table(conn: duckdb.DuckDBPyConnection, table: str, validate: bool = True) -> pd.DataFrame`

Reads from DuckDB by validated table name. When `validate=True`, checks
required columns.

#### `read_duckdb_query(conn: duckdb.DuckDBPyConnection, query: str, validate: bool = True) -> pd.DataFrame`

Reads from DuckDB using a validated read-only SQL query (`SELECT`/`WITH`,
single statement only). When `validate=True`, checks required columns.

#### `write_duckdb(df: pd.DataFrame, conn: duckdb.DuckDBPyConnection, table: str, *, overwrite: bool = True) -> None`

Writes a DataFrame to a DuckDB table. Validates table name using a regex.
Uses `CREATE OR REPLACE` when `overwrite=True`.

#### `validate_dataframe(df: pd.DataFrame, required_columns: Iterable[str] | None = None) -> pd.DataFrame`

Ensures required columns are present (defaults to `schema.REQUIRED_COLUMNS`).
Raises `ValueError` if any are missing.

#### `validate_source_data_contract(df: pd.DataFrame, required_columns: Iterable[str] | None = None, require_non_null: bool = True, enforce_formats: bool = True) -> pd.DataFrame`

Validates the source-data contract used by `honestroles`:
- required columns must exist
- required columns must be non-null when `require_non_null=True`
- format/type validation when `enforce_formats=True`:
  - timestamp columns must be parseable
  - `apply_url` must be a valid `http`/`https` URL
  - known array columns must be arrays of strings
  - known boolean columns must be booleans
  - salary metadata values must match expected format/ranges

#### `normalize_source_data_contract(df: pd.DataFrame, timestamp_columns: Iterable[str] | None = None, array_columns: Iterable[str] | None = None) -> pd.DataFrame`

Normalizes common source-data format issues:
- timestamp-like fields -> ISO-8601 UTC strings
- array-like fields encoded as strings -> Python lists

#### `build_data_quality_report(df: pd.DataFrame, *, dataset_name: str | None = None, top_n_duplicates: int = 10) -> DataQualityReport`

Builds a deterministic quality report summary:
- required-field null/empty counts
- duplicate key/hash hotspots
- listing-page ratios
- source-level quality slices
- enrichment sparsity and URL/location health

#### `DataQualityAccumulator`

Incremental accumulator for chunked inputs (for example, parquet row-group streams).
Call `update(df_chunk)` repeatedly, then `finalize()`.

### Usage examples

```python
import honestroles as hr

df = hr.read_parquet("jobs_current.parquet")
hr.write_parquet(df, "jobs_clean.parquet")
```

```python
import duckdb
from honestroles.io import read_duckdb_query, read_duckdb_table, write_duckdb

conn = duckdb.connect()
df = read_duckdb_table(conn, "jobs_current")
subset = read_duckdb_query(conn, "select * from jobs_current where source = 'greenhouse'")
write_duckdb(df, conn, "jobs_scored", overwrite=True)
```

```python
from honestroles.io import (
    build_data_quality_report,
    iter_parquet_row_groups,
    normalize_source_data_contract,
    validate_source_data_contract,
)

df = normalize_source_data_contract(df)
df = validate_source_data_contract(df)
report = build_data_quality_report(df, dataset_name="jobs_current")
for chunk in iter_parquet_row_groups("jobs_historical.parquet"):
    pass
```

### Design notes

- Contract validation is strict by default for known field types/formats.
- `read_duckdb_table` and `read_duckdb_query` are the preferred explicit APIs.
- `write_duckdb` uses a temporary registered DataFrame and validates table names.
