from __future__ import annotations

from honestroles.io.contract import (
    normalize_source_data_contract,
    validate_source_data_contract,
)
from honestroles.io.dataframe import validate_dataframe
from honestroles.io.duckdb_io import (
    read_duckdb_query,
    read_duckdb_table,
    write_duckdb,
)
from honestroles.io.parquet import iter_parquet_row_groups, read_parquet, write_parquet
from honestroles.io.quality_report import DataQualityAccumulator, DataQualityReport, build_data_quality_report

__all__ = [
    "read_parquet",
    "iter_parquet_row_groups",
    "write_parquet",
    "read_duckdb_table",
    "read_duckdb_query",
    "write_duckdb",
    "validate_dataframe",
    "normalize_source_data_contract",
    "validate_source_data_contract",
    "DataQualityReport",
    "DataQualityAccumulator",
    "build_data_quality_report",
]
