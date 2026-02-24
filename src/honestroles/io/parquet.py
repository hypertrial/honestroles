from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from honestroles.io.dataframe import validate_dataframe


def read_parquet(path: str | Path, validate: bool = True) -> pd.DataFrame:
    file_path = Path(path)
    if not file_path.exists():
        raise FileNotFoundError(f"Parquet file not found: {file_path}")
    df = pd.read_parquet(file_path)
    if validate:
        validate_dataframe(df)
    return df


def write_parquet(df: pd.DataFrame, path: str | Path) -> None:
    file_path = Path(path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(file_path, index=False)


def iter_parquet_row_groups(
    path: str | Path,
    *,
    columns: list[str] | None = None,
    validate: bool = False,
) -> Iterator[pd.DataFrame]:
    file_path = Path(path)
    if not file_path.exists():
        raise FileNotFoundError(f"Parquet file not found: {file_path}")
    parquet_file = pq.ParquetFile(file_path)
    for row_group_index in range(parquet_file.num_row_groups):
        table = parquet_file.read_row_group(row_group_index, columns=columns)
        chunk = table.to_pandas()
        if validate:
            validate_dataframe(chunk)
        yield chunk
