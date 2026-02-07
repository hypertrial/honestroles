from __future__ import annotations

from pathlib import Path

import pandas as pd

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
