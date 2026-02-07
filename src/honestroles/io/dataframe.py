from __future__ import annotations

from typing import Iterable

import pandas as pd

from honestroles.schema import REQUIRED_COLUMNS


def validate_dataframe(df: pd.DataFrame, required_columns: Iterable[str] | None = None) -> pd.DataFrame:
    required = set(required_columns or REQUIRED_COLUMNS)
    missing = required.difference(df.columns)
    if missing:
        missing_str = ", ".join(sorted(missing))
        raise ValueError(f"Missing required columns: {missing_str}")
    return df
