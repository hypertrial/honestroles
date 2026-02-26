from __future__ import annotations

import math
from collections.abc import Iterable

import pandas as pd


def assert_row_count_bounds(
    original: pd.DataFrame,
    result: pd.DataFrame,
    *,
    min_rows: int = 0,
) -> None:
    assert min_rows <= len(result) <= len(original)


def assert_index_preserved(original: pd.DataFrame, result: pd.DataFrame) -> None:
    assert len(result) == len(original)
    assert result.index.equals(original.index)


def assert_mask_shape(df: pd.DataFrame, mask: pd.Series) -> None:
    assert len(mask) == len(df)
    assert mask.index.equals(df.index)


def assert_numeric_between(series: pd.Series, *, minimum: float, maximum: float) -> None:
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if numeric.empty:
        return
    assert ((numeric >= minimum) & (numeric <= maximum)).all()


def assert_columns_present(df: pd.DataFrame, columns: Iterable[str]) -> None:
    for column in columns:
        assert column in df.columns


def assert_list_of_strings_or_empty(series: pd.Series) -> None:
    for value in series.tolist():
        if value is None:
            continue
        assert isinstance(value, list)
        assert all(isinstance(item, str) for item in value)


def assert_finite_numeric(series: pd.Series) -> None:
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if numeric.empty:
        return
    assert numeric.map(lambda value: math.isfinite(float(value))).all()
