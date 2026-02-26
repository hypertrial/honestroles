from __future__ import annotations

import math

import polars as pl


def assert_dataframe(df: pl.DataFrame) -> None:
    assert isinstance(df, pl.DataFrame)


def assert_score_bounds(df: pl.DataFrame, columns: list[str]) -> None:
    for column in columns:
        if column not in df.columns:
            continue
        values = df[column].cast(pl.Float64, strict=False).fill_null(0.0).to_list()
        for value in values:
            assert math.isfinite(float(value))
            assert 0.0 <= float(value) <= 1.0
