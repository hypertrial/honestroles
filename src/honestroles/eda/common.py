from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any

import polars as pl


def round4(value: float) -> float:
    return round(float(value), 4)


def serialize_scalar(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): jsonable(v) for k, v in value.items()}
    if isinstance(value, list):
        return [jsonable(v) for v in value]
    if isinstance(value, tuple):
        return [jsonable(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def distribution_table(rows: list[dict[str, Any]], value_column: str) -> pl.DataFrame:
    if rows:
        return pl.DataFrame(rows)
    return pl.DataFrame(
        schema={
            value_column: pl.String,
            "len": pl.Int64,
            "pct": pl.Float64,
        }
    )
