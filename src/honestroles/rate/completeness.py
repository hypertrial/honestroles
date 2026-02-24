from __future__ import annotations

import pandas as pd
from pandas.api.types import is_object_dtype, is_string_dtype

from honestroles.schema import (
    APPLY_URL,
    BENEFITS,
    COMPANY,
    DESCRIPTION_TEXT,
    LOCATION_RAW,
    SALARY_MIN,
    SKILLS,
    TITLE,
)


def rate_completeness(
    df: pd.DataFrame,
    *,
    required_fields: list[str] | None = None,
    output_column: str = "completeness_score",
) -> pd.DataFrame:
    fields = required_fields or [
        COMPANY,
        TITLE,
        LOCATION_RAW,
        APPLY_URL,
        DESCRIPTION_TEXT,
        SALARY_MIN,
        SKILLS,
        BENEFITS,
    ]
    existing = [field for field in fields if field in df.columns]
    if not existing:
        return df
    result = df.copy()

    present_counts = pd.Series(0.0, index=result.index, dtype="float64")
    for field in existing:
        series = result[field]
        present = series.notna()
        if is_object_dtype(series.dtype):
            is_list = series.map(lambda value: isinstance(value, list))
            if bool(is_list.any()):
                present &= ~(is_list & series.map(lambda value: len(value) == 0))
            is_string = series.map(lambda value: isinstance(value, str))
            if bool(is_string.any()):
                stripped = series.astype("string").fillna("").str.strip()
                present &= ~(is_string & stripped.eq("").fillna(False))
        elif is_string_dtype(series.dtype):
            stripped = series.astype("string").fillna("").str.strip()
            present &= stripped.ne("")
        present_counts += present.astype("float64")

    result[output_column] = present_counts / float(len(existing))
    return result
