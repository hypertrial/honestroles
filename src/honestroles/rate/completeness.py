from __future__ import annotations

import pandas as pd

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

    def score(row: pd.Series) -> float:
        present = 0
        for field in existing:
            value = row[field]
            if value is None:
                continue
            if isinstance(value, list) and not value:
                continue
            if isinstance(value, str) and value.strip() == "":
                continue
            try:
                if pd.isna(value):
                    continue
            except ValueError:
                pass
            present += 1
        return present / len(existing)

    result[output_column] = result.apply(score, axis=1)
    return result
