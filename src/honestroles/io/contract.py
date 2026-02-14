from __future__ import annotations

import json
from typing import Iterable

import pandas as pd

from honestroles.io.dataframe import validate_dataframe
from honestroles.schema import REQUIRED_COLUMNS

DEFAULT_TIMESTAMP_COLUMNS = (
    "ingested_at",
    "posted_at",
    "updated_at",
    "last_seen",
    "application_deadline",
)

DEFAULT_ARRAY_COLUMNS = ("skills", "languages", "benefits", "keywords")


def _normalize_timestamp_value(value: object) -> object:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        ts = value.tz_localize("UTC") if value.tzinfo is None else value.tz_convert("UTC")
        return ts.isoformat().replace("+00:00", "Z")
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        parsed = pd.to_datetime(text, errors="coerce", utc=True)
        if pd.isna(parsed):
            return text
        return parsed.isoformat().replace("+00:00", "Z")
    parsed = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(parsed):
        return value
    return parsed.isoformat().replace("+00:00", "Z")


def _normalize_array_value(value: object) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        parsed: object = text
        if text.startswith("[") and text.endswith("]"):
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                parsed = text
        if isinstance(parsed, list):
            cleaned = [str(item).strip() for item in parsed if str(item).strip()]
            return cleaned or None
        delimiter = ";" if ";" in text else ("," if "," in text else None)
        if delimiter is None:
            return [text]
        cleaned = [part.strip() for part in text.split(delimiter) if part.strip()]
        return cleaned or None

    if isinstance(value, (list, tuple, set)):
        cleaned = [str(item).strip() for item in value if str(item).strip()]
        return cleaned or None

    return [str(value)]


def normalize_source_data_contract(
    df: pd.DataFrame,
    *,
    timestamp_columns: Iterable[str] | None = None,
    array_columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    """Normalize common source-data shape issues before contract validation.

    Normalization rules:
    - Parse common timestamp representations into ISO-8601 UTC strings.
    - Normalize array-like fields from JSON strings or delimited strings to lists.
    """
    result = df.copy()
    timestamps = list(timestamp_columns or DEFAULT_TIMESTAMP_COLUMNS)
    arrays = list(array_columns or DEFAULT_ARRAY_COLUMNS)

    for column in timestamps:
        if column in result.columns:
            result[column] = result[column].apply(_normalize_timestamp_value)

    for column in arrays:
        if column in result.columns:
            result[column] = result[column].apply(_normalize_array_value)

    return result


def validate_source_data_contract(
    df: pd.DataFrame,
    *,
    required_columns: Iterable[str] | None = None,
    require_non_null: bool = True,
) -> pd.DataFrame:
    """Validate source data against the honestroles core contract.

    Validation rules:
    - Required columns must exist.
    - Required columns must be non-null when `require_non_null=True`.
    """
    required = set(required_columns or REQUIRED_COLUMNS)
    validate_dataframe(df, required_columns=required)

    if not require_non_null or df.empty:
        return df

    null_violations: list[str] = []
    for column in sorted(required):
        null_count = int(df[column].isna().sum())
        if null_count > 0:
            null_violations.append(f"{column} ({null_count} null)")

    if null_violations:
        joined = ", ".join(null_violations)
        raise ValueError(f"Required columns contain null values: {joined}")

    return df
