from __future__ import annotations

import json
import re
from urllib.parse import urlparse
from typing import Iterable

import numpy as np
import pandas as pd

from honestroles.io.dataframe import validate_dataframe
from honestroles.schema import (
    APPLY_URL,
    REMOTE_FLAG,
    REQUIRED_COLUMNS,
    SALARY_CURRENCY,
    SALARY_INTERVAL,
    SALARY_MAX,
    SALARY_MIN,
    VISA_SPONSORSHIP,
)

DEFAULT_TIMESTAMP_COLUMNS = (
    "ingested_at",
    "posted_at",
    "updated_at",
    "last_seen",
    "application_deadline",
)

DEFAULT_ARRAY_COLUMNS = ("skills", "languages", "benefits", "keywords")
DEFAULT_BOOLEAN_COLUMNS = (REMOTE_FLAG, VISA_SPONSORSHIP)
_CURRENCY_RE = re.compile(r"^[A-Z]{3}$")
_ALLOWED_SALARY_INTERVALS = {"hour", "day", "week", "month", "year"}


def _is_missing(value: object) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _mask_positions(mask: pd.Series) -> list[int]:
    return [int(pos) for pos in np.flatnonzero(mask.to_numpy())]


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
    enforce_formats: bool = True,
) -> pd.DataFrame:
    """Validate source data against the honestroles core contract.

    Validation rules:
    - Required columns must exist.
    - Required columns must be non-null when `require_non_null=True`.
    """
    required = set(required_columns or REQUIRED_COLUMNS)
    validate_dataframe(df, required_columns=required)

    if df.empty:
        return df

    if require_non_null:
        null_violations: list[str] = []
        for column in sorted(required):
            null_count = int(df[column].isna().sum())
            if null_count > 0:
                null_violations.append(f"{column} ({null_count} null)")

        if null_violations:
            joined = ", ".join(null_violations)
            raise ValueError(f"Required columns contain null values: {joined}")

    if not enforce_formats:
        return df

    format_violations: list[str] = []

    for column in DEFAULT_TIMESTAMP_COLUMNS:
        if column not in df.columns:
            continue
        series = df[column]
        invalid_mask = series.notna() & pd.to_datetime(
            series,
            errors="coerce",
            utc=True,
            format="mixed",
        ).isna()
        for pos in _mask_positions(invalid_mask):
            format_violations.append(f"{column}[{pos}] invalid timestamp")

    if APPLY_URL in df.columns:
        series = df[APPLY_URL]
        non_missing = series.notna()
        is_string = series.map(lambda value: isinstance(value, str))
        invalid_url = pd.Series([False] * len(df), index=df.index, dtype="bool")
        for pos in _mask_positions(non_missing & is_string):
            parsed = urlparse(str(series.iloc[pos]).strip())
            if parsed.scheme not in {"http", "https"} or not parsed.netloc:
                invalid_url.iloc[pos] = True
        for pos in _mask_positions(non_missing):
            if not bool(is_string.iloc[pos]):
                format_violations.append(f"{APPLY_URL}[{pos}] must be a URL string")
            elif bool(invalid_url.iloc[pos]):
                format_violations.append(f"{APPLY_URL}[{pos}] invalid URL")

    for column in DEFAULT_ARRAY_COLUMNS:
        if column not in df.columns:
            continue
        series = df[column]
        non_missing = series.notna()
        is_array = series.map(lambda value: isinstance(value, (list, tuple, set, np.ndarray)))
        bad_type_mask = non_missing & ~is_array
        for pos in _mask_positions(non_missing):
            if bool(bad_type_mask.iloc[pos]):
                format_violations.append(f"{column}[{pos}] must be an array of strings")
                continue
            value = series.iloc[pos]
            if any(not isinstance(item, str) for item in value):
                format_violations.append(f"{column}[{pos}] contains non-string values")

    for column in DEFAULT_BOOLEAN_COLUMNS:
        if column not in df.columns:
            continue
        series = df[column]
        non_missing = series.notna()
        is_bool = series.map(lambda value: isinstance(value, (bool, np.bool_)))
        for pos in _mask_positions(non_missing & ~is_bool):
            format_violations.append(f"{column}[{pos}] must be boolean")

    if SALARY_CURRENCY in df.columns:
        series = df[SALARY_CURRENCY]
        non_missing = series.notna()
        valid_currency = series.map(
            lambda value: isinstance(value, str) and _CURRENCY_RE.match(value.strip()) is not None
        )
        for pos in _mask_positions(non_missing & ~valid_currency):
            format_violations.append(
                f"{SALARY_CURRENCY}[{pos}] must be a 3-letter uppercase currency code"
            )

    if SALARY_INTERVAL in df.columns:
        series = df[SALARY_INTERVAL]
        non_missing = series.notna()
        valid_interval = series.map(
            lambda value: isinstance(value, str)
            and value.strip().lower() in _ALLOWED_SALARY_INTERVALS
        )
        for pos in _mask_positions(non_missing & ~valid_interval):
            format_violations.append(
                f"{SALARY_INTERVAL}[{pos}] must be one of {sorted(_ALLOWED_SALARY_INTERVALS)}"
            )

    if SALARY_MIN in df.columns and SALARY_MAX in df.columns:
        mins = df[SALARY_MIN]
        maxs = df[SALARY_MAX]
        both_present = mins.notna() & maxs.notna()
        mins_numeric = pd.to_numeric(mins, errors="coerce")
        maxs_numeric = pd.to_numeric(maxs, errors="coerce")
        non_numeric = both_present & (mins_numeric.isna() | maxs_numeric.isna())
        min_gt_max = both_present & ~non_numeric & (mins_numeric > maxs_numeric)
        for pos in _mask_positions(non_numeric):
            format_violations.append(f"salary_min/salary_max[{pos}] must be numeric")
        for pos in _mask_positions(min_gt_max):
            format_violations.append(
                f"salary_min/salary_max[{pos}] has min greater than max"
            )

    if format_violations:
        preview = ", ".join(format_violations[:8])
        remaining = len(format_violations) - 8
        if remaining > 0:
            preview = f"{preview}, ... (+{remaining} more)"
        raise ValueError(f"Source data contract format violations: {preview}")

    return df
