from __future__ import annotations

import logging
import re

import pandas as pd

from honestroles.schema import (
    CITY,
    COUNTRY,
    EMPLOYMENT_TYPE,
    LOCATION_RAW,
    REMOTE_FLAG,
    REMOTE_TYPE,
    SALARY_CURRENCY,
    SALARY_INTERVAL,
    SALARY_MAX,
    SALARY_MIN,
    SALARY_TEXT,
)

LOGGER = logging.getLogger(__name__)

_EMPLOYMENT_MAP = {
    "full-time": "full_time",
    "full time": "full_time",
    "part-time": "part_time",
    "part time": "part_time",
    "contract": "contract",
    "intern": "intern",
    "internship": "intern",
    "temporary": "temporary",
}

_SALARY_RANGE_RE = re.compile(
    r"\$?\s*(\d{2,3}(?:[,\d]{0,6})?)\s*(?:-|to|–)\s*\$?\s*(\d{2,3}(?:[,\d]{0,6})?)",
    re.IGNORECASE,
)


def normalize_locations(
    df: pd.DataFrame,
    *,
    location_column: str = LOCATION_RAW,
    city_column: str = CITY,
    country_column: str = COUNTRY,
    remote_flag_column: str = REMOTE_FLAG,
    remote_type_column: str = REMOTE_TYPE,
) -> pd.DataFrame:
    if location_column not in df.columns:
        LOGGER.warning("Location column %s not found; returning input DataFrame.", location_column)
        return df
    result = df.copy()

    def split_location(value: str | None) -> tuple[str | None, str | None]:
        if not value:
            return None, None
        parts = [part.strip() for part in value.split(",") if part.strip()]
        if len(parts) >= 2:
            return parts[0], parts[-1]
        return None, None

    cities: list[str | None] = []
    countries: list[str | None] = []
    for value in result[location_column].tolist():
        city, country = split_location(value)
        cities.append(city)
        countries.append(country)
    result[city_column] = cities
    result[country_column] = countries

    if remote_flag_column in result.columns:
        result[remote_type_column] = result[remote_flag_column].apply(
            lambda flag: "remote" if flag is True else None
        )
    return result


def normalize_salaries(
    df: pd.DataFrame,
    *,
    salary_text_column: str = SALARY_TEXT,
    salary_min_column: str = SALARY_MIN,
    salary_max_column: str = SALARY_MAX,
    salary_currency_column: str = SALARY_CURRENCY,
    salary_interval_column: str = SALARY_INTERVAL,
) -> pd.DataFrame:
    if salary_text_column not in df.columns:
        return df
    result = df.copy()

    def parse_range(value: str | float | None) -> tuple[float | None, float | None]:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None, None
        if not isinstance(value, str):
            return None, None
        if not value:
            return None, None
        match = _SALARY_RANGE_RE.search(value.replace(",", ""))
        if not match:
            return None, None
        low = float(match.group(1))
        high = float(match.group(2))
        return min(low, high), max(low, high)

    mins: list[float | None] = []
    maxs: list[float | None] = []
    for value in result[salary_text_column].tolist():
        low, high = parse_range(value)
        mins.append(low)
        maxs.append(high)
    result[salary_min_column] = mins
    result[salary_max_column] = maxs

    if salary_currency_column not in result.columns:
        result[salary_currency_column] = "USD"
    if salary_interval_column not in result.columns:
        result[salary_interval_column] = "year"
    return result


def normalize_employment_types(
    df: pd.DataFrame, *, employment_type_column: str = EMPLOYMENT_TYPE
) -> pd.DataFrame:
    if employment_type_column not in df.columns:
        return df
    result = df.copy()

    def normalize(value: str | None) -> str | None:
        if value is None:
            return None
        if isinstance(value, float) and pd.isna(value):
            return None
        if not isinstance(value, str):
            return None
        if not value:
            return None
        lowered = value.strip().lower()
        return _EMPLOYMENT_MAP.get(lowered, lowered.replace(" ", "_"))

    result[employment_type_column] = result[employment_type_column].apply(normalize)
    return result
