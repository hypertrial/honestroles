from __future__ import annotations

import logging
import re
import unicodedata
from dataclasses import dataclass

import pandas as pd

from honestroles.schema import (
    APPLY_URL,
    BENEFITS,
    CITY,
    COUNTRY,
    DESCRIPTION_TEXT,
    EMPLOYMENT_TYPE,
    LOCATION_RAW,
    REGION,
    REMOTE_FLAG,
    REMOTE_TYPE,
    SALARY_CURRENCY,
    SALARY_INTERVAL,
    SALARY_MAX,
    SALARY_MIN,
    SALARY_TEXT,
    TITLE,
)
from honestroles.clean.location_data import (
    CANADIAN_CITIES,
    CANADIAN_BENEFIT_KEYWORDS,
    CANADIAN_COMPLIANCE_KEYWORDS,
    CANADIAN_COUNTRY_KEYWORDS,
    CANADIAN_CURRENCY_KEYWORDS,
    CANADIAN_POSTAL_RE_PATTERN,
    COUNTRY_ALIASES,
    PROVINCE_KEYWORDS,
    REGION_ALIASES,
    REMOTE_KEYWORDS,
    US_CITIES,
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

_MULTI_LOCATION_RE = re.compile(r"\s*(?:/|;|\||\bor\b)\s*", re.IGNORECASE)
_CITY_PREFIX_RE = re.compile(r"^.+\s+-\s+", re.IGNORECASE)
_ZIP_RE = re.compile(r"\b\d{5}\b")
_CANADIAN_POSTAL_RE = re.compile(CANADIAN_POSTAL_RE_PATTERN, re.IGNORECASE)
_CANADA_CONTEXT_RE = re.compile(
    r"(?:based in|located in|position in|role in|work in|working in|must be in|"
    r"reside in|within|across|eligible to work in|authorized to work in|remote in|"
    r"remote within)\s+(?:the\s+)?(?:canada|canadian)",
    re.IGNORECASE,
)
_CANADA_BASED_RE = re.compile(r"(?:canada|canadian)[- ]based", re.IGNORECASE)


@dataclass(frozen=True)
class LocationResult:
    city: str | None
    region: str | None
    country: str | None
    remote_type: str | None


def _normalize_token(value: str) -> str:
    cleaned = _strip_accents(value.strip().lower())
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned


def _strip_accents(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(char for char in normalized if not unicodedata.combining(char))


def _strip_city_prefix(token: str) -> str:
    stripped = _CITY_PREFIX_RE.sub("", token).strip()
    return stripped if stripped else token


def _detect_remote(value: str) -> bool:
    normalized = _normalize_token(value)
    return any(keyword in normalized for keyword in REMOTE_KEYWORDS)


def _is_remote_token(value: str) -> bool:
    normalized = _normalize_token(value).replace("-", " ")
    return normalized in REMOTE_KEYWORDS


def _extract_primary_location(value: str) -> str:
    parts = [part for part in _MULTI_LOCATION_RE.split(value) if part.strip()]
    return parts[0] if parts else value


def _match_country(token: str) -> str | None:
    normalized = _normalize_token(token)
    return COUNTRY_ALIASES.get(normalized)


def _match_region(token: str) -> tuple[str, str] | None:
    normalized = _normalize_token(token)
    return REGION_ALIASES.get(normalized)


def _has_us_address_signal(tokens: list[tuple[str, str]]) -> bool:
    for orig, _ in tokens:
        if _ZIP_RE.search(orig):
            return True
        if orig.strip()[:1].isdigit():
            return True
    return False


def _is_missing(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and pd.isna(value):
        return True
    return False


def _collect_text(row: pd.Series, columns: list[str]) -> str:
    parts: list[str] = []
    for column in columns:
        if column not in row:
            continue
        value = row[column]
        if _is_missing(value):
            continue
        if isinstance(value, list):
            parts.extend(str(item) for item in value)
        else:
            parts.append(str(value))
    return " ".join(parts).lower()


def _has_canada_context(text: str) -> bool:
    if _CANADA_CONTEXT_RE.search(text):
        return True
    if _CANADA_BASED_RE.search(text):
        return True
    return False


def _detect_canadian_signals(
    row: pd.Series,
    *,
    text_columns: list[str],
    salary_currency_column: str,
    apply_url_column: str,
) -> tuple[set[str], set[str]]:
    text = _collect_text(row, text_columns + [apply_url_column])
    signals: set[str] = set()
    has_postal = _CANADIAN_POSTAL_RE.search(text) is not None
    if has_postal:
        signals.add("postal_code")
    has_country_keyword = any(keyword in text for keyword in CANADIAN_COUNTRY_KEYWORDS)
    has_context = _has_canada_context(text) if has_country_keyword else False
    if has_context:
        signals.add("country_keyword")
    has_currency_keyword = any(keyword in text for keyword in CANADIAN_CURRENCY_KEYWORDS)
    if has_currency_keyword:
        signals.add("currency_keyword")
    has_benefit = any(keyword in text for keyword in CANADIAN_BENEFIT_KEYWORDS)
    has_compliance = any(keyword in text for keyword in CANADIAN_COMPLIANCE_KEYWORDS)
    if salary_currency_column in row:
        currency = row[salary_currency_column]
        if isinstance(currency, str) and currency.strip().upper() == "CAD":
            signals.add("salary_currency")
            has_currency_keyword = True
    if has_context:
        if has_benefit:
            signals.add("benefit_keyword")
        if has_compliance:
            signals.add("compliance_keyword")
    strong_signal = has_postal or has_currency_keyword or has_context
    provinces = _detect_provinces(text) if strong_signal else set()
    return signals, provinces


def _detect_provinces(text: str) -> set[str]:
    provinces: set[str] = set()
    for keyword, province in PROVINCE_KEYWORDS.items():
        if keyword in text:
            provinces.add(province)
    return provinces


def _classify_parts(parts: list[str]) -> tuple[str | None, str | None, str | None]:
    tokens = [(part, _normalize_token(part)) for part in parts if part.strip()]
    tokens = [(orig, norm) for orig, norm in tokens if not _is_remote_token(norm)]
    if not tokens:
        return None, None, None
    if len(tokens) == 1:
        orig, norm = tokens[0]
        country = _match_country(norm)
        if country:
            return None, None, country
        region_match = _match_region(norm)
        if region_match:
            region, region_country = region_match
            return None, region, region_country
        return orig, None, None

    city_parts: list[str] = []
    region: str | None = None
    country: str | None = None
    for index in range(len(tokens) - 1, -1, -1):
        orig, norm = tokens[index]
        region_match = _match_region(norm)
        country_match = _match_country(norm)
        if country is None:
            if region_match and country_match and index > 0:
                city_hint = _strip_city_prefix(_normalize_token(tokens[index - 1][0]))
                region_value, region_country = region_match
                if city_hint in CANADIAN_CITIES:
                    if region_country == "CA":
                        region = region_value
                        country = region_country
                    else:
                        country = country_match
                    continue
                if city_hint in US_CITIES:
                    expected_state = US_CITIES[city_hint]
                    if expected_state == norm.upper():
                        region = region_value
                        country = region_country
                    else:
                        country = country_match
                    continue
                if _has_us_address_signal(tokens):
                    region = region_value
                    country = region_country
                    continue
                if region_country == "US":
                    region = region_value
                    country = region_country
                else:
                    country = country_match
                continue
            if country_match:
                country = country_match
                continue
        if region_match and country is not None:
            if norm in CANADIAN_CITIES or norm in US_CITIES:
                city_parts.insert(0, orig)
                continue
        if region is None and region_match:
            region_value, region_country = region_match
            if country is None or country == region_country:
                region = region_value
                country = country or region_country
                continue
        city_parts.insert(0, orig)
    city = ", ".join(city_parts) if city_parts else None
    return city, region, country


def normalize_locations(
    df: pd.DataFrame,
    *,
    location_column: str = LOCATION_RAW,
    city_column: str = CITY,
    region_column: str = REGION,
    country_column: str = COUNTRY,
    remote_flag_column: str = REMOTE_FLAG,
    remote_type_column: str = REMOTE_TYPE,
) -> pd.DataFrame:
    """Normalize location fields into city, region, country, and remote type."""
    if location_column not in df.columns:
        LOGGER.warning("Location column %s not found; returning input DataFrame.", location_column)
        return df
    result = df.copy()

    def parse_location(value: str | None) -> LocationResult:
        if value is None:
            return LocationResult(None, None, None, None)
        if isinstance(value, float) and pd.isna(value):
            return LocationResult(None, None, None, None)
        if not isinstance(value, str):
            return LocationResult(None, None, None, None)
        if not value:
            return LocationResult(None, None, None, None)
        cleaned = value.strip()
        remote_type = "remote" if _detect_remote(cleaned) else None
        primary = _extract_primary_location(cleaned)
        parts = [part.strip() for part in primary.split(",") if part.strip()]
        city, region, country = _classify_parts(parts)
        return LocationResult(city, region, country, remote_type)

    cities: list[str | None] = []
    regions: list[str | None] = []
    countries: list[str | None] = []
    remote_types: list[str | None] = []
    for value in result[location_column].tolist():
        parsed = parse_location(value)
        cities.append(parsed.city)
        regions.append(parsed.region)
        countries.append(parsed.country)
        remote_types.append(parsed.remote_type)
    result[city_column] = cities
    result[region_column] = regions
    result[country_column] = countries

    if remote_flag_column in result.columns:
        remote_flags = result[remote_flag_column].fillna(False).astype(bool).tolist()
        result[remote_type_column] = [
            "remote" if (flag or remote == "remote") else None
            for flag, remote in zip(remote_flags, remote_types)
        ]
    else:
        result[remote_type_column] = remote_types
    return result


def enrich_country_from_context(
    df: pd.DataFrame,
    *,
    country_column: str = COUNTRY,
    region_column: str = REGION,
    description_column: str = DESCRIPTION_TEXT,
    title_column: str = TITLE,
    salary_text_column: str = SALARY_TEXT,
    salary_currency_column: str = SALARY_CURRENCY,
    apply_url_column: str = APPLY_URL,
    benefits_column: str = BENEFITS,
) -> pd.DataFrame:
    if country_column not in df.columns:
        return df
    result = df.copy()
    text_columns = [description_column, title_column, salary_text_column, benefits_column]
    countries = result[country_column].tolist()
    regions = (
        result[region_column].tolist()
        if region_column in result.columns
        else [None] * len(result)
    )

    for index in range(len(result)):
        country = countries[index]
        region = regions[index]
        if _is_missing(country):
            signals, provinces = _detect_canadian_signals(
                result.iloc[index],
                text_columns=text_columns,
                salary_currency_column=salary_currency_column,
                apply_url_column=apply_url_column,
            )
            if signals:
                country = "CA"
                if _is_missing(region) and len(provinces) == 1:
                    region = next(iter(provinces))
        elif isinstance(country, str) and country.upper() == "CA" and _is_missing(region):
            text = _collect_text(
                result.iloc[index], text_columns + [apply_url_column]
            )
            provinces = _detect_provinces(text)
            if len(provinces) == 1:
                region = next(iter(provinces))
        countries[index] = country
        regions[index] = region

    result[country_column] = countries
    if region_column in result.columns:
        result[region_column] = regions
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

    result[employment_type_column] = (
        result[employment_type_column].apply(normalize).astype("object")
    )
    result[employment_type_column] = result[employment_type_column].where(
        result[employment_type_column].notna(), None
    )
    return result
