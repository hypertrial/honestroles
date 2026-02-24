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
_CANADIAN_COUNTRY_KEYWORDS_RE = re.compile(
    "|".join(re.escape(keyword) for keyword in sorted(CANADIAN_COUNTRY_KEYWORDS)),
    re.IGNORECASE,
)
_CANADIAN_CURRENCY_KEYWORDS_RE = re.compile(
    "|".join(re.escape(keyword) for keyword in sorted(CANADIAN_CURRENCY_KEYWORDS)),
    re.IGNORECASE,
)


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


def _series_to_lower_text(series: pd.Series) -> pd.Series:
    if series.dtype != object:
        return series.astype("string").fillna("").str.lower()

    list_mask = series.map(lambda value: isinstance(value, list))
    text = pd.Series("", index=series.index, dtype="string")
    if bool(list_mask.any()):
        list_text = series.loc[list_mask].map(
            lambda values: " ".join(str(item) for item in values)
        )
        text.loc[list_mask] = list_text.astype("string")
    non_list_mask = ~list_mask
    if bool(non_list_mask.any()):
        text.loc[non_list_mask] = (
            series.loc[non_list_mask].astype("string").fillna("")
        )
    return text.fillna("").str.lower()


def _combined_lower_text(df: pd.DataFrame, columns: list[str]) -> pd.Series:
    text = pd.Series("", index=df.index, dtype="string")
    for column in columns:
        if column not in df.columns:
            continue
        text = text.str.cat(_series_to_lower_text(df[column]), sep=" ")
    return text


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
        matched_country = _match_country(norm)
        if matched_country:
            return None, None, matched_country
        region_match = _match_region(norm)
        if region_match:
            matched_region, matched_region_country = region_match
            return None, matched_region, matched_region_country
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


def _parse_location_string(value: str) -> LocationResult:
    cleaned = value.strip()
    if not cleaned:
        return LocationResult(None, None, None, None)
    remote_type = "remote" if _detect_remote(cleaned) else None
    primary = _extract_primary_location(cleaned)
    parts = [part.strip() for part in primary.split(",") if part.strip()]
    city, region, country = _classify_parts(parts)
    return LocationResult(city, region, country, remote_type)


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
    raw = result[location_column]
    string_mask = raw.map(lambda value: isinstance(value, str)).astype("bool")
    stripped = pd.Series("", index=result.index, dtype="string")
    if bool(string_mask.any()):
        stripped.loc[string_mask] = raw.loc[string_mask].astype("string").fillna("").str.strip()
    non_empty_mask = stripped.ne("").fillna(False).astype("bool")
    parse_mask = string_mask & non_empty_mask

    parsed = pd.Series(
        [LocationResult(None, None, None, None)] * len(result),
        index=result.index,
        dtype="object",
    )
    if bool(parse_mask.any()):
        unique_locations = pd.unique(stripped.loc[parse_mask].astype(str))
        parsed_map = {
            location: _parse_location_string(location)
            for location in unique_locations
        }
        parsed.loc[parse_mask] = stripped.loc[parse_mask].map(parsed_map)

    parsed_values = parsed.tolist()
    cities = [item.city for item in parsed_values]
    regions = [item.region for item in parsed_values]
    countries = [item.country for item in parsed_values]
    remote_types = [item.remote_type for item in parsed_values]
    result[city_column] = pd.Series(cities, dtype="object")
    result[region_column] = pd.Series(regions, dtype="object")
    result[country_column] = pd.Series(countries, dtype="object")
    result[city_column] = result[city_column].where(result[city_column].notna(), None)
    result[region_column] = result[region_column].where(result[region_column].notna(), None)
    result[country_column] = result[country_column].where(result[country_column].notna(), None)

    if remote_flag_column in result.columns:
        remote_flags = result[remote_flag_column].fillna(False).astype(bool).tolist()
        remote_values = [
            "remote" if (flag or remote == "remote") else None
            for flag, remote in zip(remote_flags, remote_types)
        ]
        result[remote_type_column] = pd.Series(remote_values, dtype="object")
    else:
        result[remote_type_column] = pd.Series(remote_types, dtype="object")
    result[remote_type_column] = result[remote_type_column].where(
        result[remote_type_column].notna(), None
    )
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
    text_columns = [
        description_column,
        title_column,
        salary_text_column,
        benefits_column,
        apply_url_column,
    ]
    countries = result[country_column].copy().astype("object")
    missing_country = countries.isna()

    if region_column not in result.columns:
        if not bool(missing_country.any()):
            return result
        candidate_index = missing_country
        subset = result.loc[candidate_index]
        text = _combined_lower_text(subset, text_columns)
        has_country_keyword = text.str.contains(_CANADIAN_COUNTRY_KEYWORDS_RE, na=False)
        has_context = has_country_keyword & (
            text.str.contains(_CANADA_CONTEXT_RE, na=False)
            | text.str.contains(_CANADA_BASED_RE, na=False)
        )
        has_postal = text.str.contains(_CANADIAN_POSTAL_RE, na=False)
        has_currency_keyword = text.str.contains(_CANADIAN_CURRENCY_KEYWORDS_RE, na=False)
        salary_currency_cad = pd.Series(False, index=subset.index, dtype="bool")
        if salary_currency_column in subset.columns:
            salary_currency_cad = (
                subset[salary_currency_column]
                .astype("string")
                .fillna("")
                .str.strip()
                .str.upper()
                .eq("CAD")
            )
        ca_candidate = has_postal | has_context | has_currency_keyword | salary_currency_cad
        countries.loc[ca_candidate.index[ca_candidate]] = "CA"
        result[country_column] = countries
        return result

    regions = result[region_column].copy().astype("object")
    region_missing = regions.isna()
    ca_missing_region = countries.astype("string").fillna("").str.upper().eq("CA") & region_missing

    candidate_index = missing_country | ca_missing_region
    if not bool(candidate_index.any()):
        return result

    subset = result.loc[candidate_index]
    text = _combined_lower_text(subset, text_columns)

    missing_subset = missing_country.loc[candidate_index]
    missing_subset_text = text.loc[missing_subset]
    has_country_keyword = missing_subset_text.str.contains(_CANADIAN_COUNTRY_KEYWORDS_RE, na=False)
    has_context = has_country_keyword & (
        missing_subset_text.str.contains(_CANADA_CONTEXT_RE, na=False)
        | missing_subset_text.str.contains(_CANADA_BASED_RE, na=False)
    )
    has_postal = missing_subset_text.str.contains(_CANADIAN_POSTAL_RE, na=False)
    has_currency_keyword = missing_subset_text.str.contains(_CANADIAN_CURRENCY_KEYWORDS_RE, na=False)

    salary_currency_cad = pd.Series(False, index=missing_subset_text.index, dtype="bool")
    if salary_currency_column in subset.columns:
        salary_currency_cad = (
            subset.loc[missing_subset_text.index, salary_currency_column]
            .astype("string")
            .fillna("")
            .str.strip()
            .str.upper()
            .eq("CAD")
        )

    ca_candidate = has_postal | has_context | has_currency_keyword | salary_currency_cad
    countries.loc[ca_candidate.index[ca_candidate]] = "CA"
    result[country_column] = countries

    new_ca_region_missing = ca_candidate & region_missing.loc[ca_candidate.index]
    needs_region_index = new_ca_region_missing.index[new_ca_region_missing].union(
        ca_missing_region.index[ca_missing_region]
    )
    if len(needs_region_index) > 0:
        provinces = text.loc[needs_region_index.intersection(text.index)].map(_detect_provinces)
        has_single_province = provinces.map(len).eq(1)
        if bool(has_single_province.any()):
            regions.loc[has_single_province.index[has_single_province]] = (
                provinces.loc[has_single_province]
                .map(lambda matched: next(iter(matched)))
                .astype("object")
            )
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
