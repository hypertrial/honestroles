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
    SALARY_CONFIDENCE,
    SALARY_ANNUAL_MAX,
    SALARY_ANNUAL_MIN,
    SALARY_INTERVAL,
    SALARY_MAX,
    SALARY_MIN,
    SALARY_SOURCE,
    SALARY_TEXT,
    SKILLS,
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

_SALARY_PARSE_TEXT_MAX_CHARS = 3200
_SKILL_PARSE_TEXT_MAX_CHARS = 800

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
_SALARY_CAPTURE_RE = re.compile(
    r"(?P<prefix>[$€£]|usd|cad|gbp|eur)?\s*"
    r"(?P<low>\d{2,6}(?:,\d{3})*(?:\.\d+)?)\s*(?P<low_k>[kK]?)\s*"
    r"(?:-|to|–)\s*"
    r"(?P<prefix2>[$€£]|usd|cad|gbp|eur)?\s*"
    r"(?P<high>\d{2,6}(?:,\d{3})*(?:\.\d+)?)\s*(?P<high_k>[kK]?)",
    re.IGNORECASE,
)
_SALARY_SINGLE_RE = re.compile(
    r"(?P<prefix>[$€£]|usd|cad|gbp|eur)\s*"
    r"(?P<amount>\d{2,6}(?:,\d{3})*(?:\.\d+)?)\s*(?P<amount_k>[kK]?)",
    re.IGNORECASE,
)

_MULTI_LOCATION_RE = re.compile(r"\s*(?:/|;|\||\bor\b)\s*", re.IGNORECASE)
_CITY_PREFIX_RE = re.compile(r"^.+\s+-\s+", re.IGNORECASE)
_ZIP_RE = re.compile(r"\b\d{5}\b")
_UNKNOWN_LOCATION_VALUES = {"unknown", "n/a", "na", "none", "null", "unspecified", "tbd"}
_REMOTE_CONTEXT_RE = re.compile(
    r"\b(?:remote|work\s*from\s*home|wfh|distributed|anywhere|home[- ]based)\b",
    re.IGNORECASE,
)
_REMOTE_NEGATIVE_RE = re.compile(
    r"\b(?:not remote|non-remote|on[- ]site|onsite only|in-office only|hybrid only)\b",
    re.IGNORECASE,
)
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
_SALARY_INTERVAL_PATTERNS = [
    (re.compile(r"(?:per|/)\s*hour|hourly", re.IGNORECASE), "hour"),
    (re.compile(r"(?:per|/)\s*day|daily", re.IGNORECASE), "day"),
    (re.compile(r"(?:per|/)\s*week|weekly", re.IGNORECASE), "week"),
    (re.compile(r"(?:per|/)\s*month|monthly", re.IGNORECASE), "month"),
    (re.compile(r"(?:per|/)\s*year|annually|annual|yearly", re.IGNORECASE), "year"),
]
_SALARY_CURRENCY_MAP = {
    "$": "USD",
    "usd": "USD",
    "cad": "CAD",
    "gbp": "GBP",
    "eur": "EUR",
    "£": "GBP",
    "€": "EUR",
}
_ANNUAL_INTERVAL_MULTIPLIERS = {
    "hour": 2080.0,
    "day": 260.0,
    "week": 52.0,
    "month": 12.0,
    "year": 1.0,
}
_SALARY_CONTEXT_RE = re.compile(
    r"\b(?:salary|compensation|base pay|pay range|hourly rate|annual salary|annual pay)\b",
    re.IGNORECASE,
)
_SALARY_PARSE_CANDIDATE_RE = re.compile(
    r"[$€£]|\b(?:usd|cad|gbp|eur)\b|"
    r"\b\d{2,6}(?:,\d{3})*(?:\.\d+)?\s*[kK]?\s*(?:-|to|–)\s*\d{2,6}(?:,\d{3})*(?:\.\d+)?\s*[kK]?\b|"
    r"\b(?:salary|compensation|base pay|pay range|hourly rate|annual salary|annual pay)\b",
    re.IGNORECASE,
)
_SKILL_ALIASES = {
    "python": ("python",),
    "sql": ("sql",),
    "javascript": ("javascript", "js"),
    "typescript": ("typescript", "ts"),
    "react": ("react", "reactjs"),
    "node": ("node", "nodejs", "node.js"),
    "aws": ("aws", "amazon web services"),
    "gcp": ("gcp", "google cloud", "google cloud platform"),
    "azure": ("azure", "microsoft azure"),
    "docker": ("docker",),
    "kubernetes": ("kubernetes", "k8s"),
    "spark": ("spark", "apache spark", "pyspark"),
    "airflow": ("airflow", "apache airflow"),
    "dbt": ("dbt",),
    "tableau": ("tableau",),
    "power bi": ("power bi", "powerbi"),
    "excel": ("excel", "microsoft excel"),
    "java": ("java",),
    "c++": ("c++", "cpp"),
    "c#": ("c#", "csharp"),
    "go": ("go", "golang"),
    "rust": ("rust",),
    "postgres": ("postgres", "postgresql"),
    "mysql": ("mysql",),
    "snowflake": ("snowflake",),
    "bigquery": ("bigquery",),
    "tensorflow": ("tensorflow",),
    "pytorch": ("pytorch",),
    "machine learning": ("machine learning", "ml"),
    "nlp": ("nlp", "natural language processing"),
}
_SKILL_ALIAS_TO_CANONICAL = {
    alias: canonical
    for canonical, aliases in _SKILL_ALIASES.items()
    for alias in aliases
}
_SKILL_REQUIRED_HINTS = ("required", "must have", "must-haves", "minimum qualifications")
_SKILL_PREFERRED_HINTS = ("preferred", "nice to have", "plus", "bonus")


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


def _looks_unknown_location(value: str) -> bool:
    normalized = _normalize_token(value)
    return normalized in _UNKNOWN_LOCATION_VALUES


def _normalize_skill_token(value: str) -> str | None:
    normalized = _normalize_token(value).replace("-", " ")
    return _SKILL_ALIAS_TO_CANONICAL.get(normalized)


def _compile_skill_pattern(alias: str) -> re.Pattern[str]:
    escaped = re.escape(alias).replace(r"\ ", r"\s+")
    return re.compile(rf"(?<![a-z0-9+#]){escaped}(?![a-z0-9+#])", re.IGNORECASE)


_SKILL_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    (canonical, _compile_skill_pattern(alias))
    for alias, canonical in sorted(_SKILL_ALIAS_TO_CANONICAL.items())
]
_SKILL_EXTRACT_ALIASES = sorted(_SKILL_ALIAS_TO_CANONICAL, key=len, reverse=True)
_SKILL_EXTRACT_RE = re.compile(
    r"(?<![a-z0-9+#])("
    + "|".join(re.escape(alias).replace(r"\ ", r"\s+") for alias in _SKILL_EXTRACT_ALIASES)
    + r")(?![a-z0-9+#])",
    re.IGNORECASE,
)


def _normalize_skills_value(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, float) and pd.isna(value):
        return []
    values: list[str]
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return []
        delimiter = ";" if ";" in stripped else ("," if "," in stripped else None)
        values = [part.strip() for part in stripped.split(delimiter)] if delimiter else [stripped]
    elif isinstance(value, (list, tuple, set)):
        values = [str(item).strip() for item in value if str(item).strip()]
    else:
        values = [str(value).strip()]

    normalized = {
        canonical
        for item in values
        for canonical in [_normalize_skill_token(item)]
        if canonical is not None
    }
    return sorted(normalized)


def _extract_skills_from_text(text: str) -> list[str]:
    if not text:
        return []
    detected: set[str] = set()
    for match in _SKILL_EXTRACT_RE.finditer(text):
        canonical = _normalize_skill_token(match.group(0))
        if canonical is not None:
            detected.add(canonical)
    return sorted(detected)


def _extract_skills_from_text_series(text: pd.Series) -> pd.Series:
    lowered = text.astype("string").fillna("").str.lower()
    extracted = pd.Series([[] for _ in range(len(lowered))], index=lowered.index, dtype="object")
    if lowered.empty:
        return extracted
    matches = lowered.str.extractall(_SKILL_EXTRACT_RE)
    if matches.empty:
        return extracted
    canonical = matches[0].map(_normalize_skill_token).dropna()
    if canonical.empty:
        return extracted
    grouped = canonical.groupby(level=0).agg(lambda values: sorted(set(values)))
    extracted.loc[grouped.index] = grouped.astype("object")
    return extracted


def _parse_salary_number(value: str, has_k_suffix: bool) -> float:
    normalized = value.replace(",", "")
    if "." in normalized and normalized.replace(".", "").isdigit():
        parts = normalized.split(".")
        if len(parts) > 1 and all(len(part) == 3 for part in parts[1:]):
            normalized = "".join(parts)
    parsed = float(normalized)
    if has_k_suffix:
        parsed *= 1000.0
    return parsed


def _infer_salary_currency(currency_token: str, lowered: str) -> str | None:
    currency = _SALARY_CURRENCY_MAP.get(currency_token) if currency_token else None
    if currency is not None:
        return currency
    if " cad" in lowered or " canadian dollar" in lowered:
        return "CAD"
    if " usd" in lowered or " us dollar" in lowered:
        return "USD"
    return None


def _extract_salary_from_text(value: str) -> tuple[float | None, float | None, str | None, str | None]:
    if not value:
        return None, None, None, None
    match = _SALARY_CAPTURE_RE.search(value)
    low: float
    high: float
    currency_token = ""
    lowered = value.lower()
    if match:
        low = _parse_salary_number(match.group("low"), bool(match.group("low_k")))
        high = _parse_salary_number(match.group("high"), bool(match.group("high_k")))
        currency_token = (match.group("prefix") or match.group("prefix2") or "").strip().lower()
    else:
        if not _SALARY_CONTEXT_RE.search(value):
            return None, None, None, None
        single_matches = list(_SALARY_SINGLE_RE.finditer(value))
        if not single_matches:
            return None, None, None, None
        parsed_values = [
            _parse_salary_number(single.group("amount"), bool(single.group("amount_k")))
            for single in single_matches
        ]
        if len({round(amount, 2) for amount in parsed_values}) > 2:
            return None, None, None, None
        low = min(parsed_values)
        high = max(parsed_values)
        currency_token = (single_matches[0].group("prefix") or "").strip().lower()

    if low <= 0 or high <= 0:
        return None, None, None, None
    low, high = min(low, high), max(low, high)
    if high < 10:
        return None, None, None, None
    currency = _infer_salary_currency(currency_token, lowered)
    interval: str | None = None
    for pattern, label in _SALARY_INTERVAL_PATTERNS:
        if pattern.search(value):
            interval = label
            break
    return low, high, currency, interval


def _extract_salary_from_text_series(
    text: pd.Series,
) -> tuple[pd.Series, pd.Series, pd.Series, pd.Series]:
    parsed = text.astype("string").fillna("").map(_extract_salary_from_text)
    mins = parsed.map(lambda value: value[0]).astype("float64")
    maxs = parsed.map(lambda value: value[1]).astype("float64")
    currencies = parsed.map(lambda value: value[2]).astype("object")
    intervals = parsed.map(lambda value: value[3]).astype("object")
    return mins, maxs, currencies, intervals


def _infer_remote_from_context(
    df: pd.DataFrame,
    *,
    title_column: str = TITLE,
    description_column: str = DESCRIPTION_TEXT,
    location_column: str = LOCATION_RAW,
    remote_allowed_column: str = "remote_allowed",
    remote_scope_column: str = "remote_scope",
) -> pd.Series:
    columns = [title_column, description_column, location_column, remote_scope_column]
    text = _combined_lower_text(df, columns)
    positive = text.str.contains(_REMOTE_CONTEXT_RE, na=False)
    negative = text.str.contains(_REMOTE_NEGATIVE_RE, na=False)
    inferred = positive & ~negative
    if remote_allowed_column in df.columns:
        allowed = (
            df[remote_allowed_column]
            .astype("string")
            .fillna("")
            .str.strip()
            .str.lower()
            .isin({"true", "1", "yes", "remote"})
        )
        inferred |= allowed
    return inferred.astype("bool")


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
    if _looks_unknown_location(cleaned):
        return LocationResult(None, None, None, None)
    remote_type = "remote" if _detect_remote(cleaned) else None
    primary = _extract_primary_location(cleaned)
    parts = [part.strip() for part in primary.split(",") if part.strip()]
    city, region, country = _classify_parts(parts)
    if city is not None and _looks_unknown_location(city):
        city = None
    if city and (region is not None or country is not None) and "," in city:
        if not city.strip()[:1].isdigit():
            city = city.split(",", 1)[0].strip()
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
    unknown_mask = stripped.str.lower().isin(_UNKNOWN_LOCATION_VALUES).fillna(False).astype("bool")
    parse_mask = string_mask & non_empty_mask & ~unknown_mask

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

    context_remote = _infer_remote_from_context(result, location_column=location_column)
    if remote_flag_column in result.columns:
        remote_flags = result[remote_flag_column].fillna(False).astype(bool).tolist()
        remote_values = [
            "remote" if (flag or remote == "remote" or context) else None
            for flag, remote, context in zip(remote_flags, remote_types, context_remote.tolist())
        ]
        result[remote_type_column] = pd.Series(remote_values, dtype="object")
    else:
        remote_values = [
            "remote" if (remote == "remote" or context) else None
            for remote, context in zip(remote_types, context_remote.tolist())
        ]
        result[remote_type_column] = pd.Series(remote_values, dtype="object")
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
    description_column: str = DESCRIPTION_TEXT,
    salary_min_column: str = SALARY_MIN,
    salary_max_column: str = SALARY_MAX,
    salary_currency_column: str = SALARY_CURRENCY,
    salary_interval_column: str = SALARY_INTERVAL,
) -> pd.DataFrame:
    if salary_text_column not in df.columns:
        return df
    result = df.copy()

    salary_text = (
        result[salary_text_column].astype("string").fillna("")
        if salary_text_column in result.columns
        else pd.Series("", index=result.index, dtype="string")
    )
    description_text = (
        result[description_column]
        .astype("string")
        .fillna("")
        .str.slice(0, _SALARY_PARSE_TEXT_MAX_CHARS)
        if description_column in result.columns
        else pd.Series("", index=result.index, dtype="string")
    )
    salary_present = salary_text.str.strip().ne("")
    source_text = pd.Series("", index=result.index, dtype="string")
    parsed_source = pd.Series("none", index=result.index, dtype="object")
    if bool(salary_present.any()):
        source_text.loc[salary_present] = salary_text.loc[salary_present]
        parsed_source.loc[salary_present] = "salary_text"
    description_candidates = (~salary_present) & description_text.str.contains(
        _SALARY_PARSE_CANDIDATE_RE, na=False
    )
    if bool(description_candidates.any()):
        source_text.loc[description_candidates] = description_text.loc[description_candidates]
        parsed_source.loc[description_candidates] = "description_text"
    parse_candidates = source_text.str.strip().ne("")
    parsed_min = pd.Series(float("nan"), index=result.index, dtype="float64")
    parsed_max = pd.Series(float("nan"), index=result.index, dtype="float64")
    parsed_currency = pd.Series([None] * len(result), index=result.index, dtype="object")
    parsed_interval = pd.Series([None] * len(result), index=result.index, dtype="object")
    if bool(parse_candidates.any()):
        subset_min, subset_max, subset_currency, subset_interval = _extract_salary_from_text_series(
            source_text.loc[parse_candidates]
        )
        parsed_min.loc[parse_candidates] = subset_min
        parsed_max.loc[parse_candidates] = subset_max
        parsed_currency.loc[parse_candidates] = subset_currency
        parsed_interval.loc[parse_candidates] = subset_interval
    parsed_has_salary = parsed_min.notna() | parsed_max.notna()

    existing_min = (
        pd.to_numeric(result[salary_min_column], errors="coerce")
        if salary_min_column in result.columns
        else pd.Series(float("nan"), index=result.index, dtype="float64")
    )
    existing_max = (
        pd.to_numeric(result[salary_max_column], errors="coerce")
        if salary_max_column in result.columns
        else pd.Series(float("nan"), index=result.index, dtype="float64")
    )
    existing_has_salary = existing_min.notna() | existing_max.notna()
    result[salary_min_column] = existing_min.where(existing_min.notna(), parsed_min)
    result[salary_max_column] = existing_max.where(existing_max.notna(), parsed_max)
    result[salary_min_column] = result[salary_min_column].astype("object").where(
        pd.notna(result[salary_min_column]),
        None,
    )
    result[salary_max_column] = result[salary_max_column].astype("object").where(
        pd.notna(result[salary_max_column]),
        None,
    )

    existing_currency_present = pd.Series(False, index=result.index, dtype="bool")
    if salary_currency_column in result.columns:
        existing_currency = result[salary_currency_column].astype("string").fillna("").str.strip()
        existing_currency_present = existing_currency.ne("")
        result[salary_currency_column] = existing_currency.where(
            existing_currency.ne(""),
            parsed_currency,
        ).astype("object")
        result[salary_currency_column] = result[salary_currency_column].where(
            result[salary_currency_column].notna(), None
        )
    else:
        result[salary_currency_column] = parsed_currency.where(
            parsed_currency.notna(),
            "USD",
        ).astype("object")

    existing_interval_present = pd.Series(False, index=result.index, dtype="bool")
    if salary_interval_column in result.columns:
        existing_interval = result[salary_interval_column].astype("string").fillna("").str.strip()
        existing_interval_present = existing_interval.ne("")
        result[salary_interval_column] = existing_interval.where(
            existing_interval.ne(""),
            parsed_interval,
        ).astype("object")
        result[salary_interval_column] = result[salary_interval_column].where(
            result[salary_interval_column].notna(), None
        )
    else:
        result[salary_interval_column] = parsed_interval.where(
            parsed_interval.notna(),
            "year",
        ).astype("object")

    salary_source = pd.Series("none", index=result.index, dtype="object")
    salary_source.loc[existing_has_salary] = "existing"
    parsed_new = ~existing_has_salary & parsed_has_salary
    salary_source.loc[parsed_new] = parsed_source.loc[parsed_new]
    result[SALARY_SOURCE] = salary_source

    salary_confidence = pd.Series(0.0, index=result.index, dtype="float64")
    existing_coherent = existing_has_salary & existing_currency_present & existing_interval_present
    salary_confidence.loc[existing_has_salary] = 0.85
    salary_confidence.loc[existing_coherent] = 1.0

    parsed_explicit = parsed_new & parsed_currency.notna() & parsed_interval.notna()
    from_salary_text = parsed_new & parsed_source.eq("salary_text")
    from_description = parsed_new & parsed_source.eq("description_text")
    salary_confidence.loc[from_salary_text] = 0.55
    salary_confidence.loc[from_description] = 0.55
    salary_confidence.loc[from_salary_text & parsed_explicit] = 0.85
    salary_confidence.loc[from_description & parsed_explicit] = 0.70
    result[SALARY_CONFIDENCE] = salary_confidence

    annual_min = pd.Series(float("nan"), index=result.index, dtype="float64")
    annual_max = pd.Series(float("nan"), index=result.index, dtype="float64")
    min_numeric = pd.to_numeric(result[salary_min_column], errors="coerce")
    max_numeric = pd.to_numeric(result[salary_max_column], errors="coerce")
    interval_normalized = (
        result[salary_interval_column].astype("string").fillna("").str.strip().str.lower()
    )
    multiplier = interval_normalized.map(_ANNUAL_INTERVAL_MULTIPLIERS).astype("float64")

    known_interval = multiplier.notna()
    if bool(known_interval.any()):
        annual_min.loc[known_interval] = min_numeric.loc[known_interval] * multiplier.loc[known_interval]
        annual_max.loc[known_interval] = max_numeric.loc[known_interval] * multiplier.loc[known_interval]

    annual_like_min = min_numeric.where(min_numeric.ge(15000.0))
    annual_like_max = max_numeric.where(max_numeric.ge(15000.0))
    annual_min = annual_min.where(annual_min.notna(), annual_like_min)
    annual_max = annual_max.where(annual_max.notna(), annual_like_max)

    result[SALARY_ANNUAL_MIN] = annual_min.astype("object").where(annual_min.notna(), None)
    result[SALARY_ANNUAL_MAX] = annual_max.astype("object").where(annual_max.notna(), None)
    return result


def normalize_skills(
    df: pd.DataFrame,
    *,
    skills_column: str = SKILLS,
    title_column: str = TITLE,
    description_column: str = DESCRIPTION_TEXT,
) -> pd.DataFrame:
    if skills_column not in df.columns and title_column not in df.columns and description_column not in df.columns:
        return df
    result = df.copy()
    existing = (
        result[skills_column].map(_normalize_skills_value)
        if skills_column in result.columns
        else pd.Series([[] for _ in range(len(result))], index=result.index, dtype="object")
    )
    missing_skills = existing.map(len).eq(0)

    text = pd.Series("", index=result.index, dtype="string")
    if title_column in result.columns:
        text = text.str.cat(result[title_column].astype("string").fillna(""), sep=" ")
    if description_column in result.columns:
        text = text.str.cat(
            result[description_column]
            .astype("string")
            .fillna("")
            .str.slice(0, _SKILL_PARSE_TEXT_MAX_CHARS),
            sep=" ",
        )

    extracted = pd.Series([[] for _ in range(len(result))], index=result.index, dtype="object")
    if bool(missing_skills.any()):
        extracted.loc[missing_skills] = _extract_skills_from_text_series(text.loc[missing_skills])
    merged = existing.where(~missing_skills, extracted)
    result[skills_column] = merged.map(lambda value: value if value else []).astype("object")

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
