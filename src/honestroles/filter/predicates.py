from __future__ import annotations

import re
from typing import Iterable

import pandas as pd

from honestroles.schema import (
    ACTIVE_LIKELIHOOD,
    APPLICATION_FRICTION_SCORE,
    CITY,
    COUNTRY,
    DESCRIPTION_TEXT,
    EMPLOYMENT_TYPE,
    ENTRY_LEVEL_LIKELY,
    EXPERIENCE_YEARS_MIN,
    INGESTED_AT,
    LAST_SEEN,
    LOCATION_RAW,
    POSTED_AT,
    REQUIRED_SKILLS_EXTRACTED,
    REGION,
    REMOTE_FLAG,
    REMOTE_TYPE,
    SALARY_CURRENCY,
    SALARY_MAX,
    SALARY_MIN,
    SKILLS,
    TECH_STACK,
    TITLE,
    VISA_SPONSORSHIP_SIGNAL,
    WORK_AUTHORIZATION_REQUIRED,
    CITIZENSHIP_REQUIRED,
)


def _series_or_true(df: pd.DataFrame) -> pd.Series:
    return pd.Series([True] * len(df), index=df.index)


def _skills_from_value(value: object) -> set[str]:
    if value is None:
        return set()
    if isinstance(value, float) and pd.isna(value):
        return set()
    if isinstance(value, str):
        stripped = value.strip().lower()
        return {stripped} if stripped else set()
    if isinstance(value, (list, tuple, set)):
        return {str(item).strip().lower() for item in value if str(item).strip()}
    return {str(value).strip().lower()}


def _short_token_pattern(term: str) -> re.Pattern[str] | None:
    if term.isalpha() and len(term) <= 3:
        escaped = re.escape(term)
        return re.compile(rf"(?<![a-z0-9]){escaped}(?![a-z0-9])", re.IGNORECASE)
    return None


def _resolve_as_of(as_of: str | pd.Timestamp | None) -> pd.Timestamp:
    if as_of is None:
        return pd.Timestamp.now(tz="UTC")
    parsed = pd.to_datetime(as_of, errors="coerce", utc=True)
    if pd.isna(parsed):
        return pd.Timestamp.now(tz="UTC")
    return pd.Timestamp(parsed)


_EMPLOYMENT_NORMALIZATION = {
    "full-time": "full_time",
    "full time": "full_time",
    "part-time": "part_time",
    "part time": "part_time",
}


def _normalize_employment(value: object) -> str:
    if not isinstance(value, str):
        return ""
    lowered = value.strip().lower()
    if not lowered:
        return ""
    canonical = _EMPLOYMENT_NORMALIZATION.get(lowered, lowered)
    return canonical.replace("-", "_").replace(" ", "_")


def by_location(
    df: pd.DataFrame,
    *,
    cities: Iterable[str] | None = None,
    regions: Iterable[str] | None = None,
    countries: Iterable[str] | None = None,
    remote_only: bool = False,
) -> pd.Series:
    """Filter rows by city, region, country, and remote-only flag."""
    mask = _series_or_true(df)
    if cities and CITY in df.columns:
        allowed = {city.lower() for city in cities}
        mask &= df[CITY].fillna("").str.lower().isin(allowed)
    elif cities and LOCATION_RAW in df.columns:
        allowed_substrings = [city.lower() for city in cities if city]
        if allowed_substrings:
            city_pattern = "|".join(re.escape(city) for city in allowed_substrings)
            mask &= df[LOCATION_RAW].fillna("").str.lower().str.contains(city_pattern, regex=True)
    if countries and COUNTRY in df.columns:
        allowed = {country.lower() for country in countries}
        mask &= df[COUNTRY].fillna("").str.lower().isin(allowed)
    if regions and REGION in df.columns:
        allowed = {region.lower() for region in regions}
        mask &= df[REGION].fillna("").str.lower().isin(allowed)
    if remote_only:
        remote_mask = _series_or_true(df)
        has_remote_signal = False
        if REMOTE_FLAG in df.columns:
            remote_mask = df[REMOTE_FLAG].fillna(False).astype(bool)
            has_remote_signal = True
        if REMOTE_TYPE in df.columns:
            remote_type_mask = (
                df[REMOTE_TYPE].fillna("").astype(str).str.strip().str.lower().eq("remote")
            )
            remote_mask = (remote_mask | remote_type_mask) if has_remote_signal else remote_type_mask
            has_remote_signal = True
        if has_remote_signal:
            mask &= remote_mask
    return mask


def by_salary(
    df: pd.DataFrame,
    *,
    min_salary: float | None = None,
    max_salary: float | None = None,
    currency: str | None = None,
) -> pd.Series:
    if SALARY_MIN not in df.columns or SALARY_MAX not in df.columns:
        return _series_or_true(df)
    mask = _series_or_true(df)
    if currency and SALARY_CURRENCY in df.columns:
        mask &= df[SALARY_CURRENCY].fillna("").str.upper().eq(currency.upper())
    if min_salary is not None:
        mask &= df[SALARY_MAX].fillna(0) >= min_salary
    if max_salary is not None:
        mask &= df[SALARY_MIN].fillna(0) <= max_salary
    return mask


def by_employment_type(
    df: pd.DataFrame,
    *,
    employment_types: Iterable[str] | None = None,
) -> pd.Series:
    if not employment_types:
        return _series_or_true(df)
    if EMPLOYMENT_TYPE not in df.columns:
        return _series_or_true(df)
    allowed = {_normalize_employment(value) for value in employment_types if _normalize_employment(value)}
    if not allowed:
        return _series_or_true(df)
    normalized = df[EMPLOYMENT_TYPE].map(_normalize_employment)
    return normalized.isin(allowed)


def by_entry_level(
    df: pd.DataFrame,
    *,
    entry_level_only: bool = False,
) -> pd.Series:
    if not entry_level_only:
        return _series_or_true(df)
    has_entry = ENTRY_LEVEL_LIKELY in df.columns
    has_exp = EXPERIENCE_YEARS_MIN in df.columns
    if not has_entry and not has_exp:
        return _series_or_true(df)

    entry_mask = (
        df[ENTRY_LEVEL_LIKELY].map(lambda value: value is True)
        if has_entry
        else pd.Series(False, index=df.index, dtype="bool")
    )
    exp_mask = (
        pd.to_numeric(df[EXPERIENCE_YEARS_MIN], errors="coerce").le(2).fillna(False)
        if has_exp
        else pd.Series(False, index=df.index, dtype="bool")
    )
    return (entry_mask | exp_mask).astype("bool")


def by_experience(
    df: pd.DataFrame,
    *,
    max_experience_years: int | None = None,
) -> pd.Series:
    if max_experience_years is None:
        return _series_or_true(df)
    if EXPERIENCE_YEARS_MIN not in df.columns:
        return _series_or_true(df)
    minimum = pd.to_numeric(df[EXPERIENCE_YEARS_MIN], errors="coerce")
    return (minimum.le(max_experience_years) | minimum.isna()).astype("bool")


def by_visa_requirements(
    df: pd.DataFrame,
    *,
    needs_visa_sponsorship: bool | None = None,
    include_unknown_visa: bool = True,
) -> pd.Series:
    if needs_visa_sponsorship is not True:
        return _series_or_true(df)
    if VISA_SPONSORSHIP_SIGNAL not in df.columns:
        return _series_or_true(df)

    visa_series = df[VISA_SPONSORSHIP_SIGNAL]
    supports = visa_series.map(lambda value: value is True)
    unknown = visa_series.isna()
    mask = supports | (unknown & include_unknown_visa)

    if CITIZENSHIP_REQUIRED in df.columns:
        citizenship_required = df[CITIZENSHIP_REQUIRED].map(lambda value: value is True)
        mask &= ~citizenship_required
    if WORK_AUTHORIZATION_REQUIRED in df.columns:
        work_auth = df[WORK_AUTHORIZATION_REQUIRED].map(lambda value: value is True)
        mask &= ~(work_auth & ~supports & ~unknown)
    return mask.astype("bool")


def by_application_friction(
    df: pd.DataFrame,
    *,
    max_application_friction: float | None = None,
) -> pd.Series:
    if max_application_friction is None:
        return _series_or_true(df)
    if APPLICATION_FRICTION_SCORE not in df.columns:
        return _series_or_true(df)
    friction = pd.to_numeric(df[APPLICATION_FRICTION_SCORE], errors="coerce")
    return (friction.le(max_application_friction) | friction.isna()).astype("bool")


def by_active_likelihood(
    df: pd.DataFrame,
    *,
    active_within_days: int | None = None,
    min_active_likelihood: float | None = None,
    as_of: str | pd.Timestamp | None = None,
) -> pd.Series:
    if active_within_days is None and min_active_likelihood is None:
        return _series_or_true(df)

    mask = _series_or_true(df)
    anchor = _resolve_as_of(as_of)
    if active_within_days is not None:
        seen_column = LAST_SEEN if LAST_SEEN in df.columns else INGESTED_AT
        if seen_column in df.columns:
            seen = pd.to_datetime(df[seen_column], errors="coerce", utc=True, format="mixed")
            if seen_column == LAST_SEEN and INGESTED_AT in df.columns:
                ingested = pd.to_datetime(df[INGESTED_AT], errors="coerce", utc=True, format="mixed")
                seen = seen.where(seen.notna(), ingested)
            cutoff = anchor - pd.Timedelta(days=active_within_days)
            mask &= seen.ge(cutoff).fillna(False)
    if min_active_likelihood is not None and ACTIVE_LIKELIHOOD in df.columns:
        active_score = pd.to_numeric(df[ACTIVE_LIKELIHOOD], errors="coerce")
        mask &= active_score.ge(min_active_likelihood).fillna(False)
    return mask


def by_skills(
    df: pd.DataFrame,
    *,
    required: Iterable[str] | None = None,
    excluded: Iterable[str] | None = None,
) -> pd.Series:
    skill_columns = [
        column
        for column in (SKILLS, TECH_STACK, REQUIRED_SKILLS_EXTRACTED)
        if column in df.columns
    ]
    if not skill_columns:
        return _series_or_true(df)
    required_set = {skill.lower() for skill in required or []}
    excluded_set = {skill.lower() for skill in excluded or []}
    if not required_set and not excluded_set:
        return _series_or_true(df)

    combined = pd.Series([set() for _ in range(len(df))], index=df.index, dtype="object")
    for column in skill_columns:
        current = df[column].map(_skills_from_value)
        combined = combined.combine(current, lambda left, right: left.union(right))

    def matches(skill_set: set[str]) -> bool:
        if required_set and not required_set.issubset(skill_set):
            return False
        if excluded_set and excluded_set.intersection(skill_set):
            return False
        return True

    return combined.map(matches)


def by_keywords(
    df: pd.DataFrame,
    *,
    include: Iterable[str] | None = None,
    exclude: Iterable[str] | None = None,
    columns: Iterable[str] | None = None,
) -> pd.Series:
    include_terms = [term.lower() for term in (include or [])]
    exclude_terms = [term.lower() for term in (exclude or [])]
    if not include_terms and not exclude_terms:
        return _series_or_true(df)
    search_columns = list(columns or [TITLE, DESCRIPTION_TEXT])
    existing = [col for col in search_columns if col in df.columns]
    if not existing:
        return _series_or_true(df)

    if len(include_terms) == 1 and not exclude_terms:
        term = include_terms[0]
        if not term:
            return _series_or_true(df)
        short_pattern = _short_token_pattern(term)
        include_mask = pd.Series(False, index=df.index, dtype="bool")
        string_columns: list[pd.Series] = []
        for column in existing:
            column_text = df[column].astype("string")
            string_columns.append(column_text)
            if short_pattern is not None:
                include_mask |= column_text.str.contains(short_pattern, na=False)
            else:
                include_mask |= column_text.str.contains(
                    term,
                    case=False,
                    regex=False,
                    na=False,
                )

        # Preserve previous behavior where concatenated column boundaries could match spaced terms.
        if " " in term and len(string_columns) > 1:
            combined = pd.Series("", index=df.index, dtype="string")
            for column_text in string_columns:
                combined = combined.str.cat(column_text.fillna(""), sep=" ")
            include_mask |= combined.str.contains(
                term,
                case=False,
                regex=False,
                na=False,
            )
        return include_mask

    texts = pd.Series("", index=df.index, dtype="string")
    for column in existing:
        column_text = df[column].astype("string").fillna("").str.lower()
        texts = texts.str.cat(column_text, sep=" ")
    mask = _series_or_true(df)
    if include_terms:
        include_pattern = "|".join(re.escape(term) for term in include_terms if term)
        if include_pattern:
            mask &= texts.str.contains(include_pattern, regex=True)
    if exclude_terms:
        exclude_pattern = "|".join(re.escape(term) for term in exclude_terms if term)
        if exclude_pattern:
            mask &= ~texts.str.contains(exclude_pattern, regex=True)
    return mask


def by_recency(
    df: pd.DataFrame,
    *,
    posted_within_days: int | None = None,
    seen_within_days: int | None = None,
    as_of: str | pd.Timestamp | None = None,
) -> pd.Series:
    if posted_within_days is None and seen_within_days is None:
        return _series_or_true(df)
    anchor = _resolve_as_of(as_of)
    mask = _series_or_true(df)

    if posted_within_days is not None:
        posted_column = POSTED_AT if POSTED_AT in df.columns else INGESTED_AT
        if posted_column in df.columns:
            posted = pd.to_datetime(df[posted_column], errors="coerce", utc=True, format="mixed")
            cutoff = anchor - pd.Timedelta(days=posted_within_days)
            mask &= posted.ge(cutoff).fillna(False)

    if seen_within_days is not None:
        seen_column = LAST_SEEN if LAST_SEEN in df.columns else INGESTED_AT
        if seen_column in df.columns:
            seen = pd.to_datetime(df[seen_column], errors="coerce", utc=True, format="mixed")
            cutoff = anchor - pd.Timedelta(days=seen_within_days)
            mask &= seen.ge(cutoff).fillna(False)

    return mask


def by_completeness(df: pd.DataFrame, *, required_fields: Iterable[str] | None = None) -> pd.Series:
    if not required_fields:
        return _series_or_true(df)
    fields = [field for field in required_fields if field in df.columns]
    if not fields:
        return _series_or_true(df)
    mask = _series_or_true(df)
    for field in fields:
        mask &= df[field].notna()
    return mask
