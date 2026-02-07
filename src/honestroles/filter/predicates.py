from __future__ import annotations

from typing import Iterable

import pandas as pd

from honestroles.schema import (
    CITY,
    COUNTRY,
    DESCRIPTION_TEXT,
    LOCATION_RAW,
    REGION,
    REMOTE_FLAG,
    SALARY_CURRENCY,
    SALARY_MAX,
    SALARY_MIN,
    SKILLS,
    TITLE,
)


def _series_or_true(df: pd.DataFrame) -> pd.Series:
    return pd.Series([True] * len(df), index=df.index)


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
        allowed = [city.lower() for city in cities]
        mask &= df[LOCATION_RAW].fillna("").str.lower().apply(
            lambda value: any(city in value for city in allowed)
        )
    if countries and COUNTRY in df.columns:
        allowed = {country.lower() for country in countries}
        mask &= df[COUNTRY].fillna("").str.lower().isin(allowed)
    if regions and REGION in df.columns:
        allowed = {region.lower() for region in regions}
        mask &= df[REGION].fillna("").str.lower().isin(allowed)
    if remote_only and REMOTE_FLAG in df.columns:
        mask &= df[REMOTE_FLAG].fillna(False)
    return mask


def by_salary(
    df: pd.DataFrame,
    *,
    min_salary: float | None = None,
    max_salary: float | None = None,
    currency: str | None = "USD",
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


def by_skills(
    df: pd.DataFrame,
    *,
    required: Iterable[str] | None = None,
    excluded: Iterable[str] | None = None,
) -> pd.Series:
    if SKILLS not in df.columns:
        return _series_or_true(df)
    required_set = {skill.lower() for skill in required or []}
    excluded_set = {skill.lower() for skill in excluded or []}

    def matches(skills: object) -> bool:
        if skills is None:
            skill_list: list[str] = []
        elif isinstance(skills, float) and pd.isna(skills):
            skill_list = []
        elif isinstance(skills, list):
            skill_list = [str(skill) for skill in skills]
        else:
            skill_list = [str(skills)]
        skill_set = {skill.lower() for skill in skill_list}
        if required_set and not required_set.issubset(skill_set):
            return False
        if excluded_set and excluded_set.intersection(skill_set):
            return False
        return True

    return df[SKILLS].apply(matches)


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

    def row_text(row: pd.Series) -> str:
        parts = [str(row[col]) for col in existing if pd.notna(row[col])]
        return " ".join(parts).lower()

    texts = df.apply(row_text, axis=1)
    mask = _series_or_true(df)
    if include_terms:
        mask &= texts.apply(lambda text: any(term in text for term in include_terms))
    if exclude_terms:
        mask &= texts.apply(lambda text: all(term not in text for term in exclude_terms))
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
