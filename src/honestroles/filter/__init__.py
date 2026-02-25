from __future__ import annotations

import pandas as pd

from honestroles.filter.chain import FilterChain
from honestroles.filter.predicates import (
    by_active_likelihood,
    by_application_friction,
    by_completeness,
    by_employment_type,
    by_entry_level,
    by_experience,
    by_keywords,
    by_location,
    by_recency,
    by_salary,
    by_skills,
    by_visa_requirements,
)
from honestroles.plugins import apply_filter_plugins

__all__ = [
    "FilterChain",
    "filter_jobs",
    "by_location",
    "by_salary",
    "by_skills",
    "by_keywords",
    "by_recency",
    "by_completeness",
    "by_employment_type",
    "by_entry_level",
    "by_experience",
    "by_visa_requirements",
    "by_application_friction",
    "by_active_likelihood",
]


def filter_jobs(
    df: pd.DataFrame,
    *,
    cities: list[str] | None = None,
    regions: list[str] | None = None,
    countries: list[str] | None = None,
    remote_only: bool = False,
    min_salary: float | None = None,
    max_salary: float | None = None,
    currency: str | None = None,
    required_skills: list[str] | None = None,
    excluded_skills: list[str] | None = None,
    include_keywords: list[str] | None = None,
    exclude_keywords: list[str] | None = None,
    keyword_columns: list[str] | None = None,
    required_fields: list[str] | None = None,
    posted_within_days: int | None = None,
    seen_within_days: int | None = None,
    as_of: str | pd.Timestamp | None = None,
    employment_types: list[str] | None = None,
    entry_level_only: bool = False,
    max_experience_years: int | None = None,
    needs_visa_sponsorship: bool | None = None,
    include_unknown_visa: bool = True,
    max_application_friction: float | None = None,
    active_within_days: int | None = None,
    min_active_likelihood: float | None = None,
    plugin_filters: list[str] | None = None,
    plugin_filter_kwargs: dict[str, dict[str, object]] | None = None,
    plugin_filter_mode: str = "and",
) -> pd.DataFrame:
    chain = FilterChain()
    has_predicate = False
    if bool(cities or regions or countries or remote_only):
        chain.add(
            by_location,
            cities=cities,
            regions=regions,
            countries=countries,
            remote_only=remote_only,
        )
        has_predicate = True
    if min_salary is not None or max_salary is not None or bool(currency):
        chain.add(by_salary, min_salary=min_salary, max_salary=max_salary, currency=currency)
        has_predicate = True
    if bool(required_skills or excluded_skills):
        chain.add(by_skills, required=required_skills, excluded=excluded_skills)
        has_predicate = True
    if bool(include_keywords or exclude_keywords):
        chain.add(
            by_keywords,
            include=include_keywords,
            exclude=exclude_keywords,
            columns=keyword_columns,
        )
        has_predicate = True
    if bool(required_fields):
        chain.add(by_completeness, required_fields=required_fields)
        has_predicate = True
    if posted_within_days is not None or seen_within_days is not None:
        chain.add(
            by_recency,
            posted_within_days=posted_within_days,
            seen_within_days=seen_within_days,
            as_of=as_of,
        )
        has_predicate = True
    if bool(employment_types):
        chain.add(by_employment_type, employment_types=employment_types)
        has_predicate = True
    if entry_level_only:
        chain.add(by_entry_level, entry_level_only=entry_level_only)
        has_predicate = True
    if max_experience_years is not None:
        chain.add(by_experience, max_experience_years=max_experience_years)
        has_predicate = True
    if needs_visa_sponsorship is not None:
        chain.add(
            by_visa_requirements,
            needs_visa_sponsorship=needs_visa_sponsorship,
            include_unknown_visa=include_unknown_visa,
        )
        has_predicate = True
    if max_application_friction is not None:
        chain.add(by_application_friction, max_application_friction=max_application_friction)
        has_predicate = True
    if active_within_days is not None or min_active_likelihood is not None:
        chain.add(
            by_active_likelihood,
            active_within_days=active_within_days,
            min_active_likelihood=min_active_likelihood,
            as_of=as_of,
        )
        has_predicate = True

    filtered = chain.apply(df) if has_predicate else df.reset_index(drop=True)
    if not plugin_filters:
        return filtered
    return apply_filter_plugins(
        filtered,
        plugin_filters,
        mode=plugin_filter_mode,
        plugin_kwargs=plugin_filter_kwargs,
    )
