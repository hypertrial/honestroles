from __future__ import annotations

import pandas as pd

from honestroles.filter.chain import FilterChain
from honestroles.filter.predicates import (
    by_completeness,
    by_keywords,
    by_location,
    by_salary,
    by_skills,
)
from honestroles.plugins import apply_filter_plugins

__all__ = [
    "FilterChain",
    "filter_jobs",
    "by_location",
    "by_salary",
    "by_skills",
    "by_keywords",
    "by_completeness",
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
    currency: str | None = "USD",
    required_skills: list[str] | None = None,
    excluded_skills: list[str] | None = None,
    include_keywords: list[str] | None = None,
    exclude_keywords: list[str] | None = None,
    keyword_columns: list[str] | None = None,
    required_fields: list[str] | None = None,
    plugin_filters: list[str] | None = None,
    plugin_filter_kwargs: dict[str, dict[str, object]] | None = None,
    plugin_filter_mode: str = "and",
) -> pd.DataFrame:
    chain = FilterChain()
    chain.add(
        by_location,
        cities=cities,
        regions=regions,
        countries=countries,
        remote_only=remote_only,
    )
    chain.add(by_salary, min_salary=min_salary, max_salary=max_salary, currency=currency)
    chain.add(by_skills, required=required_skills, excluded=excluded_skills)
    chain.add(
        by_keywords, include=include_keywords, exclude=exclude_keywords, columns=keyword_columns
    )
    chain.add(by_completeness, required_fields=required_fields)
    filtered = chain.apply(df)
    if not plugin_filters:
        return filtered
    return apply_filter_plugins(
        filtered,
        plugin_filters,
        mode=plugin_filter_mode,
        plugin_kwargs=plugin_filter_kwargs,
    )
