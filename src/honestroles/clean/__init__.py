from __future__ import annotations

import pandas as pd

from honestroles.clean.dedup import deduplicate
from honestroles.clean.historical import (
    HistoricalCleanOptions,
    clean_historical_jobs,
    detect_historical_listing_pages,
)
from honestroles.clean.html import strip_html
from honestroles.clean.normalize import (
    enrich_country_from_context,
    normalize_employment_types,
    normalize_locations,
    normalize_salaries,
    normalize_skills,
)

__all__ = [
    "clean_jobs",
    "clean_historical_jobs",
    "HistoricalCleanOptions",
    "detect_historical_listing_pages",
    "strip_html",
    "normalize_locations",
    "enrich_country_from_context",
    "normalize_salaries",
    "normalize_skills",
    "normalize_employment_types",
    "deduplicate",
]


def clean_jobs(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = strip_html(df)
    cleaned = normalize_locations(cleaned)
    cleaned = enrich_country_from_context(cleaned)
    cleaned = normalize_salaries(cleaned)
    cleaned = normalize_skills(cleaned)
    cleaned = normalize_employment_types(cleaned)
    cleaned = deduplicate(cleaned)
    return cleaned
