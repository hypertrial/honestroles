from __future__ import annotations

import pandas as pd

from honestroles.clean.dedup import deduplicate
from honestroles.clean.html import strip_html
from honestroles.clean.normalize import (
    normalize_employment_types,
    enrich_country_from_context,
    normalize_locations,
    normalize_salaries,
)

__all__ = [
    "clean_jobs",
    "strip_html",
    "normalize_locations",
    "enrich_country_from_context",
    "normalize_salaries",
    "normalize_employment_types",
    "deduplicate",
]


def clean_jobs(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = strip_html(df)
    cleaned = normalize_locations(cleaned)
    cleaned = enrich_country_from_context(cleaned)
    cleaned = normalize_salaries(cleaned)
    cleaned = normalize_employment_types(cleaned)
    cleaned = deduplicate(cleaned)
    return cleaned
