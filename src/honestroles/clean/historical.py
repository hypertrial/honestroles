from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Literal

import pandas as pd

from honestroles.clean.dedup import compact_snapshots
from honestroles.clean.html import strip_html
from honestroles.clean.normalize import (
    enrich_country_from_context,
    normalize_employment_types,
    normalize_locations,
    normalize_salaries,
    normalize_skills,
)
from honestroles.io.contract import normalize_source_data_contract
from honestroles.schema import CONTENT_HASH, INGESTED_AT, JOB_ID, JOB_KEY, LOCATION_RAW, TITLE

LOGGER = logging.getLogger(__name__)

HISTORICAL_IS_LISTING_PAGE = "historical_is_listing_page"
FIRST_SEEN = "first_seen"
LAST_SEEN = "last_seen"
SNAPSHOT_COUNT = "snapshot_count"
_JOB_ID_SLUG_RE = re.compile(r"^[a-z0-9-]{3,40}$")


@dataclass(frozen=True)
class HistoricalCleanOptions:
    detect_listing_pages: bool = True
    drop_listing_pages: bool = True
    compact_snapshots: bool = True
    prefer_existing_description_text: bool = True
    snapshot_timestamp_output: Literal["iso8601", "datetime"] = "datetime"
    compaction_keys: tuple[str, ...] = (JOB_KEY, CONTENT_HASH)
    ingested_at_column: str = INGESTED_AT


def detect_historical_listing_pages(
    df: pd.DataFrame,
    *,
    location_column: str = LOCATION_RAW,
    title_column: str = TITLE,
    job_id_column: str = JOB_ID,
) -> pd.Series:
    if (
        location_column not in df.columns
        or title_column not in df.columns
        or job_id_column not in df.columns
    ):
        return pd.Series([False] * len(df), index=df.index, dtype="bool")

    location_unknown = (
        df[location_column].fillna("").astype(str).str.strip().str.lower().eq("unknown")
    )
    title_jobs = df[title_column].fillna("").astype(str).str.strip().str.lower().str.endswith(" jobs")
    job_id_slug = df[job_id_column].fillna("").astype(str).str.strip().str.fullmatch(_JOB_ID_SLUG_RE)
    return (location_unknown & title_jobs & job_id_slug).astype("bool")


def _duplicate_ratio(df: pd.DataFrame, keys: tuple[str, ...]) -> float:
    present_keys = [key for key in keys if key in df.columns]
    if not present_keys or df.empty:
        return 0.0
    unique = int(df[present_keys].drop_duplicates().shape[0])
    return 1.0 - (unique / len(df))


def clean_historical_jobs(
    df: pd.DataFrame,
    *,
    options: HistoricalCleanOptions | None = None,
) -> pd.DataFrame:
    opts = options or HistoricalCleanOptions()
    result = normalize_source_data_contract(df)

    if opts.detect_listing_pages:
        listing_mask = detect_historical_listing_pages(result)
    else:
        listing_mask = pd.Series([False] * len(result), index=result.index, dtype="bool")
    result[HISTORICAL_IS_LISTING_PAGE] = listing_mask

    listing_rows_detected = int(listing_mask.sum())
    listing_rows_dropped = 0
    if opts.drop_listing_pages and listing_rows_detected > 0:
        result = result.loc[~listing_mask].reset_index(drop=True)
        listing_rows_dropped = listing_rows_detected

    duplicate_ratio_before = _duplicate_ratio(result, opts.compaction_keys)
    rows_before_compact = len(result)
    if opts.compact_snapshots:
        result = compact_snapshots(
            result,
            key_columns=opts.compaction_keys,
            timestamp_column=opts.ingested_at_column,
            first_seen_column=FIRST_SEEN,
            last_seen_column=LAST_SEEN,
            snapshot_count_column=SNAPSHOT_COUNT,
            timestamp_output=opts.snapshot_timestamp_output,
        )
    rows_compacted = rows_before_compact - len(result)
    duplicate_ratio_after = _duplicate_ratio(result, opts.compaction_keys)

    cleaned = strip_html(
        result,
        overwrite_existing=not opts.prefer_existing_description_text,
    )
    cleaned = normalize_locations(cleaned)
    cleaned = enrich_country_from_context(cleaned)
    cleaned = normalize_salaries(cleaned)
    cleaned = normalize_skills(cleaned)
    cleaned = normalize_employment_types(cleaned)

    LOGGER.info(
        (
            "historical_cleaning listing_rows_detected=%d listing_rows_dropped=%d "
            "rows_compacted=%d duplicate_ratio_before=%.4f duplicate_ratio_after=%.4f"
        ),
        listing_rows_detected,
        listing_rows_dropped,
        rows_compacted,
        duplicate_ratio_before,
        duplicate_ratio_after,
    )
    return cleaned.reset_index(drop=True)
