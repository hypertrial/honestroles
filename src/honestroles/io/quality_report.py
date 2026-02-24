from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
import re
from urllib.parse import urlparse

import numpy as np
import pandas as pd

from honestroles.schema import APPLY_URL, CONTENT_HASH, JOB_KEY, LOCATION_RAW, REMOTE_FLAG, REQUIRED_COLUMNS, SOURCE

_ENRICHMENT_COLUMNS = (
    "salary_text",
    "salary_min",
    "salary_max",
    "salary_currency",
    "salary_interval",
    "skills",
    "languages",
    "benefits",
    "keywords",
    "visa_sponsorship",
    "remote_type",
    "posted_at",
    "updated_at",
)
_LISTING_JOB_ID_RE = re.compile(r"^[a-z0-9-]{3,40}$")


def _detect_listing_pages(df: pd.DataFrame) -> pd.Series:
    required = {LOCATION_RAW, "title", "job_id"}
    if not required.issubset(df.columns):
        return pd.Series([False] * len(df), index=df.index, dtype="bool")
    location_unknown = (
        df[LOCATION_RAW].fillna("").astype(str).str.strip().str.lower().eq("unknown")
    )
    title_jobs = df["title"].fillna("").astype(str).str.strip().str.lower().str.endswith(" jobs")
    job_id_slug = df["job_id"].fillna("").astype(str).str.strip().str.fullmatch(_LISTING_JOB_ID_RE)
    return (location_unknown & title_jobs & job_id_slug).astype("bool")


@dataclass(frozen=True)
class DataQualityReport:
    dataset_name: str | None
    row_count: int
    column_count: int
    required_field_null_counts: dict[str, int]
    required_field_empty_counts: dict[str, int]
    top_duplicate_job_keys: list[dict[str, object]]
    top_duplicate_content_hashes: list[dict[str, object]]
    listing_page_rows: int
    listing_page_ratio: float
    source_row_counts: dict[str, int]
    source_quality: dict[str, dict[str, float]]
    enrichment_sparsity_pct: dict[str, float]
    invalid_apply_url_count: int
    unknown_location_count: int

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


class DataQualityAccumulator:
    def __init__(self, *, dataset_name: str | None = None, top_n_duplicates: int = 10) -> None:
        self.dataset_name = dataset_name
        self.top_n_duplicates = top_n_duplicates
        self.row_count = 0
        self.columns: set[str] = set()
        self.required_field_null_counts = {column: 0 for column in sorted(REQUIRED_COLUMNS)}
        self.required_field_empty_counts = {column: 0 for column in sorted(REQUIRED_COLUMNS)}
        self.job_key_counter: Counter[str] = Counter()
        self.content_hash_counter: Counter[str] = Counter()
        self.listing_page_rows = 0
        self.invalid_apply_url_count = 0
        self.unknown_location_count = 0
        self.source_row_counts: Counter[str] = Counter()
        self.source_unknown_location_counts: Counter[str] = Counter()
        self.source_missing_description_counts: Counter[str] = Counter()
        self.source_remote_true_counts: Counter[str] = Counter()
        self.enrichment_non_null_counts: dict[str, int] = {
            column: 0 for column in _ENRICHMENT_COLUMNS
        }

    def update(self, df: pd.DataFrame) -> None:
        if df.empty:
            self.columns.update(df.columns)
            return

        self.row_count += len(df)
        self.columns.update(df.columns)
        self._update_required_counts(df)
        self._update_duplicate_counts(df)
        self._update_listing_counts(df)
        self._update_url_counts(df)
        self._update_location_counts(df)
        self._update_source_counts(df)
        self._update_enrichment_counts(df)

    def finalize(self) -> DataQualityReport:
        source_quality: dict[str, dict[str, float]] = {}
        for source_name, total in self.source_row_counts.items():
            source_quality[source_name] = {
                "unknown_location_pct": self._pct(self.source_unknown_location_counts[source_name], total),
                "missing_description_pct": self._pct(
                    self.source_missing_description_counts[source_name],
                    total,
                ),
                "remote_true_pct": self._pct(self.source_remote_true_counts[source_name], total),
            }

        enrichment_sparsity_pct = {
            column: self._pct(self.row_count - count, self.row_count)
            for column, count in self.enrichment_non_null_counts.items()
        }
        return DataQualityReport(
            dataset_name=self.dataset_name,
            row_count=self.row_count,
            column_count=len(self.columns),
            required_field_null_counts=dict(self.required_field_null_counts),
            required_field_empty_counts=dict(self.required_field_empty_counts),
            top_duplicate_job_keys=self._top_duplicates(self.job_key_counter),
            top_duplicate_content_hashes=self._top_duplicates(self.content_hash_counter),
            listing_page_rows=self.listing_page_rows,
            listing_page_ratio=self._pct(self.listing_page_rows, self.row_count),
            source_row_counts=dict(self.source_row_counts),
            source_quality=source_quality,
            enrichment_sparsity_pct=enrichment_sparsity_pct,
            invalid_apply_url_count=self.invalid_apply_url_count,
            unknown_location_count=self.unknown_location_count,
        )

    def _update_required_counts(self, df: pd.DataFrame) -> None:
        for column in sorted(REQUIRED_COLUMNS):
            if column not in df.columns:
                self.required_field_null_counts[column] += len(df)
                self.required_field_empty_counts[column] += len(df)
                continue
            self.required_field_null_counts[column] += int(df[column].isna().sum())
            stripped = df[column].astype("string").fillna("").str.strip()
            self.required_field_empty_counts[column] += int(stripped.eq("").sum())

    def _update_duplicate_counts(self, df: pd.DataFrame) -> None:
        if JOB_KEY in df.columns:
            self.job_key_counter.update(df[JOB_KEY].fillna("<NULL>").astype(str).tolist())
        if CONTENT_HASH in df.columns:
            self.content_hash_counter.update(
                df[CONTENT_HASH].fillna("<NULL>").astype(str).tolist()
            )

    def _update_listing_counts(self, df: pd.DataFrame) -> None:
        listing_mask = _detect_listing_pages(df)
        self.listing_page_rows += int(listing_mask.sum())

    def _update_url_counts(self, df: pd.DataFrame) -> None:
        if APPLY_URL not in df.columns:
            return
        urls = df[APPLY_URL]
        non_missing = urls.notna()
        is_string = urls.map(lambda value: isinstance(value, str))
        invalid_type = non_missing & ~is_string
        self.invalid_apply_url_count += int(invalid_type.sum())

        string_values = urls[non_missing & is_string].astype(str).str.strip()
        invalid_url_count = 0
        for value in string_values.tolist():
            parsed = urlparse(value)
            if parsed.scheme not in {"http", "https"} or not parsed.netloc:
                invalid_url_count += 1
        self.invalid_apply_url_count += invalid_url_count

    def _update_location_counts(self, df: pd.DataFrame) -> None:
        if LOCATION_RAW not in df.columns:
            return
        unknown_mask = (
            df[LOCATION_RAW].fillna("").astype(str).str.strip().str.lower().eq("unknown")
        )
        self.unknown_location_count += int(unknown_mask.sum())

    def _update_source_counts(self, df: pd.DataFrame) -> None:
        if SOURCE not in df.columns:
            return
        source_values = df[SOURCE].fillna("<NULL>").astype(str)
        self.source_row_counts.update(source_values.tolist())

        unknown_mask = pd.Series([False] * len(df), index=df.index)
        if LOCATION_RAW in df.columns:
            unknown_mask = (
                df[LOCATION_RAW].fillna("").astype(str).str.strip().str.lower().eq("unknown")
            )
        missing_description_mask = pd.Series([False] * len(df), index=df.index)
        if "description_text" in df.columns:
            missing_description_mask = (
                df["description_text"].astype("string").fillna("").str.strip().eq("")
            )
        remote_true_mask = pd.Series([False] * len(df), index=df.index)
        if REMOTE_FLAG in df.columns:
            remote_true_mask = df[REMOTE_FLAG].map(
                lambda value: isinstance(value, (bool, np.bool_)) and bool(value)
            )

        for source_name in source_values.unique().tolist():
            source_mask = source_values.eq(source_name)
            self.source_unknown_location_counts[source_name] += int(
                (unknown_mask & source_mask).sum()
            )
            self.source_missing_description_counts[source_name] += int(
                (missing_description_mask & source_mask).sum()
            )
            self.source_remote_true_counts[source_name] += int((remote_true_mask & source_mask).sum())

    def _update_enrichment_counts(self, df: pd.DataFrame) -> None:
        for column in _ENRICHMENT_COLUMNS:
            if column not in df.columns:
                continue
            series = df[column]
            if column in {"skills", "languages", "benefits", "keywords"}:
                non_null = series.apply(lambda value: isinstance(value, list) and len(value) > 0)
            else:
                non_null = series.notna()
                if series.dtype == object:
                    non_null &= series.astype("string").fillna("").str.strip().ne("")
            self.enrichment_non_null_counts[column] += int(non_null.sum())

    def _top_duplicates(self, counter: Counter[str]) -> list[dict[str, object]]:
        top = []
        for key, count in counter.most_common(self.top_n_duplicates):
            if count <= 1:
                continue
            top.append({"value": key, "count": int(count)})
        return top

    @staticmethod
    def _pct(numerator: int, denominator: int) -> float:
        if denominator <= 0:
            return 0.0
        return round((100.0 * numerator) / denominator, 4)


def build_data_quality_report(
    df: pd.DataFrame,
    *,
    dataset_name: str | None = None,
    top_n_duplicates: int = 10,
) -> DataQualityReport:
    accumulator = DataQualityAccumulator(
        dataset_name=dataset_name,
        top_n_duplicates=top_n_duplicates,
    )
    accumulator.update(df)
    return accumulator.finalize()
