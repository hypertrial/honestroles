from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from honestroles.errors import ConfigValidationError


def read_parquet(path: str | Path) -> pl.DataFrame:
    return pl.read_parquet(path)


def write_parquet(df: pl.DataFrame, path: str | Path) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(target)


def normalize_source_data_contract(df: pl.DataFrame) -> pl.DataFrame:
    required: tuple[str, ...] = (
        "id",
        "title",
        "company",
        "location",
        "remote",
        "description_text",
        "description_html",
        "skills",
        "salary_min",
        "salary_max",
        "apply_url",
        "posted_at",
    )
    missing = [name for name in required if name not in df.columns]
    if missing:
        df = df.with_columns(pl.lit(None).alias(name) for name in missing)
    return df.with_columns(
        pl.col("title").cast(pl.String, strict=False),
        pl.col("company").cast(pl.String, strict=False),
        pl.col("location").cast(pl.String, strict=False),
        pl.col("description_text").cast(pl.String, strict=False),
        pl.col("description_html").cast(pl.String, strict=False),
        pl.col("apply_url").cast(pl.String, strict=False),
        pl.col("salary_min").cast(pl.Float64, strict=False),
        pl.col("salary_max").cast(pl.Float64, strict=False),
        pl.col("posted_at").cast(pl.String, strict=False),
    )


def validate_source_data_contract(df: pl.DataFrame) -> pl.DataFrame:
    if "title" not in df.columns:
        raise ConfigValidationError("source data missing required column 'title'")
    if "description_text" not in df.columns and "description_html" not in df.columns:
        raise ConfigValidationError(
            "source data requires at least one of 'description_text' or 'description_html'"
        )
    return df


def _validate_read_query(query: str) -> str:
    normalized = query.strip()
    if not normalized:
        raise ConfigValidationError("query must be non-empty")
    if ";" in normalized:
        raise ConfigValidationError("query must not contain ';'")

    scrubbed = re.sub(r"(?is)/\*.*?\*/|--[^\n]*", " ", normalized)
    first_token_match = re.match(r"\s*([A-Za-z_]+)", scrubbed)
    if first_token_match is None:
        raise ConfigValidationError("query must begin with SELECT or WITH")
    first_token = first_token_match.group(1).lower()
    if first_token not in {"select", "with"}:
        raise ConfigValidationError("only SELECT/CTE read-only queries are allowed")

    if re.search(r"\b(insert|update|delete|drop|alter|create|attach)\b", scrubbed, flags=re.I):
        raise ConfigValidationError("only read-only SELECT queries are allowed")
    return normalized


def _validate_table_name(name: str) -> str:
    cleaned = name.strip()
    if not cleaned:
        raise ConfigValidationError("table name must be non-empty")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", cleaned):
        raise ConfigValidationError(
            f"invalid table name '{name}'. only alphanumeric and '_' are allowed"
        )
    return cleaned


@dataclass(slots=True)
class DataQualityReport:
    row_count: int
    null_percentages: dict[str, float]
    score_percent: float


@dataclass(slots=True)
class DataQualityAccumulator:
    row_count: int = 0
    null_count_by_column: dict[str, int] | None = None

    def __post_init__(self) -> None:
        if self.null_count_by_column is None:
            self.null_count_by_column = {}

    def update(self, frame: pl.DataFrame) -> None:
        self.row_count += frame.height
        for name in frame.columns:
            current = self.null_count_by_column.get(name, 0)
            self.null_count_by_column[name] = current + int(frame[name].null_count())

    def finalize(self) -> DataQualityReport:
        if self.row_count <= 0:
            return DataQualityReport(row_count=0, null_percentages={}, score_percent=100.0)
        null_percentages = {
            name: (count / self.row_count) * 100.0
            for name, count in sorted(self.null_count_by_column.items())
        }
        mean_null = sum(null_percentages.values()) / max(len(null_percentages), 1)
        return DataQualityReport(
            row_count=self.row_count,
            null_percentages={k: min(max(v, 0.0), 100.0) for k, v in null_percentages.items()},
            score_percent=max(0.0, min(100.0, 100.0 - mean_null)),
        )


def build_data_quality_report(frame: pl.DataFrame) -> DataQualityReport:
    acc = DataQualityAccumulator()
    acc.update(frame)
    return acc.finalize()


__all__ = [
    "DataQualityAccumulator",
    "DataQualityReport",
    "_validate_read_query",
    "_validate_table_name",
    "build_data_quality_report",
    "normalize_source_data_contract",
    "read_parquet",
    "validate_source_data_contract",
    "write_parquet",
]
