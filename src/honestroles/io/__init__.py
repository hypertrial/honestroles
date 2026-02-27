from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import polars as pl

from honestroles.config.models import (
    CANONICAL_SOURCE_FIELDS,
    RuntimeQualityConfig,
)
from honestroles.errors import ConfigValidationError
from honestroles.io.adapter import (
    AdapterInferenceResult,
    apply_source_adapter,
    infer_source_adapter,
    render_adapter_toml_fragment,
)

_BUILTIN_SOURCE_ALIASES: dict[str, tuple[str, ...]] = {
    "location": ("location_raw",),
    "remote": ("remote_flag",),
}

_QUALITY_PRESET_CORE_FIELDS_WEIGHTED: dict[str, float] = {
    "title": 3.0,
    "company": 2.5,
    "description_text": 3.0,
    "apply_url": 2.5,
    "source": 1.5,
    "location": 1.5,
    "remote": 1.0,
    "posted_at": 1.5,
}

_QUALITY_PRESET_STRICT_RECRUITING: dict[str, float] = {
    "title": 2.0,
    "company": 2.0,
    "description_text": 2.0,
    "apply_url": 1.5,
    "location": 2.0,
    "remote": 1.5,
    "posted_at": 1.5,
    "salary_min": 2.0,
    "salary_max": 2.0,
    "salary_currency": 1.0,
    "salary_interval": 1.0,
    "employment_type": 1.0,
}


def read_parquet(path: str | Path) -> pl.DataFrame:
    return pl.read_parquet(path)


def write_parquet(df: pl.DataFrame, path: str | Path) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(target)


def _coerce_alias_mapping(value: object) -> dict[str, tuple[str, ...]]:
    if value is None:
        return {}
    if hasattr(value, "model_dump"):
        dumped = value.model_dump(mode="python")
        return _coerce_alias_mapping(dumped)
    if not isinstance(value, Mapping):
        raise TypeError("input aliases must be a mapping")

    alias_mapping: dict[str, tuple[str, ...]] = {}
    for canonical, aliases in value.items():
        if canonical not in CANONICAL_SOURCE_FIELDS:
            raise ConfigValidationError(f"invalid alias canonical field '{canonical}'")
        if isinstance(aliases, list):
            alias_values = tuple(aliases)
        elif isinstance(aliases, tuple):
            alias_values = aliases
        else:
            raise TypeError(f"aliases for '{canonical}' must be list/tuple")
        if any(not isinstance(item, str) for item in alias_values):
            raise TypeError(f"aliases for '{canonical}' must be strings")
        alias_mapping[str(canonical)] = alias_values
    return alias_mapping


def _ordered_candidates(canonical: str, configured_aliases: tuple[str, ...]) -> list[str]:
    order: list[str] = []
    for candidate in (canonical, *configured_aliases, *_BUILTIN_SOURCE_ALIASES.get(canonical, ())):
        if candidate not in order:
            order.append(candidate)
    return order


def _normalized_compare_expr(column: str, canonical: str) -> pl.Expr:
    if canonical == "remote":
        value = pl.col(column).cast(pl.String, strict=False).str.strip_chars().str.to_lowercase()
        return (
            pl.when(pl.col(column).is_null())
            .then(pl.lit(None, dtype=pl.String))
            .when(value.is_in(["true", "1", "yes", "y", "remote"]))
            .then(pl.lit("true"))
            .when(value.is_in(["false", "0", "no", "n", "onsite", "on-site"]))
            .then(pl.lit("false"))
            .otherwise(pl.lit("__invalid__:") + value.fill_null(""))
        )
    return (
        pl.when(pl.col(column).is_null())
        .then(pl.lit(None, dtype=pl.String))
        .otherwise(
            pl.col(column)
            .cast(pl.String, strict=False)
            .str.strip_chars()
            .str.to_lowercase()
        )
    )


def resolve_source_aliases(
    df: pl.DataFrame, aliases: object = None
) -> tuple[pl.DataFrame, dict[str, Any]]:
    alias_mapping = _coerce_alias_mapping(aliases)
    resolved = df

    applied: dict[str, str] = {}
    conflict_counts: dict[str, int] = {}

    for canonical in CANONICAL_SOURCE_FIELDS:
        configured = alias_mapping.get(canonical, ())
        candidates = _ordered_candidates(canonical, configured)
        present = [name for name in candidates if name in resolved.columns]

        if canonical not in resolved.columns:
            selected = next((name for name in present if name != canonical), None)
            if selected is not None:
                resolved = resolved.with_columns(pl.col(selected).alias(canonical))
                applied[canonical] = selected

        if canonical not in resolved.columns:
            continue

        canonical_expr = _normalized_compare_expr(canonical, canonical)
        for alias in present:
            if alias == canonical:
                continue
            alias_expr = _normalized_compare_expr(alias, canonical)
            mismatch = resolved.select(
                (
                    canonical_expr.is_not_null()
                    & alias_expr.is_not_null()
                    & (canonical_expr != alias_expr)
                )
                .sum()
                .alias("mismatch")
            ).item()
            mismatch_count = int(mismatch)
            if mismatch_count > 0:
                conflict_counts[canonical] = conflict_counts.get(canonical, 0) + mismatch_count

    unresolved = [name for name in CANONICAL_SOURCE_FIELDS if name not in resolved.columns]
    diagnostics = {
        "applied": dict(sorted(applied.items())),
        "conflicts": dict(sorted(conflict_counts.items())),
        "unresolved": unresolved,
    }
    return resolved, diagnostics


def normalize_source_data_contract(df: pl.DataFrame) -> pl.DataFrame:
    required = CANONICAL_SOURCE_FIELDS
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
    profile: str
    effective_weights: dict[str, float]
    weighted_null_percent: float


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

    def finalize(
        self,
        quality: RuntimeQualityConfig | None = None,
    ) -> DataQualityReport:
        quality_cfg = quality if quality is not None else RuntimeQualityConfig()
        if self.row_count <= 0:
            return DataQualityReport(
                row_count=0,
                null_percentages={},
                score_percent=100.0,
                profile=quality_cfg.profile,
                effective_weights={},
                weighted_null_percent=0.0,
            )
        null_percentages = {
            name: (count / self.row_count) * 100.0
            for name, count in sorted(self.null_count_by_column.items())
        }

        effective_weights = _resolve_quality_weights(
            columns=tuple(sorted(self.null_count_by_column.keys())),
            quality=quality_cfg,
        )
        total_weight = sum(effective_weights.values())
        if total_weight <= 0:
            raise ConfigValidationError("quality scoring total weight must be positive")
        weighted_null = sum(
            effective_weights[name] * null_percentages.get(name, 100.0)
            for name in effective_weights
        )
        weighted_null_percent = weighted_null / total_weight
        score_percent = max(0.0, min(100.0, 100.0 - weighted_null_percent))
        return DataQualityReport(
            row_count=self.row_count,
            null_percentages={k: min(max(v, 0.0), 100.0) for k, v in null_percentages.items()},
            score_percent=score_percent,
            profile=quality_cfg.profile,
            effective_weights=effective_weights,
            weighted_null_percent=weighted_null_percent,
        )


def _resolve_quality_weights(
    columns: tuple[str, ...], quality: RuntimeQualityConfig
) -> dict[str, float]:
    if quality.profile == "equal_weight_all":
        base = {name: 1.0 for name in columns}
    elif quality.profile == "strict_recruiting":
        base = dict(_QUALITY_PRESET_STRICT_RECRUITING)
    else:
        base = dict(_QUALITY_PRESET_CORE_FIELDS_WEIGHTED)
    base.update(quality.field_weights)
    return dict(sorted(base.items()))


def build_data_quality_report(
    frame: pl.DataFrame, quality: RuntimeQualityConfig | None = None
) -> DataQualityReport:
    acc = DataQualityAccumulator()
    acc.update(frame)
    return acc.finalize(quality=quality)


__all__ = [
    "AdapterInferenceResult",
    "DataQualityAccumulator",
    "DataQualityReport",
    "_validate_read_query",
    "_validate_table_name",
    "apply_source_adapter",
    "build_data_quality_report",
    "infer_source_adapter",
    "normalize_source_data_contract",
    "read_parquet",
    "render_adapter_toml_fragment",
    "resolve_source_aliases",
    "validate_source_data_contract",
    "write_parquet",
]
