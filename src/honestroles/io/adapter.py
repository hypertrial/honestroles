from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import re
from typing import Any, Mapping

import polars as pl

from honestroles.config.models import (
    AdapterCastType,
    CANONICAL_SOURCE_FIELDS,
    InputAdapterConfig,
    InputAdapterFieldConfig,
)
from honestroles.errors import ConfigValidationError

_DEFAULT_NULL_LIKE: tuple[str, ...] = ("", "null", "none", "n/a", "na")
_DEFAULT_TRUE_VALUES: tuple[str, ...] = ("true", "1", "yes", "y", "remote")
_DEFAULT_FALSE_VALUES: tuple[str, ...] = (
    "false",
    "0",
    "no",
    "n",
    "onsite",
    "on-site",
)
_DEFAULT_DATETIME_FORMATS: tuple[str, ...] = (
    "%Y-%m-%d",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S%z",
)
_MAX_ERROR_SAMPLES = 20


@dataclass(frozen=True, slots=True)
class AdapterInferenceResult:
    adapter_config: InputAdapterConfig
    toml_fragment: str
    report: dict[str, Any]
    field_suggestions: int


def _coerce_adapter_config(value: object) -> InputAdapterConfig:
    if value is None:
        return InputAdapterConfig(enabled=False)
    if isinstance(value, InputAdapterConfig):
        return value
    if hasattr(value, "model_dump"):
        dumped = value.model_dump(mode="python")
        return InputAdapterConfig.model_validate(dumped)
    if isinstance(value, Mapping):
        return InputAdapterConfig.model_validate(value)
    raise TypeError("input.adapter must be a mapping")


def _normalized_token_set(value: str) -> set[str]:
    return {part for part in re.split(r"[^a-z0-9]+", value.lower()) if part}


def _effective_null_like(cfg: InputAdapterFieldConfig) -> tuple[str, ...]:
    return cfg.null_like if cfg.null_like else _DEFAULT_NULL_LIKE


def _effective_true_values(cfg: InputAdapterFieldConfig) -> tuple[str, ...]:
    return cfg.true_values if cfg.true_values else _DEFAULT_TRUE_VALUES


def _effective_false_values(cfg: InputAdapterFieldConfig) -> tuple[str, ...]:
    return cfg.false_values if cfg.false_values else _DEFAULT_FALSE_VALUES


def _effective_datetime_formats(cfg: InputAdapterFieldConfig) -> tuple[str, ...]:
    return cfg.datetime_formats if cfg.datetime_formats else _DEFAULT_DATETIME_FORMATS


def _build_clean_expr(column: str, cfg: InputAdapterFieldConfig) -> pl.Expr:
    expr = pl.col(column).cast(pl.String, strict=False)
    if cfg.trim:
        expr = expr.str.strip_chars()
    return expr


def _build_coercion_plan(
    *, column: str, cfg: InputAdapterFieldConfig
) -> tuple[pl.Expr, pl.Expr, pl.Expr, pl.Expr, str]:
    clean_expr = _build_clean_expr(column, cfg)
    null_like = tuple(value.lower() for value in _effective_null_like(cfg))
    null_like_expr = clean_expr.is_null() | clean_expr.str.to_lowercase().is_in(null_like)

    if cfg.cast == "string":
        parsed_expr = pl.when(null_like_expr).then(pl.lit(None, dtype=pl.String)).otherwise(clean_expr)
        parse_error_expr = pl.lit(False)
        reason = "string_parse_failed"
    elif cfg.cast == "bool":
        true_values = tuple(v.lower() for v in _effective_true_values(cfg))
        false_values = tuple(v.lower() for v in _effective_false_values(cfg))
        lowered = clean_expr.str.to_lowercase()
        is_true = lowered.is_in(true_values)
        is_false = lowered.is_in(false_values)
        parsed_expr = (
            pl.when(null_like_expr)
            .then(pl.lit(None, dtype=pl.Boolean))
            .when(is_true)
            .then(pl.lit(True))
            .when(is_false)
            .then(pl.lit(False))
            .otherwise(pl.lit(None, dtype=pl.Boolean))
        )
        parse_error_expr = (~null_like_expr) & clean_expr.is_not_null() & (~is_true) & (~is_false)
        reason = "bool_parse_failed"
    elif cfg.cast == "float":
        numeric = clean_expr.str.replace_all(",", "").cast(pl.Float64, strict=False)
        parsed_expr = pl.when(null_like_expr).then(pl.lit(None, dtype=pl.Float64)).otherwise(numeric)
        parse_error_expr = (~null_like_expr) & clean_expr.is_not_null() & numeric.is_null()
        reason = "float_parse_failed"
    elif cfg.cast == "int":
        numeric = clean_expr.str.replace_all(",", "").cast(pl.Int64, strict=False)
        parsed_expr = pl.when(null_like_expr).then(pl.lit(None, dtype=pl.Int64)).otherwise(numeric)
        parse_error_expr = (~null_like_expr) & clean_expr.is_not_null() & numeric.is_null()
        reason = "int_parse_failed"
    else:
        parsed_variants: list[pl.Expr] = [
            clean_expr.str.strptime(pl.Datetime, format=fmt, strict=False)
            for fmt in _effective_datetime_formats(cfg)
        ]
        parsed_dt = pl.coalesce(parsed_variants)
        parsed_expr = (
            pl.when(null_like_expr)
            .then(pl.lit(None, dtype=pl.String))
            .otherwise(parsed_dt.cast(pl.String))
        )
        parse_error_expr = (~null_like_expr) & clean_expr.is_not_null() & parsed_dt.is_null()
        reason = "date_parse_failed"

    return parsed_expr, clean_expr, null_like_expr, parse_error_expr, reason


def apply_source_adapter(
    df: pl.DataFrame, adapter_cfg: object = None
) -> tuple[pl.DataFrame, dict[str, Any]]:
    cfg = _coerce_adapter_config(adapter_cfg)
    diagnostics: dict[str, Any] = {
        "enabled": cfg.enabled,
        "applied": {},
        "conflicts": {},
        "coercion_errors": {},
        "null_like_hits": {},
        "unresolved": [],
        "error_samples": [],
    }
    if not cfg.enabled or not cfg.fields:
        return df, diagnostics

    result = df
    error_budget = _MAX_ERROR_SAMPLES

    for canonical in CANONICAL_SOURCE_FIELDS:
        if canonical not in cfg.fields:
            continue
        field_cfg = cfg.fields[canonical]
        present_sources = [name for name in field_cfg.from_ if name in result.columns]
        if not present_sources:
            if canonical not in result.columns:
                diagnostics["unresolved"].append(canonical)
            continue

        selected_source = present_sources[0]
        parsed_expr, clean_expr, null_like_expr, parse_error_expr, reason = _build_coercion_plan(
            column=selected_source,
            cfg=field_cfg,
        )

        counts = result.select(
            null_like_expr.sum().alias("null_like_hits"),
            parse_error_expr.sum().alias("coercion_errors"),
        ).to_dicts()[0]
        null_like_hits = int(counts["null_like_hits"])
        coercion_errors = int(counts["coercion_errors"])

        if null_like_hits > 0:
            diagnostics["null_like_hits"][canonical] = null_like_hits
        if coercion_errors > 0:
            diagnostics["coercion_errors"][canonical] = coercion_errors

        if error_budget > 0 and coercion_errors > 0:
            samples = (
                result.filter(parse_error_expr)
                .select(clean_expr.alias("value"))
                .head(error_budget)
                .to_series()
                .to_list()
            )
            for value in samples:
                diagnostics["error_samples"].append(
                    {
                        "field": canonical,
                        "source": selected_source,
                        "value": None if value is None else str(value),
                        "reason": reason,
                    }
                )
            error_budget = max(0, error_budget - len(samples))

        if canonical not in result.columns:
            result = result.with_columns(parsed_expr.alias(canonical))
            diagnostics["applied"][canonical] = selected_source
            continue

        canonical_expr, _, _, _, _ = _build_coercion_plan(column=canonical, cfg=field_cfg)
        conflict_count = int(
            result.select(
                (
                    canonical_expr.is_not_null()
                    & parsed_expr.is_not_null()
                    & (canonical_expr != parsed_expr)
                )
                .sum()
                .alias("conflicts")
            ).item()
        )
        if conflict_count > 0:
            diagnostics["conflicts"][canonical] = conflict_count

    diagnostics["applied"] = dict(sorted(diagnostics["applied"].items()))
    diagnostics["conflicts"] = dict(sorted(diagnostics["conflicts"].items()))
    diagnostics["coercion_errors"] = dict(sorted(diagnostics["coercion_errors"].items()))
    diagnostics["null_like_hits"] = dict(sorted(diagnostics["null_like_hits"].items()))
    diagnostics["unresolved"] = sorted(set(diagnostics["unresolved"]))
    return result, diagnostics


def _expected_cast(canonical: str) -> AdapterCastType:
    if canonical == "remote":
        return "bool"
    if canonical in {"salary_min", "salary_max"}:
        return "float"
    if canonical == "posted_at":
        return "date_string"
    return "string"


def _name_score(canonical: str, column: str) -> float:
    if canonical == column:
        return 1.0

    synonyms: dict[str, set[str]] = {
        "location": {"location", "location_raw", "job_location", "city", "geo_location"},
        "remote": {"remote", "remote_flag", "is_remote", "work_from_home", "wfh"},
        "description_text": {"description", "description_text", "job_description", "details"},
        "description_html": {"description_html", "job_html", "html_description"},
        "apply_url": {"apply_url", "application_url", "url", "job_url"},
        "posted_at": {"posted_at", "date_posted", "published_at", "posted_date", "created_at"},
        "salary_min": {"salary_min", "min_salary", "comp_min", "salary_from"},
        "salary_max": {"salary_max", "max_salary", "comp_max", "salary_to"},
    }

    canonical_tokens = _normalized_token_set(canonical)
    column_tokens = _normalized_token_set(column)
    if not canonical_tokens or not column_tokens:
        return 0.0

    if canonical in synonyms and column.lower() in synonyms[canonical]:
        return 0.95

    overlap = len(canonical_tokens & column_tokens)
    if overlap == 0:
        return 0.0
    denom = max(len(canonical_tokens), len(column_tokens))
    return min(0.9, overlap / denom)


def _type_score(cast: AdapterCastType, dtype: pl.DataType) -> float:
    numeric_types = {
        pl.Int8,
        pl.Int16,
        pl.Int32,
        pl.Int64,
        pl.UInt8,
        pl.UInt16,
        pl.UInt32,
        pl.UInt64,
        pl.Float32,
        pl.Float64,
    }
    if cast == "bool":
        if dtype == pl.Boolean:
            return 1.0
        if dtype in numeric_types or dtype == pl.String:
            return 0.7
        return 0.2
    if cast in {"float", "int"}:
        if dtype in numeric_types:
            return 1.0
        if dtype == pl.String:
            return 0.6
        return 0.2
    if cast == "date_string":
        if dtype in {pl.Date, pl.Datetime}:
            return 1.0
        if dtype == pl.String:
            return 0.8
        return 0.3
    if dtype == pl.String:
        return 1.0
    return 0.6


def _parse_score(sample: pl.DataFrame, column: str, cast: AdapterCastType) -> float:
    payload: dict[str, Any] = {"from": [column], "cast": cast}
    if cast == "bool":
        payload["true_values"] = list(_DEFAULT_TRUE_VALUES)
        payload["false_values"] = list(_DEFAULT_FALSE_VALUES)
    if cast == "date_string":
        payload["datetime_formats"] = list(_DEFAULT_DATETIME_FORMATS)
    tmp_cfg = InputAdapterFieldConfig.model_validate(payload)
    parsed_expr, clean_expr, null_like_expr, parse_error_expr, _ = _build_coercion_plan(
        column=column,
        cfg=tmp_cfg,
    )

    counts = sample.select(
        clean_expr.is_not_null().sum().alias("non_null"),
        null_like_expr.sum().alias("null_like"),
        parse_error_expr.sum().alias("parse_errors"),
        parsed_expr.is_not_null().sum().alias("parsed_non_null"),
    ).to_dicts()[0]

    candidate_count = int(counts["non_null"]) - int(counts["null_like"])
    if candidate_count <= 0:
        return 0.5
    parsed_count = int(counts["parsed_non_null"])
    successful = max(0, min(candidate_count, parsed_count))
    return max(0.0, min(1.0, successful / candidate_count))


def infer_source_adapter(
    df: pl.DataFrame,
    *,
    sample_rows: int = 50_000,
    top_candidates: int = 3,
    min_confidence: float = 0.55,
) -> AdapterInferenceResult:
    if sample_rows < 1:
        raise ConfigValidationError("sample_rows must be >= 1")
    if top_candidates < 1:
        raise ConfigValidationError("top_candidates must be >= 1")
    if not (0.0 <= min_confidence <= 1.0):
        raise ConfigValidationError("min_confidence must be between 0 and 1")

    sample = df.head(sample_rows)
    field_rankings: dict[str, list[dict[str, Any]]] = {}
    inferred_fields: dict[str, InputAdapterFieldConfig] = {}
    unresolved: list[str] = []

    for canonical in CANONICAL_SOURCE_FIELDS:
        cast = _expected_cast(canonical)
        candidates: list[dict[str, Any]] = []

        for column in sample.columns:
            if column == canonical:
                continue
            name_score = _name_score(canonical, column)
            dtype = sample.schema[column]
            type_score = _type_score(cast, dtype)
            parse_score = _parse_score(sample, column, cast)
            score = (0.5 * name_score) + (0.2 * type_score) + (0.3 * parse_score)
            candidates.append(
                {
                    "candidate_column": column,
                    "score": round(float(score), 4),
                    "name_score": round(float(name_score), 4),
                    "type_score": round(float(type_score), 4),
                    "parse_score": round(float(parse_score), 4),
                    "selected": False,
                }
            )

        ranked = sorted(candidates, key=lambda item: (-item["score"], item["candidate_column"]))
        selected = [
            item for item in ranked if item["score"] >= min_confidence
        ][:top_candidates]
        selected_columns = [item["candidate_column"] for item in selected]

        for item in ranked:
            if item["candidate_column"] in selected_columns:
                item["selected"] = True

        field_rankings[canonical] = ranked[: max(10, top_candidates)]
        if selected_columns:
            payload: dict[str, Any] = {"from": selected_columns, "cast": cast}
            if cast == "bool":
                payload["true_values"] = list(_DEFAULT_TRUE_VALUES)
                payload["false_values"] = list(_DEFAULT_FALSE_VALUES)
            if cast == "date_string":
                payload["datetime_formats"] = list(_DEFAULT_DATETIME_FORMATS)
            inferred_fields[canonical] = InputAdapterFieldConfig.model_validate(payload)
        else:
            unresolved.append(canonical)

    adapter_config = InputAdapterConfig(enabled=True, on_error="null_warn", fields=inferred_fields)
    report: dict[str, Any] = {
        "schema_version": "1.0",
        "inference_version": "1.0",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "sample_strategy": "head",
        "sample_rows": sample.height,
        "requested_sample_rows": sample_rows,
        "top_candidates": top_candidates,
        "min_confidence": min_confidence,
        "fields": field_rankings,
        "unresolved": sorted(unresolved),
    }
    return AdapterInferenceResult(
        adapter_config=adapter_config,
        toml_fragment=render_adapter_toml_fragment(adapter_config),
        report=report,
        field_suggestions=len(inferred_fields),
    )


def render_adapter_toml_fragment(adapter_config: object) -> str:
    cfg = _coerce_adapter_config(adapter_config)
    lines = [
        "[input.adapter]",
        f"enabled = {str(cfg.enabled).lower()}",
        f'on_error = "{cfg.on_error}"',
        "",
    ]

    for canonical in CANONICAL_SOURCE_FIELDS:
        if canonical not in cfg.fields:
            continue
        field = cfg.fields[canonical]
        lines.append(f"[input.adapter.fields.{canonical}]")
        from_values = ", ".join(f'"{name}"' for name in field.from_)
        lines.append(f"from = [{from_values}]")
        lines.append(f'cast = "{field.cast}"')
        if field.cast == "bool":
            true_values = field.true_values if field.true_values else _DEFAULT_TRUE_VALUES
            false_values = field.false_values if field.false_values else _DEFAULT_FALSE_VALUES
            lines.append(
                "true_values = ["
                + ", ".join(f'\"{value}\"' for value in true_values)
                + "]"
            )
            lines.append(
                "false_values = ["
                + ", ".join(f'\"{value}\"' for value in false_values)
                + "]"
            )
        if field.cast == "date_string":
            formats = (
                field.datetime_formats if field.datetime_formats else _DEFAULT_DATETIME_FORMATS
            )
            lines.append(
                "datetime_formats = ["
                + ", ".join(f'\"{value}\"' for value in formats)
                + "]"
            )
        lines.append("")

    return "\n".join(lines).strip() + "\n"


__all__ = [
    "AdapterInferenceResult",
    "apply_source_adapter",
    "infer_source_adapter",
    "render_adapter_toml_fragment",
]
