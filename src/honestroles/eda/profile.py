from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Mapping

import polars as pl

from honestroles.config.models import RuntimeQualityConfig
from honestroles.errors import ConfigValidationError
from honestroles.io import build_data_quality_report
from honestroles.runtime import HonestRolesRuntime

from .models import EDAProfileResult

_SENTINEL_STRINGS = {"unknown", "n/a", "na", "none", "null"}
_SEVERITY_RANK = {"P0": 0, "P1": 1, "P2": 2}


def parse_quality_weight_overrides(items: list[str]) -> dict[str, float]:
    weights: dict[str, float] = {}
    for item in items:
        if "=" not in item:
            raise ConfigValidationError(
                f"invalid --quality-weight '{item}', expected FIELD=WEIGHT"
            )
        field, value = item.split("=", 1)
        key = field.strip()
        if not key:
            raise ConfigValidationError("quality weight field must be non-empty")
        try:
            weight = float(value.strip())
        except ValueError as exc:
            raise ConfigValidationError(
                f"invalid quality weight for '{key}': '{value}'"
            ) from exc
        if weight < 0:
            raise ConfigValidationError(
                f"quality weight for '{key}' must be >= 0"
            )
        weights[key] = weight
    if weights and sum(weights.values()) <= 0:
        raise ConfigValidationError(
            "quality weights must include at least one positive value"
        )
    return weights


def build_eda_profile(
    *,
    input_parquet: Path,
    quality_profile: str,
    field_weights: Mapping[str, float],
    top_k: int,
    max_rows: int | None,
) -> EDAProfileResult:
    if top_k < 1:
        raise ConfigValidationError("top_k must be >= 1")
    if max_rows is not None and max_rows < 1:
        raise ConfigValidationError("max_rows must be >= 1")

    try:
        quality_cfg = RuntimeQualityConfig(
            profile=quality_profile, field_weights=dict(field_weights)
        )
    except Exception as exc:  # pydantic validation error
        raise ConfigValidationError(f"invalid EDA quality configuration: {exc}") from exc

    raw_df = pl.read_parquet(input_parquet)
    if max_rows is not None:
        raw_df = raw_df.head(max_rows)

    aliases = _build_aliases(raw_df)
    runtime_df, diagnostics, quality_payload = _run_runtime_profile(
        input_df=raw_df,
        input_parquet=input_parquet,
        aliases=aliases,
        quality_profile=quality_cfg.profile,
        field_weights=quality_cfg.field_weights,
    )

    column_profile = _build_column_profile(raw_df)
    source_profile = _build_source_profile(raw_df, runtime_df)
    quality_by_source = _build_quality_by_source(
        runtime_df=runtime_df,
        effective_weights=quality_payload["effective_weights"],
    )

    key_fields_runtime = _key_field_completeness(
        runtime_df,
        [
            "id",
            "title",
            "company",
            "location",
            "remote",
            "description_text",
            "posted_at",
            "apply_url",
        ],
    )

    quality_top_null = [
        {"column": name, "null_pct": _round4(value)}
        for name, value in sorted(
            quality_payload["null_percentages"].items(),
            key=lambda item: item[1],
            reverse=True,
        )[:20]
    ]

    consistency = _build_consistency(raw_df, runtime_df)
    temporal = _build_temporal(runtime_df)
    distributions = {
        "source_raw": _distribution(raw_df, "source", top_k=top_k),
        "remote_runtime": _distribution(runtime_df, "remote", top_k=top_k),
        "top_companies_raw": _non_empty_distribution(raw_df, "company", top_k=top_k),
        "top_titles_raw": _non_empty_distribution(raw_df, "title", top_k=top_k),
        "top_locations_runtime": _non_empty_distribution(
            runtime_df, "location", top_k=top_k
        ),
    }

    completeness = {
        "key_fields_runtime": key_fields_runtime,
        "by_source": source_profile.to_dicts(),
        "high_sentinel_columns": _high_sentinel_columns(column_profile, limit=10),
    }

    shape = {
        "input_path": str(input_parquet),
        "raw": {"rows": raw_df.height, "columns": len(raw_df.columns)},
        "runtime": {"rows": runtime_df.height, "columns": len(runtime_df.columns)},
    }

    quality_summary = {
        "row_count": quality_payload["row_count"],
        "score_percent": _round4(quality_payload["score_percent"]),
        "weighted_null_percent": _round4(quality_payload["weighted_null_percent"]),
        "profile": quality_payload["profile"],
        "effective_weights": quality_payload["effective_weights"],
        "top_null_percentages": quality_top_null,
        "by_source": quality_by_source,
    }

    consistency_by_source = _build_consistency_by_source(raw_df=raw_df, runtime_df=runtime_df)

    summary: dict[str, Any] = {
        "shape": shape,
        "quality": quality_summary,
        "completeness": completeness,
        "distributions": distributions,
        "consistency": {**consistency, "by_source": consistency_by_source},
        "temporal": temporal,
        "diagnostics": diagnostics,
        "findings": [],
        "findings_by_source": [],
    }
    summary["findings"] = _build_findings(summary)
    summary["findings_by_source"] = _build_source_findings(summary)

    tables = {
        "null_percentages": pl.DataFrame(
            {
                "column": list(quality_payload["null_percentages"].keys()),
                "null_percent": list(quality_payload["null_percentages"].values()),
            }
        ).sort("null_percent", descending=True),
        "column_profile": column_profile,
        "source_profile": source_profile,
        "top_values_source": _distribution_table(
            distributions["source_raw"], value_column="source"
        ),
        "top_values_company": _distribution_table(
            distributions["top_companies_raw"], value_column="company"
        ),
        "top_values_title": _distribution_table(
            distributions["top_titles_raw"], value_column="title"
        ),
        "top_values_location": _distribution_table(
            distributions["top_locations_runtime"], value_column="location"
        ),
        "numeric_quantiles": _build_numeric_quantiles_table(runtime_df=runtime_df),
        "categorical_distribution": _build_categorical_distribution_table(
            runtime_df=runtime_df, columns=("source", "remote", "location", "company")
        ),
    }

    return EDAProfileResult(summary=_jsonable(summary), tables=tables)


def _run_runtime_profile(
    *,
    input_df: pl.DataFrame,
    input_parquet: Path,
    aliases: dict[str, tuple[str, ...]],
    quality_profile: str,
    field_weights: Mapping[str, float],
) -> tuple[pl.DataFrame, dict[str, Any], dict[str, Any]]:
    with TemporaryDirectory(prefix="honestroles_eda_") as tmp_dir:
        tmp_path = Path(tmp_dir)
        runtime_input_path = tmp_path / "input.parquet"
        pipeline_path = tmp_path / "pipeline.toml"

        input_df.write_parquet(runtime_input_path)
        pipeline_path.write_text(
            _render_pipeline_text(
                input_parquet_path=runtime_input_path,
                aliases=aliases,
                profile=quality_profile,
                field_weights=field_weights,
            ),
            encoding="utf-8",
        )

        runtime = HonestRolesRuntime.from_configs(pipeline_config_path=pipeline_path)
        result = runtime.run()
        report = build_data_quality_report(
            result.dataframe,
            quality=runtime.pipeline_config.runtime.quality,
        )

    quality_payload = {
        "row_count": report.row_count,
        "score_percent": report.score_percent,
        "weighted_null_percent": report.weighted_null_percent,
        "profile": report.profile,
        "effective_weights": report.effective_weights,
        "null_percentages": report.null_percentages,
    }
    return result.dataframe, result.diagnostics, quality_payload


def _render_pipeline_text(
    *,
    input_parquet_path: Path,
    aliases: dict[str, tuple[str, ...]],
    profile: str,
    field_weights: Mapping[str, float],
) -> str:
    lines = [
        "[input]",
        'kind = "parquet"',
        f'path = "{input_parquet_path}"',
        "",
    ]

    if aliases:
        lines.append("[input.aliases]")
        for canonical in sorted(aliases):
            values = ", ".join(f'"{alias}"' for alias in aliases[canonical])
            lines.append(f"{canonical} = [{values}]")
        lines.append("")

    lines.extend(
        [
            "[stages.clean]",
            "enabled = true",
            "",
            "[stages.filter]",
            "enabled = false",
            "",
            "[stages.label]",
            "enabled = true",
            "",
            "[stages.rate]",
            "enabled = true",
            "",
            "[stages.match]",
            "enabled = false",
            "",
            "[runtime]",
            "fail_fast = true",
            "random_seed = 0",
            "",
            "[runtime.quality]",
            f'profile = "{profile}"',
            "",
        ]
    )

    if field_weights:
        lines.append("[runtime.quality.field_weights]")
        for field in sorted(field_weights):
            lines.append(f"{field} = {field_weights[field]}")
        lines.append("")

    return "\n".join(lines).strip() + "\n"


def _build_aliases(raw_df: pl.DataFrame) -> dict[str, tuple[str, ...]]:
    aliases: dict[str, tuple[str, ...]] = {}
    if "location_raw" in raw_df.columns and "location" not in raw_df.columns:
        aliases["location"] = ("location_raw",)
    if "remote_flag" in raw_df.columns and "remote" not in raw_df.columns:
        aliases["remote"] = ("remote_flag",)
    return aliases


def _distribution(df: pl.DataFrame, col: str, top_k: int) -> list[dict[str, Any]]:
    if col not in df.columns or df.height == 0:
        return []
    grouped = (
        df.group_by(col)
        .len()
        .sort("len", descending=True)
        .head(top_k)
        .with_columns((pl.col("len") / float(df.height) * 100.0).round(2).alias("pct"))
    )
    return _jsonable(grouped.to_dicts())


def _non_empty_distribution(df: pl.DataFrame, col: str, top_k: int) -> list[dict[str, Any]]:
    if col not in df.columns or df.height == 0:
        return []
    cleaned = df.with_columns(pl.col(col).cast(pl.String, strict=False).alias(col))
    grouped = (
        cleaned.filter(pl.col(col).is_not_null() & (pl.col(col).str.strip_chars() != ""))
        .group_by(col)
        .len()
        .sort("len", descending=True)
        .head(top_k)
        .with_columns((pl.col("len") / float(df.height) * 100.0).round(2).alias("pct"))
    )
    return _jsonable(grouped.to_dicts())


def _build_column_profile(raw_df: pl.DataFrame) -> pl.DataFrame:
    rows = raw_df.height
    null_counts = raw_df.null_count().to_dicts()[0] if rows > 0 else {}
    unique_counts = (
        raw_df.select([pl.col(name).approx_n_unique().alias(name) for name in raw_df.columns])
        .to_dicts()[0]
        if rows > 0 and raw_df.columns
        else {}
    )

    records: list[dict[str, Any]] = []
    for name, dtype in raw_df.schema.items():
        null_count = int(null_counts.get(name, 0))
        null_pct = 0.0 if rows == 0 else (null_count / rows) * 100.0

        empty_count = 0
        sentinel_count = 0
        if dtype == pl.String and rows > 0:
            normalized = pl.col(name).cast(pl.String, strict=False).str.strip_chars()
            empty_count = int(
                raw_df.select(
                    (pl.col(name).is_not_null() & (normalized == "")).sum().alias("count")
                ).item()
            )
            sentinel_count = int(
                raw_df.select(
                    (
                        pl.col(name).is_not_null()
                        & normalized.str.to_lowercase().is_in(_SENTINEL_STRINGS)
                    )
                    .sum()
                    .alias("count")
                ).item()
            )

        records.append(
            {
                "column": name,
                "dtype": str(dtype),
                "null_count": null_count,
                "null_percent": _round4(null_pct),
                "empty_count": empty_count,
                "empty_percent": _round4(0.0 if rows == 0 else (empty_count / rows) * 100.0),
                "sentinel_count": sentinel_count,
                "sentinel_percent": _round4(
                    0.0 if rows == 0 else (sentinel_count / rows) * 100.0
                ),
                "cardinality_estimate": int(unique_counts.get(name, 0)),
            }
        )

    return pl.DataFrame(records).sort("null_percent", descending=True)


def _build_source_profile(raw_df: pl.DataFrame, runtime_df: pl.DataFrame) -> pl.DataFrame:
    schema = {
        "source": pl.String,
        "rows_raw": pl.Int64,
        "rows_runtime": pl.Int64,
        "posted_at_non_null_pct_raw": pl.Float64,
        "remote_true_pct_runtime": pl.Float64,
    }
    if "source" not in raw_df.columns:
        return pl.DataFrame(schema=schema)

    raw_source = raw_df.with_columns(
        pl.col("source").cast(pl.String, strict=False).fill_null("<null>").alias("source")
    )
    source_raw = raw_source.group_by("source").len().rename({"len": "rows_raw"})

    if "posted_at" in raw_df.columns:
        source_raw = source_raw.join(
            raw_source.group_by("source")
            .agg(
                (
                    pl.col("posted_at").is_not_null().sum() / pl.len() * 100.0
                ).alias("posted_at_non_null_pct_raw")
            )
            .with_columns(pl.col("posted_at_non_null_pct_raw").round(4)),
            on="source",
            how="left",
        )
    else:
        source_raw = source_raw.with_columns(
            pl.lit(0.0).cast(pl.Float64).alias("posted_at_non_null_pct_raw")
        )

    if "source" in runtime_df.columns:
        runtime_source = runtime_df.with_columns(
            pl.col("source")
            .cast(pl.String, strict=False)
            .fill_null("<null>")
            .alias("source")
        )
        source_runtime = runtime_source.group_by("source").len().rename(
            {"len": "rows_runtime"}
        )
        if "remote" in runtime_df.columns:
            remote_share = runtime_source.group_by("source").agg(
                (
                    pl.col("remote").cast(pl.Boolean, strict=False).fill_null(False).mean()
                    * 100.0
                ).alias("remote_true_pct_runtime")
            )
            source_runtime = source_runtime.join(remote_share, on="source", how="left")
        else:
            source_runtime = source_runtime.with_columns(
                pl.lit(0.0).cast(pl.Float64).alias("remote_true_pct_runtime")
            )
    else:
        source_runtime = pl.DataFrame(
            {
                "source": source_raw["source"],
                "rows_runtime": [0 for _ in range(source_raw.height)],
                "remote_true_pct_runtime": [0.0 for _ in range(source_raw.height)],
            }
        )

    return (
        source_raw.join(source_runtime, on="source", how="left")
        .with_columns(
            pl.col("rows_runtime").fill_null(0).cast(pl.Int64),
            pl.col("remote_true_pct_runtime").fill_null(0.0).round(4),
        )
        .select(list(schema.keys()))
        .sort("rows_raw", descending=True)
    )


def _build_quality_by_source(
    runtime_df: pl.DataFrame, effective_weights: Mapping[str, float]
) -> list[dict[str, Any]]:
    if "source" not in runtime_df.columns or runtime_df.height == 0:
        return []

    source_df = runtime_df.with_columns(
        pl.col("source").cast(pl.String, strict=False).fill_null("<null>").alias("source")
    )
    sources = (
        source_df.select("source")
        .unique()
        .sort("source")
        .to_series()
        .to_list()
    )

    weights = dict(sorted((str(k), float(v)) for k, v in effective_weights.items()))
    total_weight = sum(weights.values()) if weights else 0.0

    rows: list[dict[str, Any]] = []
    for source in sources:
        subset = source_df.filter(pl.col("source") == source)
        row_count = subset.height
        if row_count == 0:
            continue

        null_pct_by_field: dict[str, float] = {}
        for field, weight in weights.items():
            if weight <= 0:
                continue
            if field not in subset.columns:
                null_pct_by_field[field] = 100.0
                continue
            null_count = int(subset.select(pl.col(field).is_null().sum()).item())
            null_pct_by_field[field] = (null_count / row_count) * 100.0

        weighted_null = 0.0
        if total_weight > 0:
            weighted_null = sum(
                weights[field] * null_pct_by_field.get(field, 100.0) for field in weights
            ) / total_weight
        score_proxy = max(0.0, min(100.0, 100.0 - weighted_null))

        key_nulls = [
            {"field": field, "null_pct": _round4(null_pct)}
            for field, null_pct in sorted(
                null_pct_by_field.items(), key=lambda item: item[1], reverse=True
            )[:3]
        ]
        rows.append(
            {
                "source": str(source),
                "rows_runtime": row_count,
                "score_proxy": _round4(score_proxy),
                "weighted_null_percent_proxy": _round4(weighted_null),
                "key_nulls": key_nulls,
            }
        )

    return sorted(rows, key=lambda item: (-item["rows_runtime"], item["source"]))


def _build_consistency_by_source(
    raw_df: pl.DataFrame,
    runtime_df: pl.DataFrame,
) -> list[dict[str, Any]]:
    if "source" not in raw_df.columns and "source" not in runtime_df.columns:
        return []

    raw_by_source: dict[str, dict[str, Any]] = {}
    if "source" in raw_df.columns:
        raw_source = raw_df.with_columns(
            pl.col("source").cast(pl.String, strict=False).fill_null("<null>").alias("source")
        )
        for source in raw_source.select("source").unique().sort("source").to_series().to_list():
            subset = raw_source.filter(pl.col("source") == source)
            rows_raw = subset.height
            title_eq = 0
            if "title" in subset.columns and "company" in subset.columns:
                title_eq = subset.filter(
                    pl.col("title").cast(pl.String, strict=False).str.strip_chars()
                    == pl.col("company").cast(pl.String, strict=False).str.strip_chars()
                ).height
            raw_by_source[str(source)] = {
                "rows_raw": rows_raw,
                "title_equals_company_count": title_eq,
                "title_equals_company_pct": _round4(
                    0.0 if rows_raw == 0 else (title_eq / rows_raw) * 100.0
                ),
            }

    runtime_by_source: dict[str, dict[str, Any]] = {}
    if "source" in runtime_df.columns:
        runtime_source = runtime_df.with_columns(
            pl.col("source").cast(pl.String, strict=False).fill_null("<null>").alias("source")
        )
        for source in (
            runtime_source.select("source").unique().sort("source").to_series().to_list()
        ):
            subset = runtime_source.filter(pl.col("source") == source)
            rows_runtime = subset.height
            inversion = 0
            if "salary_min" in subset.columns and "salary_max" in subset.columns:
                inversion = subset.filter(
                    pl.col("salary_min").is_not_null()
                    & pl.col("salary_max").is_not_null()
                    & (pl.col("salary_min") > pl.col("salary_max"))
                ).height
            runtime_by_source[str(source)] = {
                "rows_runtime": rows_runtime,
                "salary_min_gt_salary_max_count": inversion,
                "salary_min_gt_salary_max_pct": _round4(
                    0.0 if rows_runtime == 0 else (inversion / rows_runtime) * 100.0
                ),
            }

    all_sources = sorted(set(raw_by_source.keys()) | set(runtime_by_source.keys()))
    rows: list[dict[str, Any]] = []
    for source in all_sources:
        rows.append(
            {
                "source": source,
                "rows_raw": int(raw_by_source.get(source, {}).get("rows_raw", 0)),
                "rows_runtime": int(runtime_by_source.get(source, {}).get("rows_runtime", 0)),
                "title_equals_company_count": int(
                    raw_by_source.get(source, {}).get("title_equals_company_count", 0)
                ),
                "title_equals_company_pct": float(
                    raw_by_source.get(source, {}).get("title_equals_company_pct", 0.0)
                ),
                "salary_min_gt_salary_max_count": int(
                    runtime_by_source.get(source, {}).get("salary_min_gt_salary_max_count", 0)
                ),
                "salary_min_gt_salary_max_pct": float(
                    runtime_by_source.get(source, {}).get("salary_min_gt_salary_max_pct", 0.0)
                ),
            }
        )
    return rows


def _build_numeric_quantiles_table(runtime_df: pl.DataFrame) -> pl.DataFrame:
    quantiles = [i / 10.0 for i in range(11)]
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

    rows: list[dict[str, Any]] = []
    for column, dtype in runtime_df.schema.items():
        if dtype not in numeric_types:
            continue
        non_null = int(runtime_df.select(pl.col(column).is_not_null().sum()).item())
        if non_null == 0:
            for q in quantiles:
                rows.append(
                    {
                        "column": column,
                        "quantile": _round4(q),
                        "value": None,
                        "non_null_count": non_null,
                    }
                )
            continue
        for q in quantiles:
            value = runtime_df.select(
                pl.col(column).cast(pl.Float64, strict=False).quantile(q).alias("v")
            ).item()
            rows.append(
                {
                    "column": column,
                    "quantile": _round4(q),
                    "value": None if value is None else float(value),
                    "non_null_count": non_null,
                }
            )

    if not rows:
        return pl.DataFrame(
            schema={
                "column": pl.String,
                "quantile": pl.Float64,
                "value": pl.Float64,
                "non_null_count": pl.Int64,
            }
        )
    return pl.DataFrame(rows).sort(["column", "quantile"])


def _build_categorical_distribution_table(
    runtime_df: pl.DataFrame, columns: tuple[str, ...], top_k: int = 50
) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    if runtime_df.height == 0:
        return pl.DataFrame(
            schema={
                "column": pl.String,
                "value": pl.String,
                "count": pl.Int64,
                "pct": pl.Float64,
            }
        )

    total_rows = runtime_df.height
    for column in columns:
        if column not in runtime_df.columns:
            continue
        grouped = (
            runtime_df.with_columns(
                pl.col(column)
                .cast(pl.String, strict=False)
                .fill_null("<null>")
                .alias(column)
            )
            .group_by(column)
            .len()
            .sort("len", descending=True)
        )
        head = grouped.head(top_k)
        head_rows = head.to_dicts()
        head_count = sum(int(item["len"]) for item in head_rows)
        for item in head_rows:
            count = int(item["len"])
            rows.append(
                {
                    "column": column,
                    "value": str(item[column]),
                    "count": count,
                    "pct": _round4((count / total_rows) * 100.0),
                }
            )
        other_count = max(0, total_rows - head_count)
        if other_count > 0:
            rows.append(
                {
                    "column": column,
                    "value": "__other__",
                    "count": other_count,
                    "pct": _round4((other_count / total_rows) * 100.0),
                }
            )

    if not rows:
        return pl.DataFrame(
            schema={
                "column": pl.String,
                "value": pl.String,
                "count": pl.Int64,
                "pct": pl.Float64,
            }
        )
    return pl.DataFrame(rows).sort(["column", "count"], descending=[False, True])


def _key_field_completeness(df: pl.DataFrame, fields: list[str]) -> dict[str, dict[str, float]]:
    rows = df.height
    out: dict[str, dict[str, float]] = {}
    for field in fields:
        if field not in df.columns:
            out[field] = {"non_null_count": 0.0, "non_null_pct": 0.0}
            continue
        non_null = float(df.select(pl.col(field).is_not_null().sum()).item())
        out[field] = {
            "non_null_count": non_null,
            "non_null_pct": _round4(0.0 if rows == 0 else (non_null / rows) * 100.0),
        }
    return out


def _build_consistency(raw_df: pl.DataFrame, runtime_df: pl.DataFrame) -> dict[str, Any]:
    title_equals_company = 0
    if "title" in raw_df.columns and "company" in raw_df.columns:
        title_equals_company = raw_df.filter(
            pl.col("title").cast(pl.String, strict=False).str.strip_chars()
            == pl.col("company").cast(pl.String, strict=False).str.strip_chars()
        ).height

    salary_inversion = 0
    if "salary_min" in runtime_df.columns and "salary_max" in runtime_df.columns:
        salary_inversion = runtime_df.filter(
            pl.col("salary_min").is_not_null()
            & pl.col("salary_max").is_not_null()
            & (pl.col("salary_min") > pl.col("salary_max"))
        ).height

    return {
        "title_equals_company": {
            "count": title_equals_company,
            "pct": _round4(
                0.0 if raw_df.height == 0 else (title_equals_company / raw_df.height) * 100.0
            ),
        },
        "salary_min_gt_salary_max": {
            "count": salary_inversion,
            "pct": _round4(
                0.0 if runtime_df.height == 0 else (salary_inversion / runtime_df.height) * 100.0
            ),
        },
    }


def _build_temporal(runtime_df: pl.DataFrame) -> dict[str, Any]:
    if "posted_at" not in runtime_df.columns or runtime_df.height == 0:
        return {"posted_at_range": {"min": None, "max": None}, "monthly_counts": []}

    parsed = runtime_df.with_columns(
        pl.col("posted_at")
        .cast(pl.String, strict=False)
        .str.strptime(pl.Datetime, strict=False)
        .alias("_posted_at_dt")
    )

    range_payload = parsed.select(
        pl.col("_posted_at_dt").min().alias("min"),
        pl.col("_posted_at_dt").max().alias("max"),
    ).to_dicts()[0]

    monthly = (
        parsed.filter(pl.col("_posted_at_dt").is_not_null())
        .group_by(pl.col("_posted_at_dt").dt.strftime("%Y-%m").alias("month"))
        .len()
        .rename({"len": "count"})
        .sort("month")
    )

    return {
        "posted_at_range": {
            "min": _serialize_scalar(range_payload.get("min")),
            "max": _serialize_scalar(range_payload.get("max")),
        },
        "monthly_counts": _jsonable(monthly.to_dicts()),
    }


def _high_sentinel_columns(column_profile: pl.DataFrame, limit: int) -> list[dict[str, Any]]:
    if column_profile.is_empty():
        return []
    subset = (
        column_profile.filter(pl.col("sentinel_percent") > 0)
        .sort("sentinel_percent", descending=True)
        .head(limit)
    )
    return _jsonable(subset.to_dicts())


def _build_findings(summary: Mapping[str, Any]) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []

    score_percent = float(summary["quality"]["score_percent"])
    if score_percent < 90.0:
        findings.append(
            {
                "severity": "P1",
                "title": "Low weighted quality score",
                "detail": f"score_percent={_round4(score_percent)} is below 90.",
                "recommendation": "Prioritize high-weight fields with high null rates.",
            }
        )
    elif score_percent < 95.0:
        findings.append(
            {
                "severity": "P2",
                "title": "Quality score below target",
                "detail": f"score_percent={_round4(score_percent)} is below 95.",
                "recommendation": "Review top weighted-null columns and adjust extraction.",
            }
        )

    salary_inversion_count = int(summary["consistency"]["salary_min_gt_salary_max"]["count"])
    if salary_inversion_count > 0:
        findings.append(
            {
                "severity": "P0",
                "title": "Invalid salary ranges detected",
                "detail": f"{salary_inversion_count} rows have salary_min > salary_max.",
                "recommendation": "Normalize salary parsing and enforce min<=max before scoring.",
            }
        )

    title_eq_pct = float(summary["consistency"]["title_equals_company"]["pct"])
    if title_eq_pct >= 5.0:
        findings.append(
            {
                "severity": "P1",
                "title": "Title appears to contain company names",
                "detail": f"title_equals_company={_round4(title_eq_pct)}%.",
                "recommendation": "Add source-specific title cleanup or fallback title extraction.",
            }
        )

    posted_at_pct = float(
        summary["completeness"]["key_fields_runtime"].get("posted_at", {}).get(
            "non_null_pct", 0.0
        )
    )
    if posted_at_pct < 80.0:
        findings.append(
            {
                "severity": "P1",
                "title": "Low posted_at coverage",
                "detail": f"posted_at non-null coverage is {_round4(posted_at_pct)}%.",
                "recommendation": "Improve date extraction or add alias/transform for source fields.",
            }
        )

    unknown_location_pct = 0.0
    for item in summary["distributions"].get("top_locations_runtime", []):
        if str(item.get("location", "")).strip().lower() == "unknown":
            unknown_location_pct = float(item.get("pct", 0.0))
            break
    if unknown_location_pct >= 30.0:
        findings.append(
            {
                "severity": "P1",
                "title": "Large unknown location share",
                "detail": f"unknown location rows account for {_round4(unknown_location_pct)}%.",
                "recommendation": "Expand location extraction and source alias mapping coverage.",
            }
        )

    return sorted(
        findings,
        key=lambda item: (_SEVERITY_RANK.get(item["severity"], 99), item["title"]),
    )


def _build_source_findings(summary: Mapping[str, Any]) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    quality_rows = {
        str(item["source"]): item for item in summary.get("quality", {}).get("by_source", [])
    }
    consistency_rows = {
        str(item["source"]): item
        for item in summary.get("consistency", {}).get("by_source", [])
    }

    all_sources = sorted(set(quality_rows.keys()) | set(consistency_rows.keys()))
    for source in all_sources:
        quality_row = quality_rows.get(source, {})
        consistency_row = consistency_rows.get(source, {})
        score = float(quality_row.get("score_proxy", 100.0))
        if score < 90.0:
            findings.append(
                {
                    "severity": "P1",
                    "source": source,
                    "metric": "quality.score_proxy",
                    "title": "Low source quality score",
                    "detail": f"source={source} score_proxy={_round4(score)} below 90.",
                    "recommendation": "Prioritize missing high-weight fields for this source.",
                }
            )
        elif score < 95.0:
            findings.append(
                {
                    "severity": "P2",
                    "source": source,
                    "metric": "quality.score_proxy",
                    "title": "Source quality below target",
                    "detail": f"source={source} score_proxy={_round4(score)} below 95.",
                    "recommendation": "Review weighted nulls and extraction quality for this source.",
                }
            )

        salary_pct = float(consistency_row.get("salary_min_gt_salary_max_pct", 0.0))
        salary_count = int(consistency_row.get("salary_min_gt_salary_max_count", 0))
        if salary_count > 0:
            findings.append(
                {
                    "severity": "P0",
                    "source": source,
                    "metric": "consistency.salary_min_gt_salary_max",
                    "title": "Invalid salary ranges in source",
                    "detail": (
                        f"source={source} has {salary_count} rows with salary_min > salary_max "
                        f"({salary_pct}%)."
                    ),
                    "recommendation": "Fix salary parsing and normalization for this source.",
                }
            )

        title_pct = float(consistency_row.get("title_equals_company_pct", 0.0))
        if title_pct >= 5.0:
            findings.append(
                {
                    "severity": "P1",
                    "source": source,
                    "metric": "consistency.title_equals_company",
                    "title": "Title/company contamination in source",
                    "detail": (
                        f"source={source} has title_equals_company={_round4(title_pct)}%."
                    ),
                    "recommendation": "Apply source-specific title cleanup rules.",
                }
            )

    return sorted(
        findings,
        key=lambda item: (
            _SEVERITY_RANK.get(item["severity"], 99),
            str(item.get("source", "")),
            item["title"],
        ),
    )


def _serialize_scalar(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_jsonable(v) for v in value]
    if isinstance(value, tuple):
        return [_jsonable(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _round4(value: float) -> float:
    return round(float(value), 4)


def _distribution_table(rows: list[dict[str, Any]], value_column: str) -> pl.DataFrame:
    if rows:
        return pl.DataFrame(rows)
    return pl.DataFrame(
        schema={
            value_column: pl.String,
            "len": pl.Int64,
            "pct": pl.Float64,
        }
    )
