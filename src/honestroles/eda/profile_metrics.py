from __future__ import annotations

from typing import Any, Mapping

import polars as pl

from .common import jsonable, round4, serialize_scalar

_SENTINEL_STRINGS = {"unknown", "n/a", "na", "none", "null"}


def distribution(df: pl.DataFrame, col: str, top_k: int) -> list[dict[str, Any]]:
    if col not in df.columns or df.height == 0:
        return []
    grouped = (
        df.group_by(col)
        .len()
        .sort("len", descending=True)
        .head(top_k)
        .with_columns((pl.col("len") / float(df.height) * 100.0).round(2).alias("pct"))
    )
    return jsonable(grouped.to_dicts())


def non_empty_distribution(df: pl.DataFrame, col: str, top_k: int) -> list[dict[str, Any]]:
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
    return jsonable(grouped.to_dicts())


def build_column_profile(raw_df: pl.DataFrame) -> pl.DataFrame:
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
                "null_percent": round4(null_pct),
                "empty_count": empty_count,
                "empty_percent": round4(0.0 if rows == 0 else (empty_count / rows) * 100.0),
                "sentinel_count": sentinel_count,
                "sentinel_percent": round4(
                    0.0 if rows == 0 else (sentinel_count / rows) * 100.0
                ),
                "cardinality_estimate": int(unique_counts.get(name, 0)),
            }
        )

    return pl.DataFrame(records).sort("null_percent", descending=True)


def build_source_profile(raw_df: pl.DataFrame, runtime_df: pl.DataFrame) -> pl.DataFrame:
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


def build_quality_by_source(
    runtime_df: pl.DataFrame,
    effective_weights: Mapping[str, float],
) -> list[dict[str, Any]]:
    if "source" not in runtime_df.columns or runtime_df.height == 0:
        return []

    source_df = runtime_df.with_columns(
        pl.col("source").cast(pl.String, strict=False).fill_null("<null>").alias("source")
    )
    sources = source_df.select("source").unique().sort("source").to_series().to_list()

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
            {"field": field, "null_pct": round4(null_pct)}
            for field, null_pct in sorted(
                null_pct_by_field.items(), key=lambda item: item[1], reverse=True
            )[:3]
        ]
        rows.append(
            {
                "source": str(source),
                "rows_runtime": row_count,
                "score_proxy": round4(score_proxy),
                "weighted_null_percent_proxy": round4(weighted_null),
                "key_nulls": key_nulls,
            }
        )

    return sorted(rows, key=lambda item: (-item["rows_runtime"], item["source"]))


def build_consistency_by_source(
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
                "title_equals_company_pct": round4(
                    0.0 if rows_raw == 0 else (title_eq / rows_raw) * 100.0
                ),
            }

    runtime_by_source: dict[str, dict[str, Any]] = {}
    if "source" in runtime_df.columns:
        runtime_source = runtime_df.with_columns(
            pl.col("source").cast(pl.String, strict=False).fill_null("<null>").alias("source")
        )
        for source in runtime_source.select("source").unique().sort("source").to_series().to_list():
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
                "salary_min_gt_salary_max_pct": round4(
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


def build_numeric_quantiles_table(runtime_df: pl.DataFrame) -> pl.DataFrame:
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
                        "quantile": round4(q),
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
                    "quantile": round4(q),
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


def build_categorical_distribution_table(
    runtime_df: pl.DataFrame,
    columns: tuple[str, ...],
    top_k: int = 50,
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
                    "pct": round4((count / total_rows) * 100.0),
                }
            )
        other_count = max(0, total_rows - head_count)
        if other_count > 0:
            rows.append(
                {
                    "column": column,
                    "value": "__other__",
                    "count": other_count,
                    "pct": round4((other_count / total_rows) * 100.0),
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


def key_field_completeness(df: pl.DataFrame, fields: list[str]) -> dict[str, dict[str, float]]:
    rows = df.height
    out: dict[str, dict[str, float]] = {}
    for field in fields:
        if field not in df.columns:
            out[field] = {"non_null_count": 0.0, "non_null_pct": 0.0}
            continue
        non_null = float(df.select(pl.col(field).is_not_null().sum()).item())
        out[field] = {
            "non_null_count": non_null,
            "non_null_pct": round4(0.0 if rows == 0 else (non_null / rows) * 100.0),
        }
    return out


def build_consistency(raw_df: pl.DataFrame, runtime_df: pl.DataFrame) -> dict[str, Any]:
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
            "pct": round4(
                0.0 if raw_df.height == 0 else (title_equals_company / raw_df.height) * 100.0
            ),
        },
        "salary_min_gt_salary_max": {
            "count": salary_inversion,
            "pct": round4(
                0.0 if runtime_df.height == 0 else (salary_inversion / runtime_df.height) * 100.0
            ),
        },
    }


def build_temporal(runtime_df: pl.DataFrame) -> dict[str, Any]:
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
            "min": serialize_scalar(range_payload.get("min")),
            "max": serialize_scalar(range_payload.get("max")),
        },
        "monthly_counts": jsonable(monthly.to_dicts()),
    }


def high_sentinel_columns(column_profile: pl.DataFrame, limit: int) -> list[dict[str, Any]]:
    if column_profile.is_empty():
        return []
    subset = (
        column_profile.filter(pl.col("sentinel_percent") > 0)
        .sort("sentinel_percent", descending=True)
        .head(limit)
    )
    return jsonable(subset.to_dicts())
