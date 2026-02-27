from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

import polars as pl

from honestroles.errors import ConfigValidationError

from .common import distribution_table, jsonable, round4
from .models import EDAProfileResult
from .profile_findings import build_findings, build_source_findings
from .profile_metrics import (
    build_categorical_distribution_table,
    build_column_profile,
    build_consistency,
    build_consistency_by_source,
    build_numeric_quantiles_table,
    build_quality_by_source,
    build_source_profile,
    build_temporal,
    distribution,
    high_sentinel_columns,
    key_field_completeness,
    non_empty_distribution,
)
from .profile_runtime import (
    build_aliases,
    parse_quality_weight_overrides,
    run_runtime_profile,
    validate_quality_config,
)

__all__ = ["build_eda_profile", "parse_quality_weight_overrides"]


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

    quality_cfg = validate_quality_config(
        quality_profile=quality_profile,
        field_weights=field_weights,
    )

    raw_df = pl.read_parquet(input_parquet)
    if max_rows is not None:
        raw_df = raw_df.head(max_rows)

    aliases = build_aliases(raw_df)
    runtime_df, diagnostics, quality_payload = run_runtime_profile(
        input_df=raw_df,
        input_parquet=input_parquet,
        aliases=aliases,
        quality_profile=quality_cfg.profile,
        field_weights=quality_cfg.field_weights,
    )

    column_profile = build_column_profile(raw_df)
    source_profile = build_source_profile(raw_df, runtime_df)
    quality_by_source = build_quality_by_source(
        runtime_df=runtime_df,
        effective_weights=quality_payload["effective_weights"],
    )

    key_fields_runtime = key_field_completeness(
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
        {"column": name, "null_pct": round4(value)}
        for name, value in sorted(
            quality_payload["null_percentages"].items(),
            key=lambda item: item[1],
            reverse=True,
        )[:20]
    ]

    consistency = build_consistency(raw_df, runtime_df)
    temporal = build_temporal(runtime_df)
    distributions = {
        "source_raw": distribution(raw_df, "source", top_k=top_k),
        "remote_runtime": distribution(runtime_df, "remote", top_k=top_k),
        "top_companies_raw": non_empty_distribution(raw_df, "company", top_k=top_k),
        "top_titles_raw": non_empty_distribution(raw_df, "title", top_k=top_k),
        "top_locations_runtime": non_empty_distribution(
            runtime_df,
            "location",
            top_k=top_k,
        ),
    }

    completeness = {
        "key_fields_runtime": key_fields_runtime,
        "by_source": source_profile.to_dicts(),
        "high_sentinel_columns": high_sentinel_columns(column_profile, limit=10),
    }

    shape = {
        "input_path": str(input_parquet),
        "raw": {"rows": raw_df.height, "columns": len(raw_df.columns)},
        "runtime": {"rows": runtime_df.height, "columns": len(runtime_df.columns)},
    }

    quality_summary = {
        "row_count": quality_payload["row_count"],
        "score_percent": round4(quality_payload["score_percent"]),
        "weighted_null_percent": round4(quality_payload["weighted_null_percent"]),
        "profile": quality_payload["profile"],
        "effective_weights": quality_payload["effective_weights"],
        "top_null_percentages": quality_top_null,
        "by_source": quality_by_source,
    }

    consistency_by_source = build_consistency_by_source(
        raw_df=raw_df,
        runtime_df=runtime_df,
    )

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
    summary["findings"] = build_findings(summary)
    summary["findings_by_source"] = build_source_findings(summary)

    tables = {
        "null_percentages": pl.DataFrame(
            {
                "column": list(quality_payload["null_percentages"].keys()),
                "null_percent": list(quality_payload["null_percentages"].values()),
            }
        ).sort("null_percent", descending=True),
        "column_profile": column_profile,
        "source_profile": source_profile,
        "top_values_source": distribution_table(
            distributions["source_raw"],
            value_column="source",
        ),
        "top_values_company": distribution_table(
            distributions["top_companies_raw"],
            value_column="company",
        ),
        "top_values_title": distribution_table(
            distributions["top_titles_raw"],
            value_column="title",
        ),
        "top_values_location": distribution_table(
            distributions["top_locations_runtime"],
            value_column="location",
        ),
        "numeric_quantiles": build_numeric_quantiles_table(runtime_df=runtime_df),
        "categorical_distribution": build_categorical_distribution_table(
            runtime_df=runtime_df,
            columns=("source", "remote", "location", "company"),
        ),
    }

    return EDAProfileResult(summary=jsonable(summary), tables=tables)
