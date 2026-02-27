#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path

import polars as pl

try:
    from honestroles import HonestRolesRuntime, build_data_quality_report
except ModuleNotFoundError as exc:
    if exc.name != "honestroles":
        raise
    repo_src = Path(__file__).resolve().parents[1] / "src"
    import sys

    sys.path.insert(0, str(repo_src))
    from honestroles import HonestRolesRuntime, build_data_quality_report


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Profile a historical jobs parquet using HonestRoles runtime and emit a JSON summary."
        )
    )
    parser.add_argument(
        "--input-parquet",
        default="jobs_historical.parquet",
        help="Path to source parquet file.",
    )
    parser.add_argument(
        "--output-json",
        default="dist/jobs_historical_profile.json",
        help="Path to output JSON summary.",
    )
    parser.add_argument(
        "--quality-profile",
        default="core_fields_weighted",
        choices=["core_fields_weighted", "equal_weight_all", "strict_recruiting"],
        help="Quality profile used by runtime report scoring.",
    )
    parser.add_argument(
        "--quality-weight",
        action="append",
        default=[],
        help="Override quality weight as FIELD=WEIGHT. Can be repeated.",
    )
    parser.add_argument(
        "--manual-map-debug",
        action="store_true",
        help=(
            "Run an additional manual mapping pass (location_raw->location, "
            "remote_flag->remote) for debug comparison."
        ),
    )
    return parser.parse_args()


def _parse_weight_overrides(raw: list[str]) -> dict[str, float]:
    weights: dict[str, float] = {}
    for item in raw:
        if "=" not in item:
            raise ValueError(f"invalid --quality-weight '{item}', expected FIELD=WEIGHT")
        field, value = item.split("=", 1)
        key = field.strip()
        if not key:
            raise ValueError("quality weight field must be non-empty")
        weights[key] = float(value.strip())
    return weights


def _build_aliases(raw_df: pl.DataFrame) -> dict[str, tuple[str, ...]]:
    aliases: dict[str, tuple[str, ...]] = {}
    if "location_raw" in raw_df.columns and "location" not in raw_df.columns:
        aliases["location"] = ("location_raw",)
    if "remote_flag" in raw_df.columns and "remote" not in raw_df.columns:
        aliases["remote"] = ("remote_flag",)
    return aliases


def _render_pipeline_text(
    input_parquet_path: Path,
    aliases: dict[str, tuple[str, ...]],
    profile: str,
    field_weights: dict[str, float],
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


def _distribution(df: pl.DataFrame, col: str, top_k: int = 10) -> list[dict[str, object]]:
    if col not in df.columns:
        return []
    grouped = (
        df.group_by(col)
        .len()
        .sort("len", descending=True)
        .head(top_k)
        .with_columns((pl.col("len") / df.height * 100.0).round(2).alias("pct"))
    )
    return grouped.to_dicts()


def _non_empty_distribution(
    df: pl.DataFrame, col: str, top_k: int = 10
) -> list[dict[str, object]]:
    if col not in df.columns:
        return []
    cleaned = df.with_columns(pl.col(col).cast(pl.String, strict=False).alias(col))
    grouped = (
        cleaned.filter(pl.col(col).is_not_null() & (pl.col(col).str.strip_chars() != ""))
        .group_by(col)
        .len()
        .sort("len", descending=True)
        .head(top_k)
    )
    return grouped.to_dicts()


def _key_completeness(df: pl.DataFrame, keys: list[str]) -> dict[str, dict[str, float]]:
    out: dict[str, dict[str, float]] = {}
    for key in keys:
        if key not in df.columns:
            out[key] = {"non_null_count": 0.0, "non_null_pct": 0.0}
            continue
        non_null = float(df.select(pl.col(key).is_not_null().sum()).item())
        out[key] = {
            "non_null_count": non_null,
            "non_null_pct": round((non_null / df.height * 100.0), 2),
        }
    return out


def _run_runtime_profile(pipeline_path: Path) -> tuple[pl.DataFrame, dict[str, object], dict[str, object]]:
    runtime = HonestRolesRuntime.from_configs(pipeline_config_path=pipeline_path)
    result = runtime.run()
    report = build_data_quality_report(
        result.dataframe,
        quality=runtime.pipeline_config.runtime.quality,
    )
    quality_payload = {
        "score_percent": round(report.score_percent, 4),
        "weighted_null_percent": round(report.weighted_null_percent, 4),
        "profile": report.profile,
        "effective_weights": report.effective_weights,
        "top_null_percentages": [
            {"column": name, "null_pct": round(value, 4)}
            for name, value in sorted(
                report.null_percentages.items(), key=lambda item: item[1], reverse=True
            )[:20]
        ],
    }
    return result.dataframe, result.diagnostics, quality_payload


def _run_manual_mapping_debug(
    raw_df: pl.DataFrame,
    profile: str,
    field_weights: dict[str, float],
) -> dict[str, object]:
    mapped = raw_df
    if "location_raw" in mapped.columns and "location" not in mapped.columns:
        mapped = mapped.with_columns(pl.col("location_raw").alias("location"))
    if "remote_flag" in mapped.columns and "remote" not in mapped.columns:
        mapped = mapped.with_columns(pl.col("remote_flag").alias("remote"))

    tmp_dir = Path(tempfile.mkdtemp(prefix="honestroles_profile_"))
    mapped_path = tmp_dir / "manual_mapped.parquet"
    pipeline_path = tmp_dir / "manual_pipeline.toml"
    mapped.write_parquet(mapped_path)
    pipeline_path.write_text(
        _render_pipeline_text(
            input_parquet_path=mapped_path,
            aliases={},
            profile=profile,
            field_weights=field_weights,
        ),
        encoding="utf-8",
    )

    df, diagnostics, quality = _run_runtime_profile(pipeline_path)
    return {
        "shape": {"rows": df.height, "columns": len(df.columns)},
        "remote_distribution": _distribution(df, "remote", top_k=10),
        "diagnostics": diagnostics,
        "quality": quality,
    }


def main() -> int:
    args = _parse_args()
    input_parquet = Path(args.input_parquet).expanduser().resolve()
    output_json = Path(args.output_json).expanduser().resolve()
    field_weights = _parse_weight_overrides(args.quality_weight)

    raw_df = pl.read_parquet(input_parquet)
    aliases = _build_aliases(raw_df)

    tmp_dir = Path(tempfile.mkdtemp(prefix="honestroles_profile_"))
    pipeline_path = tmp_dir / "pipeline.toml"
    pipeline_path.write_text(
        _render_pipeline_text(
            input_parquet_path=input_parquet,
            aliases=aliases,
            profile=args.quality_profile,
            field_weights=field_weights,
        ),
        encoding="utf-8",
    )

    final_df, diagnostics, quality_payload = _run_runtime_profile(pipeline_path)

    title_equals_company = 0
    if "title" in raw_df.columns and "company" in raw_df.columns:
        title_equals_company = raw_df.filter(
            pl.col("title").cast(pl.String, strict=False).str.strip_chars()
            == pl.col("company").cast(pl.String, strict=False).str.strip_chars()
        ).height

    posted_range: dict[str, str | None] = {"min": None, "max": None}
    if "posted_at" in final_df.columns:
        non_empty_posted = final_df.filter(
            pl.col("posted_at").is_not_null()
            & (pl.col("posted_at").cast(pl.String, strict=False).str.strip_chars() != "")
        )
        if non_empty_posted.height > 0:
            posted_stats = non_empty_posted.select(
                pl.col("posted_at").min().alias("min"),
                pl.col("posted_at").max().alias("max"),
            ).to_dicts()[0]
            posted_range = {"min": posted_stats["min"], "max": posted_stats["max"]}

    summary: dict[str, object] = {
        "input_path": str(input_parquet),
        "pipeline_path": str(pipeline_path),
        "configured_aliases": aliases,
        "raw_shape": {"rows": raw_df.height, "columns": len(raw_df.columns)},
        "runtime_shape": {"rows": final_df.height, "columns": len(final_df.columns)},
        "diagnostics": diagnostics,
        "quality": quality_payload,
        "key_completeness_raw": _key_completeness(
            raw_df,
            [
                "id",
                "title",
                "company",
                "location_raw",
                "remote_flag",
                "description_text",
                "description_html",
                "posted_at",
                "apply_url",
            ],
        ),
        "remote_distribution": _distribution(final_df, "remote", top_k=10),
        "source_distribution_raw": _distribution(raw_df, "source", top_k=10),
        "top_companies_raw": _non_empty_distribution(raw_df, "company", top_k=10),
        "top_titles_raw": _non_empty_distribution(raw_df, "title", top_k=10),
        "top_locations_runtime": _non_empty_distribution(final_df, "location", top_k=10),
        "title_equals_company": {
            "count": title_equals_company,
            "pct": round((title_equals_company / raw_df.height * 100.0), 4),
        },
        "posted_at_range_runtime": posted_range,
    }

    if args.manual_map_debug:
        summary["manual_map_debug"] = _run_manual_mapping_debug(
            raw_df,
            profile=args.quality_profile,
            field_weights=field_weights,
        )

    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    print(f"Wrote profile to {output_json}")
    print(
        json.dumps(
            {
                "rows": final_df.height,
                "quality_score_percent": quality_payload["score_percent"],
                "quality_profile": quality_payload["profile"],
                "remote_distribution": summary["remote_distribution"],
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
