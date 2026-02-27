from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Mapping

import polars as pl

from honestroles.config.models import RuntimeQualityConfig
from honestroles.errors import ConfigValidationError
from honestroles.io import build_data_quality_report
from honestroles.runtime import HonestRolesRuntime


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


def validate_quality_config(
    *,
    quality_profile: str,
    field_weights: Mapping[str, float],
) -> RuntimeQualityConfig:
    try:
        return RuntimeQualityConfig(
            profile=quality_profile,
            field_weights=dict(field_weights),
        )
    except Exception as exc:  # pydantic validation error
        raise ConfigValidationError(f"invalid EDA quality configuration: {exc}") from exc


def build_aliases(raw_df: pl.DataFrame) -> dict[str, tuple[str, ...]]:
    aliases: dict[str, tuple[str, ...]] = {}
    if "location_raw" in raw_df.columns and "location" not in raw_df.columns:
        aliases["location"] = ("location_raw",)
    if "remote_flag" in raw_df.columns and "remote" not in raw_df.columns:
        aliases["remote"] = ("remote_flag",)
    return aliases


def run_runtime_profile(
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
            render_pipeline_text(
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


def render_pipeline_text(
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
