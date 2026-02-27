from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from honestroles.eda.profile import build_eda_profile
from honestroles.errors import ConfigValidationError


def test_build_eda_profile_sections(sample_parquet: Path) -> None:
    result = build_eda_profile(
        input_parquet=sample_parquet,
        quality_profile="core_fields_weighted",
        field_weights={},
        top_k=5,
        max_rows=None,
    )

    assert {
        "shape",
        "quality",
        "completeness",
        "distributions",
        "consistency",
        "temporal",
        "diagnostics",
        "findings",
        "findings_by_source",
    } == set(result.summary.keys())

    assert "column_profile" in result.tables
    assert "null_percentages" in result.tables
    assert "numeric_quantiles" in result.tables
    assert "categorical_distribution" in result.tables
    assert result.tables["column_profile"].height > 0

    assert isinstance(result.summary["quality"]["by_source"], list)
    assert isinstance(result.summary["consistency"]["by_source"], list)


def test_build_eda_profile_detects_salary_inversions(tmp_path: Path) -> None:
    parquet = tmp_path / "jobs.parquet"
    pl.DataFrame(
        {
            "id": ["1"],
            "title": ["Engineer"],
            "company": ["A"],
            "source": ["lever"],
            "location_raw": ["Remote"],
            "remote_flag": [True],
            "description_text": ["Python"],
            "apply_url": ["https://x/1"],
            "posted_at": ["2026-01-01"],
            "salary_min": [200000.0],
            "salary_max": [100000.0],
        }
    ).write_parquet(parquet)

    result = build_eda_profile(
        input_parquet=parquet,
        quality_profile="core_fields_weighted",
        field_weights={},
        top_k=5,
        max_rows=None,
    )

    assert result.summary["consistency"]["salary_min_gt_salary_max"]["count"] == 1
    severities = [item["severity"] for item in result.summary["findings"]]
    assert "P0" in severities

    source_severities = [item["severity"] for item in result.summary["findings_by_source"]]
    assert "P0" in source_severities


def test_build_eda_profile_invalid_quality_config_raises(sample_parquet: Path) -> None:
    with pytest.raises(ConfigValidationError, match="invalid EDA quality configuration"):
        build_eda_profile(
            input_parquet=sample_parquet,
            quality_profile="core_fields_weighted",
            field_weights={"posted_at": -1.0},
            top_k=5,
            max_rows=None,
        )


def test_build_eda_profile_rejects_invalid_top_k_and_max_rows(sample_parquet: Path) -> None:
    with pytest.raises(ConfigValidationError, match="top_k"):
        build_eda_profile(
            input_parquet=sample_parquet,
            quality_profile="core_fields_weighted",
            field_weights={},
            top_k=0,
            max_rows=None,
        )
    with pytest.raises(ConfigValidationError, match="max_rows"):
        build_eda_profile(
            input_parquet=sample_parquet,
            quality_profile="core_fields_weighted",
            field_weights={},
            top_k=1,
            max_rows=0,
        )


def test_build_eda_profile_applies_max_rows(sample_parquet: Path) -> None:
    result = build_eda_profile(
        input_parquet=sample_parquet,
        quality_profile="core_fields_weighted",
        field_weights={},
        top_k=3,
        max_rows=1,
    )
    assert result.summary["shape"]["raw"]["rows"] == 1
