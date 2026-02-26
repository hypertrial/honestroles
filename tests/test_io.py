from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from honestroles.errors import ConfigValidationError
from honestroles.io import (
    _validate_read_query,
    _validate_table_name,
    build_data_quality_report,
    normalize_source_data_contract,
    read_parquet,
    validate_source_data_contract,
    write_parquet,
)


def test_parquet_roundtrip(sample_jobs_df: pl.DataFrame, tmp_path: Path) -> None:
    path = tmp_path / "roundtrip.parquet"
    write_parquet(sample_jobs_df, path)
    loaded = read_parquet(path)
    assert loaded.height == sample_jobs_df.height


def test_contract_normalize_and_validate(sample_jobs_df: pl.DataFrame) -> None:
    frame = sample_jobs_df.drop("title")
    normalized = normalize_source_data_contract(frame)
    with pytest.raises(ConfigValidationError):
        validate_source_data_contract(normalized.drop("title"))

    ok = validate_source_data_contract(normalized)
    assert ok.height == sample_jobs_df.height


def test_contract_validate_requires_description_columns(sample_jobs_df: pl.DataFrame) -> None:
    frame = sample_jobs_df.drop("description_text").drop("description_html")
    with pytest.raises(ConfigValidationError):
        validate_source_data_contract(frame)


def test_query_and_table_validators() -> None:
    assert _validate_read_query("SELECT * FROM jobs") == "SELECT * FROM jobs"
    assert (
        _validate_read_query("SELECT updated_at FROM jobs WHERE updated_at IS NOT NULL")
        == "SELECT updated_at FROM jobs WHERE updated_at IS NOT NULL"
    )
    assert (
        _validate_read_query(
            "WITH ranked AS (SELECT id FROM jobs) SELECT * FROM ranked"
        )
        == "WITH ranked AS (SELECT id FROM jobs) SELECT * FROM ranked"
    )
    assert _validate_table_name("jobs_2026") == "jobs_2026"
    with pytest.raises(ConfigValidationError):
        _validate_read_query("DROP TABLE jobs")
    with pytest.raises(ConfigValidationError):
        _validate_read_query("")
    with pytest.raises(ConfigValidationError):
        _validate_read_query("SELECT * FROM jobs;")
    with pytest.raises(ConfigValidationError):
        _validate_read_query("123")
    with pytest.raises(ConfigValidationError):
        _validate_read_query("SELECT * FROM jobs WHERE note='drop'")
    with pytest.raises(ConfigValidationError):
        _validate_table_name("jobs; DROP")
    with pytest.raises(ConfigValidationError):
        _validate_table_name("   ")


def test_quality_report_bounds(sample_jobs_df: pl.DataFrame) -> None:
    report = build_data_quality_report(sample_jobs_df)
    assert 0.0 <= report.score_percent <= 100.0
    assert report.row_count == sample_jobs_df.height
    assert all(0.0 <= v <= 100.0 for v in report.null_percentages.values())


def test_quality_report_finalize_empty_frame() -> None:
    report = build_data_quality_report(pl.DataFrame())
    assert report.row_count == 0
    assert report.score_percent == 100.0
    assert report.null_percentages == {}
