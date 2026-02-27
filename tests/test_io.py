from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from honestroles.config import RuntimeQualityConfig
from honestroles.errors import ConfigValidationError
from honestroles.io import (
    DataQualityAccumulator,
    _validate_read_query,
    _validate_table_name,
    build_data_quality_report,
    normalize_source_data_contract,
    read_parquet,
    resolve_source_aliases,
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
    report = build_data_quality_report(
        sample_jobs_df, quality=RuntimeQualityConfig(profile="equal_weight_all")
    )
    assert 0.0 <= report.score_percent <= 100.0
    assert report.row_count == sample_jobs_df.height
    assert all(0.0 <= v <= 100.0 for v in report.null_percentages.values())
    assert report.profile == "equal_weight_all"
    assert report.weighted_null_percent >= 0.0


def test_quality_report_finalize_empty_frame() -> None:
    report = build_data_quality_report(pl.DataFrame())
    assert report.row_count == 0
    assert report.score_percent == 100.0
    assert report.null_percentages == {}
    assert report.profile == "core_fields_weighted"
    assert report.effective_weights == {}
    assert report.weighted_null_percent == 0.0


def test_resolve_source_aliases_applies_alias_when_canonical_missing() -> None:
    df = pl.DataFrame(
        {
            "id": ["1"],
            "title": ["Engineer"],
            "company": ["Acme"],
            "location_raw": ["Remote"],
            "remote_flag": [True],
            "description_text": ["x"],
            "description_html": [None],
            "apply_url": ["https://x/1"],
            "posted_at": ["2026-01-01"],
        }
    )
    resolved, diagnostics = resolve_source_aliases(
        df,
        {"location": ("location_raw",), "remote": ("remote_flag",)},
    )
    assert resolved["location"].to_list() == ["Remote"]
    assert resolved["remote"].to_list() == [True]
    assert diagnostics["applied"] == {"location": "location_raw", "remote": "remote_flag"}
    assert "location" not in diagnostics["unresolved"]
    assert "remote" not in diagnostics["unresolved"]


def test_resolve_source_aliases_keeps_canonical_and_reports_conflicts() -> None:
    df = pl.DataFrame(
        {
            "remote": [True, False, True],
            "remote_flag": [True, True, False],
        }
    )
    resolved, diagnostics = resolve_source_aliases(df, {"remote": ("remote_flag",)})
    assert resolved["remote"].to_list() == [True, False, True]
    assert diagnostics["applied"] == {}
    assert diagnostics["conflicts"]["remote"] == 2


def test_resolve_source_aliases_counts_invalid_remote_alias_as_conflict() -> None:
    df = pl.DataFrame(
        {
            "remote": [True, False],
            "remote_flag": ["MAYBE", "0"],
        }
    )
    _, diagnostics = resolve_source_aliases(df, {"remote": ("remote_flag",)})
    assert diagnostics["conflicts"]["remote"] == 1


def test_resolve_source_aliases_deterministic_multi_alias_selection() -> None:
    df = pl.DataFrame({"loc_b": ["B"], "loc_a": ["A"]})
    resolved, diagnostics = resolve_source_aliases(
        df,
        {"location": ("loc_b", "loc_a")},
    )
    assert resolved["location"].to_list() == ["B"]
    assert diagnostics["applied"]["location"] == "loc_b"


def test_quality_report_weighted_formula_equal_weight_profile() -> None:
    frame = pl.DataFrame({"a": [1, None], "b": [None, None]})
    report = build_data_quality_report(
        frame,
        quality=RuntimeQualityConfig(profile="equal_weight_all"),
    )
    assert report.null_percentages["a"] == 50.0
    assert report.null_percentages["b"] == 100.0
    assert report.weighted_null_percent == 75.0
    assert report.score_percent == 25.0


def test_quality_report_missing_weighted_column_treated_as_fully_null() -> None:
    frame = pl.DataFrame({"a": [1, None], "b": [None, None]})
    report = build_data_quality_report(
        frame,
        quality=RuntimeQualityConfig(
            profile="equal_weight_all",
            field_weights={"missing_col": 2.0},
        ),
    )
    assert report.effective_weights["missing_col"] == 2.0
    assert report.weighted_null_percent == 87.5
    assert report.score_percent == 12.5


def test_resolve_source_aliases_validation_errors() -> None:
    frame = pl.DataFrame({"title": ["x"]})
    resolved, diagnostics = resolve_source_aliases(frame, aliases=None)
    assert resolved.height == 1
    assert diagnostics["applied"] == {}
    with pytest.raises(TypeError):
        resolve_source_aliases(frame, aliases=1)
    with pytest.raises(ConfigValidationError):
        resolve_source_aliases(frame, aliases={"bad": ("x",)})
    with pytest.raises(TypeError):
        resolve_source_aliases(frame, aliases={"title": "x"})
    with pytest.raises(TypeError):
        resolve_source_aliases(frame, aliases={"title": (1,)})
    resolved2, _ = resolve_source_aliases(frame, aliases={"title": ["alias_title"]})
    assert resolved2.height == 1


def test_quality_report_strict_profile_and_zero_total_weight_error() -> None:
    frame = pl.DataFrame({"title": ["x"], "description_text": ["y"]})
    strict = build_data_quality_report(
        frame,
        quality=RuntimeQualityConfig(profile="strict_recruiting"),
    )
    assert strict.profile == "strict_recruiting"

    no_columns = pl.DataFrame([{}])
    acc = DataQualityAccumulator(row_count=0, null_count_by_column={})
    acc.update(no_columns)
    with pytest.raises(ConfigValidationError, match="total weight must be positive"):
        acc.finalize(quality=RuntimeQualityConfig(profile="equal_weight_all"))
