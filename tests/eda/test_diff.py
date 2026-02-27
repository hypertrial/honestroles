from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from honestroles.eda import build_eda_diff, generate_eda_artifacts, generate_eda_diff_artifacts
from honestroles.eda.diff import (
    _build_drift_metrics_table,
    _count_findings,
    _cdf_from_quantile_curve,
    _classify_threshold,
    _jsonable,
    _load_table,
    _normalize_distribution,
    _numeric_psi_from_quantiles,
    _pct_delta,
)
from honestroles.eda.rules import load_eda_rules
from honestroles.errors import ConfigValidationError


def test_build_eda_diff_payload_and_drift(tmp_path: Path) -> None:
    baseline_parquet = tmp_path / "baseline_jobs.parquet"
    candidate_parquet = tmp_path / "candidate_jobs.parquet"

    pl.DataFrame(
        {
            "id": ["1", "2", "3", "4"],
            "source": ["lever", "lever", "greenhouse", "greenhouse"],
            "title": ["Engineer", "Engineer", "Analyst", "Analyst"],
            "company": ["A", "A", "B", "B"],
            "location_raw": ["Remote", "NYC", "Remote", "Austin"],
            "remote_flag": [True, False, True, False],
            "description_text": ["x", "x", "y", "y"],
            "apply_url": ["u1", "u2", "u3", "u4"],
            "posted_at": ["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04"],
            "salary_min": [100000.0, 120000.0, 90000.0, 95000.0],
            "salary_max": [150000.0, 170000.0, 130000.0, 140000.0],
        }
    ).write_parquet(baseline_parquet)

    pl.DataFrame(
        {
            "id": ["1", "2", "3", "4"],
            "source": ["lever", "lever", "greenhouse", "greenhouse"],
            "title": ["Engineer", "Engineer", "Analyst", "Analyst"],
            "company": ["A", "A", "B", "B"],
            "location_raw": ["Remote", "NYC", "Unknown", "Unknown"],
            "remote_flag": [True, False, False, False],
            "description_text": ["x", "x", "y", "y"],
            "apply_url": ["u1", "u2", "u3", "u4"],
            "posted_at": ["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04"],
            "salary_min": [220000.0, 240000.0, 210000.0, 230000.0],
            "salary_max": [260000.0, 280000.0, 250000.0, 270000.0],
        }
    ).write_parquet(candidate_parquet)

    baseline_dir = tmp_path / "baseline_artifacts"
    candidate_dir = tmp_path / "candidate_artifacts"
    generate_eda_artifacts(input_parquet=baseline_parquet, output_dir=baseline_dir)
    generate_eda_artifacts(input_parquet=candidate_parquet, output_dir=candidate_dir)

    diff_payload, tables, _, _ = build_eda_diff(
        baseline_dir=baseline_dir,
        candidate_dir=candidate_dir,
        rules=load_eda_rules(),
    )

    assert "shape_diff" in diff_payload
    assert "drift" in diff_payload
    assert "gate_evaluation" in diff_payload
    assert "diff_null_percentages" in tables
    assert "drift_metrics" in tables
    assert tables["drift_metrics"].height >= 1


def test_build_eda_diff_requires_profile_artifacts(tmp_path: Path) -> None:
    baseline_parquet = tmp_path / "baseline_jobs.parquet"
    candidate_parquet = tmp_path / "candidate_jobs.parquet"
    pl.DataFrame({"id": ["1"], "title": ["x"], "company": ["y"], "description_text": ["z"]}).write_parquet(
        baseline_parquet
    )
    pl.DataFrame({"id": ["1"], "title": ["x"], "company": ["y"], "description_text": ["z"]}).write_parquet(
        candidate_parquet
    )

    baseline_profile = tmp_path / "baseline_profile"
    candidate_profile = tmp_path / "candidate_profile"
    generate_eda_artifacts(input_parquet=baseline_parquet, output_dir=baseline_profile)
    generate_eda_artifacts(input_parquet=candidate_parquet, output_dir=candidate_profile)

    diff_a = tmp_path / "diff_a"
    diff_b = tmp_path / "diff_b"
    generate_eda_diff_artifacts(
        baseline_dir=baseline_profile,
        candidate_dir=candidate_profile,
        output_dir=diff_a,
    )
    generate_eda_diff_artifacts(
        baseline_dir=baseline_profile,
        candidate_dir=candidate_profile,
        output_dir=diff_b,
    )

    with pytest.raises(ConfigValidationError, match="requires profile artifacts"):
        build_eda_diff(
            baseline_dir=diff_a,
            candidate_dir=diff_b,
            rules=load_eda_rules(),
        )


def test_diff_helper_functions_cover_edge_branches(tmp_path: Path) -> None:
    assert _pct_delta(0, 0) == 0.0
    assert _pct_delta(0, 1) == 100.0
    assert _classify_threshold(0.3, warn=0.2, fail=0.4) == "warn"
    assert _jsonable((1, 2)) == [1, 2]
    assert _normalize_distribution([0.0, 0.0]) == [0.0, 0.0]
    assert _cdf_from_quantile_curve([], 1.0) == 0.0
    assert _cdf_from_quantile_curve([(0.0, 1.0)], 0.5) == 0.0
    assert _cdf_from_quantile_curve([(0.0, 1.0), (1.0, 1.0)], 0.5) == 0.0
    assert _cdf_from_quantile_curve([(0.0, 1.0), (1.0, 1.0)], 1.0) == 1.0

    empty_quantiles = pl.DataFrame({"column": ["x"], "quantile": [0.0], "value": [1.0]})
    assert (
        _numeric_psi_from_quantiles(
            baseline_quantiles=empty_quantiles,
            candidate_quantiles=empty_quantiles,
            column="x",
        )
        is None
    )

    none_value = pl.DataFrame({"column": ["x", "x"], "quantile": [0.0, 1.0], "value": [None, 1.0]})
    assert (
        _numeric_psi_from_quantiles(
            baseline_quantiles=none_value,
            candidate_quantiles=none_value,
            column="x",
        )
        is None
    )

    class Manifest:
        files = {}

    class Bundle:
        manifest = Manifest()
        artifacts_dir = tmp_path

    assert _load_table(Bundle(), "missing").is_empty()

    baseline_ok = pl.DataFrame({"column": ["x", "x"], "quantile": [0.0, 1.0], "value": [0.0, 1.0]})
    candidate_none = pl.DataFrame({"column": ["x", "x"], "quantile": [0.0, 1.0], "value": [0.0, None]})
    assert (
        _numeric_psi_from_quantiles(
            baseline_quantiles=baseline_ok,
            candidate_quantiles=candidate_none,
            column="x",
        )
        is None
    )
    counts = _count_findings(
        {
            "findings": [{"severity": "P9"}],
            "findings_by_source": [{"severity": "P1"}, {"severity": "P9"}],
        }
    )
    assert counts["P1"] == 1


def test_build_drift_metrics_table_empty_rows_returns_schema(tmp_path: Path) -> None:
    numeric = tmp_path / "numeric_quantiles.parquet"
    categorical = tmp_path / "categorical_distribution.parquet"
    pl.DataFrame(
        schema={
            "column": pl.String,
            "quantile": pl.Float64,
            "value": pl.Float64,
            "non_null_count": pl.Int64,
        }
    ).write_parquet(numeric)
    pl.DataFrame(
        schema={"column": pl.String, "value": pl.String, "count": pl.Int64, "pct": pl.Float64}
    ).write_parquet(categorical)

    class Manifest:
        files = {
            "numeric_quantiles": "numeric_quantiles.parquet",
            "categorical_distribution": "categorical_distribution.parquet",
        }

    class Bundle:
        manifest = Manifest()
        artifacts_dir = tmp_path

    df = _build_drift_metrics_table(
        baseline_bundle=Bundle(),
        candidate_bundle=Bundle(),
        rules=load_eda_rules(),
    )
    assert df.height == 0
