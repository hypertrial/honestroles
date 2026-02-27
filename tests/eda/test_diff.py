from __future__ import annotations

from pathlib import Path

import polars as pl

from honestroles.eda import build_eda_diff, generate_eda_artifacts
from honestroles.eda.rules import load_eda_rules


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
