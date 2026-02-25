from __future__ import annotations

from pathlib import Path

import pandas as pd

from honestroles.clean import clean_jobs


def test_edge_case_fixture_pack_normalization_regression() -> None:
    fixture_path = (
        Path(__file__).resolve().parents[1]
        / "fixtures"
        / "source_data_edge_cases.csv"
    )
    df = pd.read_csv(fixture_path)
    df["remote_flag"] = df["remote_flag"].astype(str).str.lower().eq("true")

    cleaned = clean_jobs(df)
    by_key = cleaned.set_index("job_key")

    assert by_key.loc["edge::greenhouse::1", "country"] == "CA"
    assert by_key.loc["edge::greenhouse::1", "region"] == "Quebec"
    assert by_key.loc["edge::ashby::3", "remote_type"] == "remote"
    assert by_key.loc["edge::ashby::3", "salary_min"] == 120000.0
    assert by_key.loc["edge::ashby::3", "salary_max"] == 140000.0
    assert by_key.loc["edge::lever::2", "salary_min"] == 70000.0
    assert by_key.loc["edge::lever::2", "salary_max"] == 90000.0
