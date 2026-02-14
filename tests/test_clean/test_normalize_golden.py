from pathlib import Path

import pandas as pd

from honestroles.clean.normalize import normalize_locations, normalize_salaries


def test_normalize_golden_fixture_regression() -> None:
    fixture_path = (
        Path(__file__).resolve().parents[1]
        / "fixtures"
        / "normalize_golden_input.csv"
    )
    df = pd.read_csv(fixture_path)
    df["remote_flag"] = df["remote_flag"].astype(str).str.lower().eq("true")

    normalized = normalize_locations(df)
    normalized = normalize_salaries(normalized)

    by_key = normalized.set_index("job_key")
    expected = {
        "gold-1": {
            "city": "Toronto",
            "region": "Ontario",
            "country": "CA",
            "remote_type": None,
            "salary_min": 100000.0,
            "salary_max": 120000.0,
        },
        "gold-2": {
            "city": None,
            "region": None,
            "country": "US",
            "remote_type": "remote",
            "salary_min": 80.0,
            "salary_max": 100.0,
        },
        "gold-3": {
            "city": "San Francisco",
            "region": "California",
            "country": "US",
            "remote_type": "remote",
            "salary_min": 120000.0,
            "salary_max": 150000.0,
        },
    }

    for job_key, row_expected in expected.items():
        row = by_key.loc[job_key]
        for column, expected_value in row_expected.items():
            assert row[column] == expected_value
