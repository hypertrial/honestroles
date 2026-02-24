from __future__ import annotations

import os
import time
from urllib.parse import urlparse

import pandas as pd
import pytest

from honestroles.io import validate_source_data_contract


def _perf_contract_df(rows: int) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "job_key": [f"acme::greenhouse::{i}" for i in range(rows)],
            "company": ["Acme"] * rows,
            "source": ["greenhouse"] * rows,
            "job_id": [str(i) for i in range(rows)],
            "title": ["Software Engineer"] * rows,
            "location_raw": ["Remote, US"] * rows,
            "apply_url": [f"https://example.com/apply/{i}" for i in range(rows)],
            "ingested_at": ["2025-01-01T00:00:00Z"] * rows,
            "content_hash": [f"hash{i}" for i in range(rows)],
            "posted_at": ["2025-01-01T00:00:00Z"] * rows,
            "skills": [["Python", "SQL"]] * rows,
            "languages": [["English"]] * rows,
            "benefits": [["401k"]] * rows,
            "keywords": [["backend"]] * rows,
            "remote_flag": [True] * rows,
            "visa_sponsorship": [False] * rows,
            "salary_currency": ["USD"] * rows,
            "salary_interval": ["year"] * rows,
            "salary_min": [100000.0] * rows,
            "salary_max": [120000.0] * rows,
        }
    )


def _is_missing_value(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, (list, tuple, set, dict)):
        return False
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _legacy_validate_format_only(df: pd.DataFrame) -> None:
    timestamp_columns = (
        "ingested_at",
        "posted_at",
        "updated_at",
        "last_seen",
        "application_deadline",
    )
    array_columns = ("skills", "languages", "benefits", "keywords")
    bool_columns = ("remote_flag", "visa_sponsorship")

    violations: list[str] = []
    for column in timestamp_columns:
        if column not in df.columns:
            continue
        for index, value in enumerate(df[column].tolist()):
            if _is_missing_value(value):
                continue
            parsed = pd.to_datetime(value, errors="coerce", utc=True)
            if pd.isna(parsed):
                violations.append(f"{column}[{index}] invalid timestamp")

    if "apply_url" in df.columns:
        for index, value in enumerate(df["apply_url"].tolist()):
            if _is_missing_value(value):
                continue
            if not isinstance(value, str):
                violations.append(f"apply_url[{index}] must be a URL string")
                continue
            parsed = urlparse(value.strip())
            if parsed.scheme not in {"http", "https"} or not parsed.netloc:
                violations.append(f"apply_url[{index}] invalid URL")

    for column in array_columns:
        if column not in df.columns:
            continue
        for index, value in enumerate(df[column].tolist()):
            if _is_missing_value(value):
                continue
            if not isinstance(value, (list, tuple, set)):
                violations.append(f"{column}[{index}] must be an array of strings")
                continue
            if any(not isinstance(item, str) for item in value):
                violations.append(f"{column}[{index}] contains non-string values")

    for column in bool_columns:
        if column not in df.columns:
            continue
        for index, value in enumerate(df[column].tolist()):
            if _is_missing_value(value):
                continue
            if not isinstance(value, bool):
                violations.append(f"{column}[{index}] must be boolean")

    if "salary_currency" in df.columns:
        for index, value in enumerate(df["salary_currency"].tolist()):
            if _is_missing_value(value):
                continue
            if not isinstance(value, str) or len(value.strip()) != 3 or not value.strip().isupper():
                violations.append(
                    f"salary_currency[{index}] must be a 3-letter uppercase currency code"
                )

    if "salary_interval" in df.columns:
        for index, value in enumerate(df["salary_interval"].tolist()):
            if _is_missing_value(value):
                continue
            if not isinstance(value, str) or value.strip().lower() not in {
                "hour",
                "day",
                "week",
                "month",
                "year",
            }:
                violations.append("salary_interval[{index}] must be one of ['day', 'hour', 'month', 'week', 'year']")

    if "salary_min" in df.columns and "salary_max" in df.columns:
        for index, (minimum, maximum) in enumerate(zip(df["salary_min"].tolist(), df["salary_max"].tolist())):
            if _is_missing_value(minimum) or _is_missing_value(maximum):
                continue
            try:
                min_value = float(minimum)
                max_value = float(maximum)
            except (TypeError, ValueError):
                violations.append(f"salary_min/salary_max[{index}] must be numeric")
                continue
            if min_value > max_value:
                violations.append(f"salary_min/salary_max[{index}] has min greater than max")

    if violations:
        raise ValueError("legacy-format-validation-failed")


@pytest.mark.performance
def test_validate_source_data_contract_50k_runtime_guardrail() -> None:
    rows = int(os.getenv("HONESTROLES_CONTRACT_PERF_ROWS", "50000"))
    threshold = float(os.getenv("HONESTROLES_MAX_CONTRACT_VALIDATE_SECONDS", "35.0"))
    df = _perf_contract_df(rows)

    start = time.perf_counter()
    validate_source_data_contract(df, require_non_null=True, enforce_formats=True)
    elapsed = time.perf_counter() - start
    assert elapsed <= threshold


@pytest.mark.performance
def test_validate_source_data_contract_speedup_vs_legacy() -> None:
    rows = int(os.getenv("HONESTROLES_CONTRACT_SPEEDUP_ROWS", "12000"))
    min_speedup = float(os.getenv("HONESTROLES_MIN_CONTRACT_SPEEDUP", "3.0"))
    df = _perf_contract_df(rows)

    start = time.perf_counter()
    _legacy_validate_format_only(df)
    legacy_elapsed = time.perf_counter() - start

    start = time.perf_counter()
    validate_source_data_contract(df, require_non_null=True, enforce_formats=True)
    new_elapsed = time.perf_counter() - start

    assert legacy_elapsed > 0
    assert (legacy_elapsed / new_elapsed) >= min_speedup
