from __future__ import annotations

import os
from pathlib import Path
import time

import pytest

from honestroles.clean import HistoricalCleanOptions, clean_historical_jobs
from honestroles.io import read_parquet


@pytest.mark.performance
def test_clean_historical_jobs_speedup_vs_iso8601_mode() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    dataset_path = repo_root / "jobs_historical.parquet"
    if not dataset_path.exists():
        pytest.skip("jobs_historical.parquet not available in repository root")

    rows = int(os.getenv("HONESTROLES_HISTORICAL_PERF_ROWS", "100000"))
    max_seconds = float(os.getenv("HONESTROLES_MAX_HISTORICAL_CLEAN_SECONDS", "6.0"))
    min_speedup = float(os.getenv("HONESTROLES_MIN_HISTORICAL_CLEAN_SPEEDUP", "2.0"))

    raw = read_parquet(dataset_path, validate=False).head(rows)

    start = time.perf_counter()
    legacy = clean_historical_jobs(
        raw,
        options=HistoricalCleanOptions(snapshot_timestamp_output="iso8601"),
    )
    legacy_elapsed = time.perf_counter() - start

    start = time.perf_counter()
    optimized = clean_historical_jobs(raw)
    optimized_elapsed = time.perf_counter() - start

    assert len(legacy) == len(optimized)
    assert "snapshot_count" in optimized.columns
    assert "datetime64" in str(optimized["first_seen"].dtype)
    assert "datetime64" in str(optimized["last_seen"].dtype)

    meets_absolute = optimized_elapsed <= max_seconds
    meets_relative = legacy_elapsed > 0 and (legacy_elapsed / optimized_elapsed) >= min_speedup
    assert meets_absolute or meets_relative
