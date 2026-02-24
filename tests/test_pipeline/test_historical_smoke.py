from __future__ import annotations

from pathlib import Path

import pytest

from honestroles import (
    clean_historical_jobs,
    filter_jobs,
    label_jobs,
    rate_jobs,
    read_parquet,
)


@pytest.mark.performance
@pytest.mark.historical_smoke
def test_historical_smoke_pipeline_runs_on_local_dataset() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    dataset_path = repo_root / "jobs_historical.parquet"
    if not dataset_path.exists():
        pytest.skip("jobs_historical.parquet not available in repository root")

    raw = read_parquet(dataset_path, validate=False).head(20000)
    cleaned = clean_historical_jobs(raw)
    filtered = filter_jobs(cleaned, remote_only=False)
    labeled = label_jobs(filtered, use_llm=False)
    rated = rate_jobs(labeled, use_llm=False)

    assert "historical_is_listing_page" in cleaned.columns
    assert "snapshot_count" in cleaned.columns
    assert "rating" in rated.columns
