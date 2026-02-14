from __future__ import annotations

import os
import time

import pandas as pd
import pytest

from honestroles.clean import clean_jobs
from honestroles.filter import filter_jobs
from honestroles.rate import rate_jobs


def _perf_df(rows: int = 4000) -> pd.DataFrame:
    data = {
        "job_key": [f"acme::greenhouse::{i}" for i in range(rows)],
        "company": ["Acme"] * rows,
        "source": ["greenhouse"] * rows,
        "job_id": [str(i) for i in range(rows)],
        "title": ["Senior Software Engineer"] * rows,
        "location_raw": ["Remote, US"] * rows,
        "apply_url": [f"https://example.com/apply/{i}" for i in range(rows)],
        "ingested_at": ["2025-01-01T00:00:00Z"] * rows,
        "content_hash": [f"hash{i}" for i in range(rows)],
        "description_text": [
            "Build distributed systems with Python and SQL.\n- Design APIs\n- Operate services"
        ]
        * rows,
        "salary_text": ["$120000 - $150000"] * rows,
        "skills": [["Python", "SQL"]] * rows,
        "remote_flag": [True] * rows,
    }
    return pd.DataFrame(data)


@pytest.mark.performance
def test_clean_jobs_performance_guardrail() -> None:
    df = _perf_df()
    threshold = float(os.getenv("HONESTROLES_MAX_CLEAN_SECONDS", "6.0"))

    start = time.perf_counter()
    cleaned = clean_jobs(df)
    elapsed = time.perf_counter() - start

    assert len(cleaned) == len(df)
    assert elapsed <= threshold


@pytest.mark.performance
def test_filter_jobs_performance_guardrail() -> None:
    df = _perf_df()
    threshold = float(os.getenv("HONESTROLES_MAX_FILTER_SECONDS", "2.5"))

    start = time.perf_counter()
    filtered = filter_jobs(
        df,
        remote_only=True,
        min_salary=120000,
        include_keywords=["distributed"],
    )
    elapsed = time.perf_counter() - start

    assert len(filtered) == len(df)
    assert elapsed <= threshold


@pytest.mark.performance
def test_rate_jobs_performance_guardrail() -> None:
    df = _perf_df()
    threshold = float(os.getenv("HONESTROLES_MAX_RATE_SECONDS", "2.5"))

    start = time.perf_counter()
    rated = rate_jobs(df, use_llm=False)
    elapsed = time.perf_counter() - start

    assert len(rated) == len(df)
    assert "rating" in rated.columns
    assert elapsed <= threshold
