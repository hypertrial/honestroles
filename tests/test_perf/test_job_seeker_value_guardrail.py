from __future__ import annotations

import os
from pathlib import Path
import time

import pandas as pd
import pytest

from honestroles.clean import clean_historical_jobs
from honestroles.io import read_parquet
from honestroles.label import label_jobs
from honestroles.match import build_application_plan, extract_job_signals, rank_jobs
from honestroles.schema import SALARY_MAX, SALARY_MIN, SKILLS


@pytest.mark.performance
def test_job_seeker_value_guardrail_on_historical_playground() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    dataset_path = repo_root / "jobs_historical.parquet"
    if not dataset_path.exists():
        pytest.skip("jobs_historical.parquet not available in repository root")

    rows = int(os.getenv("HONESTROLES_VALUE_GUARD_ROWS", "100000"))
    signal_rows = int(os.getenv("HONESTROLES_VALUE_SIGNAL_ROWS", "20000"))
    max_clean_seconds = float(os.getenv("HONESTROLES_MAX_VALUE_CLEAN_SECONDS", "20.0"))
    max_signal_seconds = float(os.getenv("HONESTROLES_MAX_VALUE_SIGNAL_SECONDS", "6.0"))

    min_skills_pct = float(os.getenv("HONESTROLES_MIN_VALUE_SKILLS_PCT", "10.0"))
    min_salary_pct = float(os.getenv("HONESTROLES_MIN_VALUE_SALARY_PCT", "8.0"))
    min_salary_annual_pct = float(os.getenv("HONESTROLES_MIN_VALUE_SALARY_ANNUAL_PCT", "6.0"))
    min_req_skills_pct = float(os.getenv("HONESTROLES_MIN_VALUE_REQUIRED_SKILLS_PCT", "55.0"))
    min_exp_pct = float(os.getenv("HONESTROLES_MIN_VALUE_EXPERIENCE_PCT", "60.0"))
    min_auth_or_visa_pct = float(os.getenv("HONESTROLES_MIN_VALUE_AUTH_VISA_PCT", "0.05"))
    min_next_actions_unique = int(os.getenv("HONESTROLES_MIN_VALUE_NEXT_ACTIONS_UNIQUE", "3"))

    raw = read_parquet(dataset_path, validate=False)
    sampled = raw.sample(n=min(rows, len(raw)), random_state=42).reset_index(drop=True)

    start = time.perf_counter()
    cleaned = clean_historical_jobs(sampled)
    clean_elapsed = time.perf_counter() - start

    signal_input = cleaned.head(min(signal_rows, len(cleaned))).reset_index(drop=True)
    start = time.perf_counter()
    labeled = label_jobs(signal_input, use_llm=False)
    signals = extract_job_signals(labeled, use_llm=False)
    signal_elapsed = time.perf_counter() - start

    skills_non_empty_pct = (
        cleaned.get(SKILLS, pd.Series([None] * len(cleaned)))
        .map(lambda value: isinstance(value, list) and len(value) > 0)
        .mean()
        * 100
    )
    salary_present_pct = (
        pd.to_numeric(cleaned.get(SALARY_MIN), errors="coerce").notna()
        | pd.to_numeric(cleaned.get(SALARY_MAX), errors="coerce").notna()
    ).mean() * 100
    annual_present_pct = (
        pd.to_numeric(cleaned.get("salary_annual_min"), errors="coerce").notna()
        | pd.to_numeric(cleaned.get("salary_annual_max"), errors="coerce").notna()
    ).mean() * 100
    required_skills_pct = (
        signals.get("required_skills_extracted", pd.Series([None] * len(signals)))
        .map(lambda value: isinstance(value, list) and len(value) > 0)
        .mean()
        * 100
    )
    experience_pct = pd.to_numeric(signals.get("experience_years_min"), errors="coerce").notna().mean() * 100
    auth_or_visa_pct = (
        signals.get("visa_sponsorship_signal", pd.Series([None] * len(signals))).map(
            lambda value: isinstance(value, bool)
        )
        | signals.get("work_authorization_required", pd.Series([None] * len(signals))).map(
            lambda value: isinstance(value, bool)
        )
    ).mean() * 100

    ranked = rank_jobs(signals.head(min(5000, len(signals))), use_llm_signals=False, top_n=20)
    planned = build_application_plan(ranked, top_n=10)
    next_actions_unique = planned.get("next_actions", pd.Series([], dtype="object")).astype(str).nunique()

    assert clean_elapsed <= max_clean_seconds
    assert signal_elapsed <= max_signal_seconds

    assert skills_non_empty_pct >= min_skills_pct
    assert salary_present_pct >= min_salary_pct
    assert annual_present_pct >= min_salary_annual_pct
    assert required_skills_pct >= min_req_skills_pct
    assert experience_pct >= min_exp_pct
    assert auth_or_visa_pct >= min_auth_or_visa_pct
    assert next_actions_unique >= min_next_actions_unique
