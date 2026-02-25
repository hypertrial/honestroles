from __future__ import annotations

import os
from pathlib import Path

import pytest

from honestroles.clean import clean_historical_jobs
from honestroles.io import read_parquet
from honestroles.label import label_jobs
from honestroles.match import extract_job_signals


@pytest.mark.performance
def test_historical_non_llm_quality_coverage_guardrail() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    dataset_path = repo_root / "jobs_historical.parquet"
    if not dataset_path.exists():
        pytest.skip("jobs_historical.parquet not available in repository root")

    clean_rows = int(os.getenv("HONESTROLES_QUALITY_CLEAN_ROWS", "100000"))
    signal_rows = int(os.getenv("HONESTROLES_QUALITY_SIGNAL_ROWS", "20000"))

    min_clean_skills = float(os.getenv("HONESTROLES_MIN_CLEAN_SKILLS_COVERAGE", "0.08"))
    min_clean_salary = float(os.getenv("HONESTROLES_MIN_CLEAN_SALARY_COVERAGE", "0.04"))
    min_clean_remote = float(os.getenv("HONESTROLES_MIN_CLEAN_REMOTE_COVERAGE", "0.05"))
    min_required_skills = float(os.getenv("HONESTROLES_MIN_REQUIRED_SKILLS_COVERAGE", "0.08"))
    min_experience_years = float(os.getenv("HONESTROLES_MIN_EXPERIENCE_YEARS_COVERAGE", "0.30"))
    max_pm_false_positive_ratio = float(os.getenv("HONESTROLES_MAX_PM_FALSE_POSITIVE_RATIO", "0.20"))

    raw = read_parquet(dataset_path, validate=False).head(clean_rows)
    cleaned = clean_historical_jobs(raw)
    signal_input = cleaned.head(signal_rows)

    clean_skills_coverage = cleaned["skills"].map(
        lambda value: isinstance(value, list) and len(value) > 0
    ).mean()
    clean_salary_coverage = (cleaned["salary_min"].notna() | cleaned["salary_max"].notna()).mean()
    clean_remote_coverage = (
        cleaned["remote_type"].astype("string").fillna("").str.lower().eq("remote")
        | cleaned["remote_flag"].fillna(False).astype(bool)
    ).mean()

    signals = extract_job_signals(signal_input, use_llm=False)
    required_skills_coverage = signals["required_skills_extracted"].map(
        lambda value: isinstance(value, list) and len(value) > 0
    ).mean()
    experience_years_coverage = signals["experience_years_min"].notna().mean()

    labels = label_jobs(signal_input, use_llm=False)
    product_rows = labels["role_category"].astype("string").fillna("").eq("product")
    if bool(product_rows.any()):
        title = labels["title"].astype("string").fillna("")
        likely_pm_false_positives = (
            product_rows
            & title.str.contains(r"\bpm\b", case=False, regex=True)
            & ~title.str.contains(
                "product manager|program manager|product owner|product management",
                case=False,
                regex=True,
            )
        ).sum()
        pm_false_positive_ratio = likely_pm_false_positives / int(product_rows.sum())
    else:
        pm_false_positive_ratio = 0.0

    assert clean_skills_coverage >= min_clean_skills
    assert clean_salary_coverage >= min_clean_salary
    assert clean_remote_coverage >= min_clean_remote
    assert required_skills_coverage >= min_required_skills
    assert experience_years_coverage >= min_experience_years
    assert pm_false_positive_ratio <= max_pm_false_positive_ratio
