from __future__ import annotations

from pathlib import Path

import pandas as pd

import honestroles as hr
from honestroles import schema


def test_readme_contract_first_quickstart_smoke(tmp_path: Path) -> None:
    input_path = tmp_path / "jobs_current.parquet"
    output_path = tmp_path / "jobs_scored.parquet"

    pd.DataFrame(
        [
            {
                schema.JOB_KEY: "acme::greenhouse::1",
                schema.COMPANY: "Acme",
                schema.SOURCE: "greenhouse",
                schema.JOB_ID: "1",
                schema.TITLE: "Data Scientist",
                schema.LOCATION_RAW: "Remote - US",
                schema.APPLY_URL: "https://example.com/apply",
                schema.INGESTED_AT: "2025-01-01T00:00:00Z",
                schema.CONTENT_HASH: "hash-1",
                schema.DESCRIPTION_TEXT: "Build production ML systems with Python and SQL.",
                schema.REMOTE_FLAG: True,
                schema.SALARY_MIN: 120000,
                schema.SALARY_MAX: 150000,
                schema.SALARY_CURRENCY: "USD",
                schema.SKILLS: ["Python", "SQL", "React"],
            }
        ]
    ).to_parquet(input_path, index=False)

    df = hr.read_parquet(input_path, validate=False)
    df = hr.normalize_source_data_contract(df)
    df = hr.validate_source_data_contract(df)
    df = hr.clean_jobs(df)
    df = hr.filter_jobs(
        df,
        remote_only=True,
        min_salary=120000,
        required_skills=["Python"],
    )
    df = hr.label_jobs(df, use_llm=False)
    df = hr.rate_jobs(df, use_llm=False)

    profile = hr.CandidateProfile.mds_new_grad()
    ranked = hr.rank_jobs(df, profile=profile, use_llm_signals=False, top_n=100)
    plan = hr.build_application_plan(ranked, profile=profile, top_n=20)

    assert len(df) == 1
    assert schema.RATING in df.columns
    assert schema.FIT_SCORE in ranked.columns
    assert schema.NEXT_ACTIONS in plan.columns

    hr.write_parquet(df, output_path)
    assert output_path.exists()
