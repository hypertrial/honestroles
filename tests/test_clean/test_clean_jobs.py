import pandas as pd

from honestroles.clean import clean_jobs


def test_clean_jobs_integration(sample_df: pd.DataFrame) -> None:
    cleaned = clean_jobs(sample_df)
    assert "description_text" in cleaned.columns
    assert "city" in cleaned.columns
    assert "salary_min" in cleaned.columns


def test_clean_jobs_empty_dataframe(empty_df: pd.DataFrame) -> None:
    cleaned = clean_jobs(empty_df)
    assert cleaned.empty


def test_clean_jobs_backfills_salary_skills_and_remote_from_text() -> None:
    df = pd.DataFrame(
        [
            {
                "job_key": "acme::greenhouse::1",
                "company": "Acme",
                "source": "greenhouse",
                "job_id": "1",
                "title": "Data Engineer (Remote)",
                "location_raw": "Unknown",
                "apply_url": "https://acme.com/jobs/1",
                "description_text": (
                    "Must have Python and SQL. Nice to have Airflow. "
                    "Compensation: $120,000 - $150,000 per year."
                ),
                "ingested_at": "2025-01-01T00:00:00Z",
                "content_hash": "h1",
                "salary_text": None,
            }
        ]
    )
    cleaned = clean_jobs(df)
    assert cleaned.loc[0, "remote_type"] == "remote"
    assert cleaned.loc[0, "salary_min"] == 120000.0
    assert cleaned.loc[0, "salary_max"] == 150000.0
    assert "python" in cleaned.loc[0, "skills"]
