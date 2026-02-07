import pandas as pd
import pytest


@pytest.fixture()
def sample_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "job_key": "acme::greenhouse::1",
                "company": "Acme",
                "source": "greenhouse",
                "job_id": "1",
                "title": "Senior Software Engineer",
                "location_raw": "New York, USA",
                "apply_url": "https://example.com/apply",
                "description_html": "<p>Build systems.</p>",
                "description_text": "Build systems.",
                "ingested_at": "2025-01-01",
                "content_hash": "hash1",
                "salary_text": "$120000 - $150000",
                "remote_flag": True,
                "skills": ["Python", "AWS"],
            },
            {
                "job_key": "acme::greenhouse::2",
                "company": "Acme",
                "source": "greenhouse",
                "job_id": "2",
                "title": "Product Manager",
                "location_raw": "Remote",
                "apply_url": "https://example.com/apply2",
                "description_html": "<p>Own roadmap.</p>",
                "description_text": "Own roadmap.",
                "ingested_at": "2025-01-02",
                "content_hash": "hash2",
                "remote_flag": True,
                "skills": ["Roadmapping"],
            },
        ]
    )
