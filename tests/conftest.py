import os

import pandas as pd
import pytest

from honestroles.plugins import reset_plugins
from honestroles.schema import REQUIRED_COLUMNS

try:
    from hypothesis import HealthCheck, settings
except ModuleNotFoundError:  # pragma: no cover - test dependency guard
    settings = None
else:
    settings.register_profile(
        "ci_smoke",
        max_examples=25,
        derandomize=True,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large],
    )
    settings.register_profile(
        "nightly_deep",
        max_examples=250,
        derandomize=False,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large],
    )
    profile = os.getenv("HYPOTHESIS_PROFILE")
    if profile:
        settings.load_profile(profile)


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
                "location_raw": "New York, NY, USA",
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
                "location_raw": "Remote, US",
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


@pytest.fixture()
def empty_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(REQUIRED_COLUMNS))


@pytest.fixture()
def minimal_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "job_key": "acme::greenhouse::3",
                "company": "Acme",
                "source": "greenhouse",
                "job_id": "3",
                "title": "Data Scientist",
                "location_raw": "Remote",
                "apply_url": "https://example.com/apply3",
                "ingested_at": "2025-01-03",
                "content_hash": "hash3",
            }
        ]
    )


def pytest_runtest_setup(item: pytest.Item) -> None:
    reset_plugins()


def pytest_runtest_teardown(item: pytest.Item, nextitem: pytest.Item | None) -> None:
    reset_plugins()
