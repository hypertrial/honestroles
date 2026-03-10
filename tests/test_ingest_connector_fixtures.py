from __future__ import annotations

from datetime import UTC, datetime
import json
from pathlib import Path
from typing import Any

import pytest

from honestroles.errors import ConfigValidationError
from honestroles.ingest.normalize import normalize_records
from honestroles.ingest.sources.ashby import fetch_ashby_jobs
from honestroles.ingest.sources.greenhouse import fetch_greenhouse_jobs
from honestroles.ingest.sources.lever import fetch_lever_jobs
from honestroles.ingest.sources.workable import fetch_workable_jobs

_FIXTURES = Path(__file__).resolve().parent / "fixtures" / "ingest"


def _fixture(source: str, name: str) -> Any:
    path = _FIXTURES / source / name
    return json.loads(path.read_text(encoding="utf-8"))


def test_greenhouse_connector_fixture_pagination_and_normalization() -> None:
    payloads = {
        0: _fixture("greenhouse", "page0.json"),
        1: _fixture("greenhouse", "page1.json"),
    }

    def fake_get(url: str) -> Any:
        page = int(url.rsplit("page=", 1)[1])
        return payloads[page]

    jobs, request_count, warnings = fetch_greenhouse_jobs(
        "acme", max_pages=5, max_jobs=100, http_get_json=fake_get
    )
    assert request_count == 2
    assert warnings == ()
    normalized = normalize_records(
        jobs,
        source="greenhouse",
        source_ref="acme",
        ingested_at_utc=datetime.now(UTC).isoformat(),
    )
    row = normalized[0]
    for key in (
        "source_updated_at",
        "work_mode",
        "salary_currency",
        "salary_interval",
        "employment_type",
        "seniority",
    ):
        assert key in row
    assert row["work_mode"] in {"remote", "hybrid", "onsite", "unknown"}


def test_lever_connector_fixture_pagination_and_malformed_payload() -> None:
    payloads = {
        0: _fixture("lever", "page0.json"),
        1: _fixture("lever", "page1.json"),
    }

    def fake_get(url: str) -> Any:
        skip = int(url.split("skip=", 1)[1].split("&", 1)[0])
        return payloads[0] if skip == 0 else payloads[1]

    jobs, request_count, warnings = fetch_lever_jobs(
        "acme", max_pages=5, max_jobs=100, http_get_json=fake_get
    )
    assert request_count == 2
    assert len(jobs) == 1
    assert warnings == ()
    with pytest.raises(ConfigValidationError, match="array or object"):
        fetch_lever_jobs("acme", max_pages=1, max_jobs=10, http_get_json=lambda _u: {"bad": 1})


def test_ashby_connector_fixture_loop_protection_and_partial_payload() -> None:
    pages = [
        _fixture("ashby", "page0.json"),
        _fixture("ashby", "page1_loop.json"),
    ]
    iterator = iter(pages)
    jobs, request_count, warnings = fetch_ashby_jobs(
        "acme", max_pages=5, max_jobs=100, http_get_json=lambda _u: next(iterator)
    )
    assert request_count == 2
    assert len(jobs) == 2
    assert warnings == ("INGEST_CURSOR_LOOP_DETECTED",)
    normalized = normalize_records(
        [{"id": "x"}],
        source="ashby",
        source_ref="acme",
        ingested_at_utc=datetime.now(UTC).isoformat(),
    )
    assert normalized[0]["source_job_id"] == "x"


def test_workable_connector_fixture_public_endpoints_and_payload_validation() -> None:
    account = _fixture("workable", "account.json")
    locations = _fixture("workable", "locations.json")
    departments = _fixture("workable", "departments.json")

    def fake_get(url: str) -> Any:
        if url.endswith("details=true"):
            return account
        if url.endswith("/locations"):
            return locations
        if url.endswith("/departments"):
            return departments
        return {}

    jobs, request_count, warnings = fetch_workable_jobs(
        "acme", max_pages=1, max_jobs=100, http_get_json=fake_get
    )
    assert request_count == 3
    assert len(jobs) == 1
    assert warnings == ()
    with pytest.raises(ConfigValidationError, match="'jobs' array"):
        fetch_workable_jobs("acme", max_pages=1, max_jobs=10, http_get_json=lambda _u: {"x": []})
