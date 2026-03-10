from __future__ import annotations

import argparse
from datetime import UTC, datetime, timedelta
import importlib
import json
from pathlib import Path
from typing import Any
from urllib import error

import polars as pl
import pytest

from honestroles import sync_source
from honestroles.cli import handlers, lineage, output
from honestroles.errors import ConfigValidationError, HonestRolesError
from honestroles.ingest import (
    BatchIngestionResult,
    INGEST_SCHEMA_VERSION,
    IngestionReport,
    IngestionRequest,
    IngestionResult,
    IngestionStateEntry,
    load_ingest_manifest,
    sync_sources_from_manifest,
)
from honestroles.ingest import http as ingest_http
from honestroles.ingest import models as ingest_models
from honestroles.ingest import normalize as ingest_normalize
from honestroles.ingest import service as ingest_service
from honestroles.ingest import state as ingest_state
from honestroles.ingest.dedup import dedup_key, deduplicate_records
from honestroles.ingest.sources.ashby import fetch_ashby_jobs
from honestroles.ingest.sources.greenhouse import fetch_greenhouse_jobs
from honestroles.ingest.sources.lever import fetch_lever_jobs
from honestroles.ingest.sources.workable import fetch_workable_jobs


class _DummyResponse:
    def __init__(self, body: str) -> None:
        self._body = body.encode("utf-8")

    def __enter__(self) -> "_DummyResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def read(self) -> bytes:
        return self._body

    def close(self) -> None:
        return None


def test_ingest_model_roundtrips() -> None:
    request = IngestionRequest(source="greenhouse", source_ref="acme")
    assert request.source == "greenhouse"
    entry = IngestionStateEntry.from_mapping(
        {
            "high_watermark_posted_at": "2026-01-01T00:00:00+00:00",
            "high_watermark_updated_at": "2026-01-01T12:00:00+00:00",
            "last_success_at_utc": "2026-01-02T00:00:00+00:00",
            "last_coverage_complete": True,
            "recent_source_job_ids": ["1", "", "2"],
        }
    )
    assert entry.recent_source_job_ids == ("1", "2")
    assert entry.high_watermark_updated_at == "2026-01-01T12:00:00+00:00"
    assert entry.last_coverage_complete is True
    assert IngestionStateEntry.from_mapping(None).recent_source_job_ids == ()
    assert IngestionStateEntry.from_mapping({"recent_source_job_ids": "bad"}).recent_source_job_ids == ()
    assert ingest_models._string_or_none(" ") is None
    assert ingest_models._string_or_none(None) is None
    assert ingest_models.utc_now_iso().endswith("Z")

    report = IngestionReport(
        schema_version=INGEST_SCHEMA_VERSION,
        status="pass",
        source="greenhouse",
        source_ref="acme",
        started_at_utc="2026-01-01T00:00:00+00:00",
        finished_at_utc="2026-01-01T00:00:10+00:00",
        duration_ms=10_000,
        request_count=2,
        fetched_count=3,
        normalized_count=3,
        dedup_dropped=1,
        high_watermark_before=None,
        high_watermark_after="2026-01-01T00:00:00+00:00",
        output_paths={"report": "/tmp/r.json", "parquet": "/tmp/o.parquet"},
        new_count=1,
        updated_count=1,
        unchanged_count=1,
        skipped_by_state=1,
        tombstoned_count=0,
        coverage_complete=True,
        retry_count=2,
        http_status_counts={"200": 3},
    )
    result = IngestionResult(
        report=report,
        output_parquet=Path("/tmp/o.parquet"),
        report_file=Path("/tmp/r.json"),
        rows_written=2,
    )
    payload = result.to_payload()
    assert payload["schema_version"] == INGEST_SCHEMA_VERSION
    assert payload["rows_written"] == 2
    assert payload["new_count"] == 1

    batch = BatchIngestionResult(
        schema_version=INGEST_SCHEMA_VERSION,
        status="pass",
        started_at_utc="2026-01-01T00:00:00+00:00",
        finished_at_utc="2026-01-01T00:00:10+00:00",
        duration_ms=10000,
        total_sources=1,
        pass_count=1,
        fail_count=0,
        total_rows_written=2,
        total_fetched_count=3,
        total_request_count=2,
        sources=(payload,),
        report_file=Path("/tmp/batch.json"),
        check_codes=("INGEST_TRUNCATED",),
    )
    batch_payload = batch.to_payload()
    assert batch_payload["total_sources"] == 1
    assert batch_payload["check_codes"] == ["INGEST_TRUNCATED"]
    batch_without_report = BatchIngestionResult(
        schema_version=INGEST_SCHEMA_VERSION,
        status="pass",
        started_at_utc="2026-01-01T00:00:00+00:00",
        finished_at_utc="2026-01-01T00:00:01+00:00",
        duration_ms=1000,
        total_sources=0,
        pass_count=0,
        fail_count=0,
        total_rows_written=0,
        total_fetched_count=0,
        total_request_count=0,
        sources=(),
        report_file=None,
    )
    assert "report_file" not in batch_without_report.to_payload()


def test_http_fetch_json_success_and_error_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    sequence: list[Any] = [
        _DummyResponse("{}"),
        _DummyResponse(""),
    ]

    def fake_urlopen(_req, timeout=0):
        return sequence.pop(0)

    monkeypatch.setattr(ingest_http.request, "urlopen", fake_urlopen)
    assert ingest_http.fetch_json("https://example.com") == {}
    assert ingest_http.fetch_json("https://example.com") == {}

    captured_headers: dict[str, str] = {}

    def capture_request(req, timeout=0):
        nonlocal captured_headers
        captured_headers = dict(req.headers)
        return _DummyResponse("{}")

    monkeypatch.setattr(ingest_http.request, "urlopen", capture_request)
    ingest_http.fetch_json("https://headers.example.com", headers={"X-Test": "1"})
    lowered_headers = {k.lower(): v for k, v in captured_headers.items()}
    assert lowered_headers.get("x-test") == "1"

    attempts = {"count": 0}

    def flaky(_req, timeout=0):
        attempts["count"] += 1
        if attempts["count"] < 2:
            raise error.URLError("down")
        return _DummyResponse('{"ok": true}')

    monkeypatch.setattr(ingest_http.request, "urlopen", flaky)
    monkeypatch.setattr(ingest_http.time, "sleep", lambda _s: None)
    assert ingest_http.fetch_json("https://retry.example.com", max_retries=2) == {"ok": True}

    http_attempts = {"count": 0}

    def retryable_http(_req, timeout=0):
        http_attempts["count"] += 1
        if http_attempts["count"] == 1:
            raise error.HTTPError(
                "https://retry-http",
                429,
                "rate",
                hdrs=None,
                fp=_DummyResponse("slow"),
            )
        return _DummyResponse('{"ok": true}')

    monkeypatch.setattr(ingest_http.request, "urlopen", retryable_http)
    assert ingest_http.fetch_json("https://retry-http.example.com", max_retries=2) == {"ok": True}

    def http_500(_req, timeout=0):
        raise error.HTTPError(
            "https://x",
            500,
            "err",
            hdrs=None,
            fp=_DummyResponse("server"),
        )

    monkeypatch.setattr(ingest_http.request, "urlopen", http_500)
    with pytest.raises(HonestRolesError, match="HTTP 500"):
        ingest_http.fetch_json("https://server.example.com", max_retries=0)

    def bad_json(_req, timeout=0):
        return _DummyResponse("{")

    monkeypatch.setattr(ingest_http.request, "urlopen", bad_json)
    with pytest.raises(HonestRolesError, match="not valid JSON"):
        ingest_http.fetch_json("https://json.example.com")

    monkeypatch.setattr(ingest_http.request, "urlopen", lambda _req, timeout=0: (_ for _ in ()).throw(error.URLError("down")))
    with pytest.raises(HonestRolesError, match="down"):
        ingest_http.fetch_json("https://down.example.com", max_retries=0)


def test_http_error_detail_fallback() -> None:
    exc = error.HTTPError("https://x", 400, "bad", hdrs=None, fp=None)
    assert ingest_http._http_error_detail(exc) == "<no-body>"

    class _BoomHTTPError(error.HTTPError):
        def read(self) -> bytes:  # type: ignore[override]
            raise RuntimeError("boom")

    boom = _BoomHTTPError("https://x", 400, "bad", hdrs=None, fp=None)
    assert ingest_http._http_error_detail(boom) == "<no-body>"


def test_source_connectors_shapes_and_errors() -> None:
    def greenhouse_get(url: str) -> Any:
        if "page=0" in url:
            return {"jobs": [{"id": 1}, {"id": 2}]}
        return {"jobs": []}

    jobs, requests, greenhouse_warnings = fetch_greenhouse_jobs(
        "acme",
        max_pages=3,
        max_jobs=10,
        http_get_json=greenhouse_get,
    )
    assert len(jobs) == 2
    assert requests == 2
    assert greenhouse_warnings == ()

    with pytest.raises(ConfigValidationError, match="must include 'jobs'"):
        fetch_greenhouse_jobs("acme", max_pages=1, max_jobs=1, http_get_json=lambda _u: {})
    with pytest.raises(ConfigValidationError, match="must be an array"):
        fetch_greenhouse_jobs("acme", max_pages=1, max_jobs=1, http_get_json=lambda _u: {"jobs": {}})
    with pytest.raises(ConfigValidationError, match="non-empty"):
        fetch_greenhouse_jobs("", max_pages=1, max_jobs=1, http_get_json=lambda _u: {"jobs": []})
    capped_jobs, _, _ = fetch_greenhouse_jobs(
        "acme",
        max_pages=3,
        max_jobs=1,
        http_get_json=lambda _u: {"jobs": [{"id": 1}, {"id": 2}]},
    )
    assert len(capped_jobs) == 1
    greenhouse_counter = {"n": 0}

    def greenhouse_exhausted(_url: str) -> dict[str, Any]:
        greenhouse_counter["n"] += 1
        return {"jobs": [{"id": greenhouse_counter["n"]}]}

    exhausted_greenhouse_jobs, exhausted_greenhouse_requests, _ = fetch_greenhouse_jobs(
        "acme",
        max_pages=2,
        max_jobs=10,
        http_get_json=greenhouse_exhausted,
    )
    assert len(exhausted_greenhouse_jobs) == 2
    assert exhausted_greenhouse_requests == 2
    repeated_greenhouse_jobs, repeated_greenhouse_requests, repeated_greenhouse_warnings = (
        fetch_greenhouse_jobs(
            "acme",
            max_pages=3,
            max_jobs=10,
            http_get_json=lambda _u: {"jobs": [{"id": 1}]},
        )
    )
    assert len(repeated_greenhouse_jobs) == 1
    assert repeated_greenhouse_requests == 2
    assert repeated_greenhouse_warnings == ("INGEST_PAGE_REPEAT_DETECTED",)
    mixed_shape_jobs, mixed_shape_requests, mixed_shape_warnings = fetch_greenhouse_jobs(
        "acme",
        max_pages=1,
        max_jobs=10,
        http_get_json=lambda _u: {
            "jobs": [
                {"id": None, "absolute_url": "https://x/jobs/1"},
                {"title": "fallback"},
                "non-dict-item",
            ]
        },
    )
    assert len(mixed_shape_jobs) == 2
    assert mixed_shape_requests == 1
    assert mixed_shape_warnings == ()

    lever_calls = {"n": 0}

    def lever_get(_url: str) -> Any:
        lever_calls["n"] += 1
        if lever_calls["n"] == 1:
            return [{"id": "1"}]
        return []

    lever_jobs, lever_requests, lever_warnings = fetch_lever_jobs(
        "acme",
        max_pages=3,
        max_jobs=10,
        http_get_json=lever_get,
    )
    assert len(lever_jobs) == 1
    assert lever_requests == 2
    assert lever_warnings == ()
    lever_data_jobs, _, _ = fetch_lever_jobs(
        "acme",
        max_pages=1,
        max_jobs=1,
        http_get_json=lambda _u: {"data": [{"id": "1"}, {"id": "2"}]},
    )
    assert len(lever_data_jobs) == 1
    exhausted_lever_jobs, exhausted_lever_requests, _ = fetch_lever_jobs(
        "acme",
        max_pages=2,
        max_jobs=10,
        http_get_json=lambda _u: [{"id": "1"}],
    )
    assert len(exhausted_lever_jobs) == 2
    assert exhausted_lever_requests == 2
    with pytest.raises(ConfigValidationError, match="array or object"):
        fetch_lever_jobs("acme", max_pages=1, max_jobs=1, http_get_json=lambda _u: {"x": 1})
    with pytest.raises(ConfigValidationError, match="non-empty"):
        fetch_lever_jobs("", max_pages=1, max_jobs=1, http_get_json=lambda _u: [])

    ashby_pages = iter(
        [
            {"jobs": [{"id": "1"}], "nextCursor": "abc"},
            {"jobs": [{"id": "2"}], "nextCursor": "abc"},
        ]
    )
    ashby_jobs, ashby_requests, ashby_warnings = fetch_ashby_jobs(
        "acme",
        max_pages=5,
        max_jobs=10,
        http_get_json=lambda _u: next(ashby_pages),
    )
    assert len(ashby_jobs) == 2
    assert ashby_requests == 2
    assert ashby_warnings == ("INGEST_CURSOR_LOOP_DETECTED",)
    ashby_no_cursor_jobs, ashby_no_cursor_requests, _ = fetch_ashby_jobs(
        "acme",
        max_pages=2,
        max_jobs=10,
        http_get_json=lambda _u: {"postings": [{"id": "1"}]},
    )
    assert len(ashby_no_cursor_jobs) == 1
    assert ashby_no_cursor_requests == 1
    ashby_empty_jobs, ashby_empty_requests, _ = fetch_ashby_jobs(
        "acme",
        max_pages=2,
        max_jobs=10,
        http_get_json=lambda _u: {"results": []},
    )
    assert ashby_empty_jobs == []
    assert ashby_empty_requests == 1
    ashby_limited_jobs, _, _ = fetch_ashby_jobs(
        "acme",
        max_pages=3,
        max_jobs=1,
        http_get_json=lambda _u: {"jobs": [{"id": "1"}, {"id": "2"}], "nextCursor": "n"},
    )
    assert len(ashby_limited_jobs) == 1
    ashby_exhaust_counter = {"n": 0}

    def ashby_exhaust(_url: str) -> dict[str, Any]:
        ashby_exhaust_counter["n"] += 1
        return {
            "jobs": [{"id": str(ashby_exhaust_counter["n"])}],
            "nextCursor": f"cursor-{ashby_exhaust_counter['n']}",
        }

    exhausted_ashby_jobs, exhausted_ashby_requests, _ = fetch_ashby_jobs(
        "acme",
        max_pages=2,
        max_jobs=10,
        http_get_json=ashby_exhaust,
    )
    assert len(exhausted_ashby_jobs) == 2
    assert exhausted_ashby_requests == 2
    with pytest.raises(ConfigValidationError, match="JSON object"):
        fetch_ashby_jobs("acme", max_pages=1, max_jobs=1, http_get_json=lambda _u: [])
    with pytest.raises(ConfigValidationError, match="jobs/postings/results"):
        fetch_ashby_jobs("acme", max_pages=1, max_jobs=1, http_get_json=lambda _u: {"x": []})
    with pytest.raises(ConfigValidationError, match="non-empty"):
        fetch_ashby_jobs("", max_pages=1, max_jobs=1, http_get_json=lambda _u: {"jobs": []})
    assert ingest_service._SOURCE_FETCHERS["ashby"] == fetch_ashby_jobs

    workable_calls: list[str] = []

    def workable_get(url: str) -> Any:
        workable_calls.append(url)
        if url.endswith("details=true"):
            return {"jobs": [{"shortcode": "A"}]}
        return {"locations": []}

    workable_jobs, workable_requests, workable_warnings = fetch_workable_jobs(
        "acme",
        max_pages=1,
        max_jobs=10,
        http_get_json=workable_get,
    )
    assert len(workable_jobs) == 1
    assert workable_requests == 3
    assert workable_warnings == ()
    with pytest.raises(ConfigValidationError, match="max-pages"):
        fetch_workable_jobs("acme", max_pages=0, max_jobs=1, http_get_json=lambda _u: {})
    with pytest.raises(ConfigValidationError, match="non-empty"):
        fetch_workable_jobs("", max_pages=1, max_jobs=1, http_get_json=lambda _u: {})
    with pytest.raises(ConfigValidationError, match="JSON object"):
        fetch_workable_jobs(
            "acme",
            max_pages=1,
            max_jobs=1,
            http_get_json=lambda _u: [],
        )
    workable_results_jobs, _, _ = fetch_workable_jobs(
        "acme",
        max_pages=2,
        max_jobs=1,
        http_get_json=lambda url: {"results": [{"id": "x"}, {"id": "y"}]}
        if url.endswith("details=true")
        else {},
    )
    assert len(workable_results_jobs) == 1
    with pytest.raises(ConfigValidationError, match="'jobs' array"):
        fetch_workable_jobs(
            "acme",
            max_pages=1,
            max_jobs=1,
            http_get_json=lambda _u: {"x": []},
        )
    with pytest.raises(
        ConfigValidationError, match="invalid or not publicly exposed"
    ):
        fetch_workable_jobs(
            "acme",
            max_pages=1,
            max_jobs=1,
            http_get_json=lambda _u: (_ for _ in ()).throw(
                HonestRolesError("ingestion request failed for 'x': HTTP 404 Not Found")
            ),
        )
    with pytest.raises(HonestRolesError, match="HTTP 500"):
        fetch_workable_jobs(
            "acme",
            max_pages=1,
            max_jobs=1,
            http_get_json=lambda _u: (_ for _ in ()).throw(
                HonestRolesError("ingestion request failed for 'x': HTTP 500 server")
            ),
        )


def test_normalize_records_and_dataframe_paths() -> None:
    now = datetime.now(UTC).isoformat()
    greenhouse = {
        "id": 1,
        "title": "T",
        "location": {"name": "Remote"},
        "absolute_url": "https://a",
        "company_name": "Stripe",
        "first_published": now,
    }
    lever = {
        "id": "2",
        "text": "L",
        "categories": {"location": "NYC", "team": "Ops"},
        "description": "<p>x</p>",
        "hostedUrl": "https://b",
        "createdAt": 1_700_000_000_000,
    }
    ashby = {
        "jobId": "3",
        "jobTitle": "A",
        "jobUrl": "https://c",
        "publishedAt": now,
        "descriptionPlain": "plain",
        "descriptionHtml": "<p>plain</p>",
        "isRemote": True,
    }
    workable = {
        "shortcode": "4",
        "code": "WB-4",
        "title": "W",
        "url": "https://d",
        "application_url": "https://apply.d",
        "city": "Lisbon",
        "country": "PT",
        "telecommuting": False,
        "published_on": now,
    }
    workable_nested_location = {
        "shortcode": "6",
        "title": "W3",
        "url": "https://d3",
        "location": {"city": "Porto"},
    }
    workable_text_location = {"shortcode": "5", "title": "W2", "url": "https://d2", "location": "Remote"}
    generic = {"id": "5", "title": "G", "url": "https://e", "posted_at": "bad-time"}

    rows = ingest_normalize.normalize_records(
        [greenhouse],
        source="greenhouse",
        source_ref="acme",
        ingested_at_utc=now,
    )
    assert rows[0]["source_payload_hash"]
    assert rows[0]["remote"] is True
    assert rows[0]["job_url"] == "https://a"
    assert rows[0]["company"] == "Stripe"
    assert rows[0]["posted_at"] is not None

    assert ingest_normalize._extract_lever(lever)["company"] == "Ops"
    assert ingest_normalize._extract_ashby(ashby)["title"] == "A"
    assert ingest_normalize._extract_ashby(ashby)["description_text"] == "plain"
    assert ingest_normalize._extract_ashby({"id": "a", "team": {"name": "Core"}})["company"] == "Core"
    assert ingest_normalize._extract_workable(workable)["remote"] is False
    assert ingest_normalize._extract_workable(workable)["apply_url"] == "https://apply.d"
    assert ingest_normalize._extract_workable(workable)["source_job_id"] == "WB-4"
    assert ingest_normalize._extract_workable(workable)["location"] == "Lisbon, PT"
    assert ingest_normalize._extract_workable(workable_nested_location)["location"] == "Porto"
    assert ingest_normalize._extract_workable(workable_text_location)["remote"] is True
    assert ingest_normalize._extract_generic(generic)["posted_at"] == "bad-time"
    assert ingest_normalize._ensure_metadata_columns(pl.DataFrame({"id": ["1"]})).height == 1
    assert ingest_normalize._coerce_float("x") is None
    assert ingest_normalize._coerce_float(1) == 1.0
    assert ingest_normalize._coerce_float(" ") is None
    assert ingest_normalize._coerce_timestamp("2026-01-01T00:00:00Z") is not None
    assert ingest_normalize._coerce_timestamp("2026-01-01T00:00:00").endswith("+00:00")
    assert ingest_normalize._coerce_timestamp("bad") == "bad"
    assert ingest_normalize._coerce_timestamp_or_epoch(1_700_000_000_000) is not None
    assert ingest_normalize._coerce_timestamp_or_epoch(1_700_000_000) is not None
    assert ingest_normalize._coerce_bool("yes") is True
    assert ingest_normalize._coerce_bool("no") is False
    assert ingest_normalize._coerce_bool("maybe") is None
    assert ingest_normalize._normalize_description_text(None, "<p>A&nbsp;B</p>") == "A B"
    assert ingest_normalize._infer_remote("onsite") is False
    assert ingest_normalize._infer_remote("unknown") is None
    assert ingest_normalize._infer_remote(True) is True
    assert ingest_normalize._infer_work_mode(False) == "onsite"
    assert ingest_normalize._normalize_remote_flag(None, "hybrid", None) is False
    assert (
        ingest_normalize._location_from_workable(
            {"locations": [{"name": "Lisbon"}, "Porto"]}
        )
        == "Lisbon, Porto"
    )
    assert ingest_normalize._location_from_workable({"locations": [{}, "   "]}) is None
    assert ingest_normalize._location_from_workable({}) is None
    assert ingest_normalize._text_or_none(" ") is None
    assert (
        ingest_normalize._resolve_posted_at(
            source="workable",
            raw={"created_at": "2026-01-01T00:00:00Z"},
            current=None,
        )
        is not None
    )

    generic_rows = ingest_normalize.normalize_records(
        [
            {
                "id": "g-1",
                "title": "Generic",
                "description_html": "<p>hello&nbsp;world</p>",
                "url": "https://example.com/jobs/1",
            }
        ],
        source="unknown",
        source_ref="fallback-company",
        ingested_at_utc=now,
    )
    assert generic_rows[0]["company"] == "fallback-company"
    assert generic_rows[0]["description_text"] == "hello world"

    frame = ingest_normalize.normalized_dataframe(
        ingest_normalize.normalize_records(
            [lever, ashby, workable],
            source="lever",
            source_ref="ref",
            ingested_at_utc=now,
        )
    )
    for field in ingest_normalize.INGEST_METADATA_FIELDS:
        assert field in frame.columns
    empty = ingest_normalize.normalized_dataframe([])
    assert "source" in empty.columns


def test_dedup_key_precedence_and_normalization() -> None:
    key1 = dedup_key({"apply_url": "HTTPS://x.com/job/1?utm=abc"})
    key2 = dedup_key({"job_url": "https://x.com/job/1#frag"})
    assert key1 == key2
    key3 = dedup_key({"source": "lever", "source_job_id": "12"})
    assert key3.startswith("source-id:")
    key4 = dedup_key({"title": "A", "company": "B", "location": "C", "posted_at": "D"})
    assert key4.startswith("fallback:")
    key5 = dedup_key({"apply_url": "jobs/123"})
    assert key5 == "url:jobs/123"
    key6 = dedup_key({"apply_url": "   ", "job_url": "   ", "title": "A"})
    assert key6.startswith("fallback:")
    gh_key_one = dedup_key({"apply_url": "https://stripe.com/jobs/search?gh_jid=111&utm=abc"})
    gh_key_two = dedup_key({"apply_url": "https://stripe.com/jobs/search?gh_jid=222&utm=xyz"})
    gh_key_tracking_variant = dedup_key(
        {"apply_url": "https://stripe.com/jobs/search?utm=xyz&gh_jid=111"}
    )
    assert gh_key_one != gh_key_two
    assert gh_key_one == gh_key_tracking_variant

    records, dropped = deduplicate_records(
        [
            {"apply_url": "https://x.com/job/1"},
            {"job_url": "https://x.com/job/1?x=1"},
            {"source": "lever", "source_job_id": "1"},
            {"source": "lever", "source_job_id": "1"},
        ]
    )
    assert len(records) == 2
    assert dropped == 2


def test_state_load_write_filter_update(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    assert ingest_state.load_state(state_path) == {}

    entries = {
        ingest_state.state_key("lever", "acme"): IngestionStateEntry(
            high_watermark_posted_at="2026-01-01T00:00:00+00:00",
            last_success_at_utc="2026-01-01T00:00:00+00:00",
            recent_source_job_ids=("1",),
        )
    }
    ingest_state.write_state(state_path, entries)
    loaded = ingest_state.load_state(state_path)
    entry = loaded[ingest_state.state_key("lever", "acme")]
    assert entry.high_watermark_posted_at == "2026-01-01T00:00:00+00:00"
    assert ingest_state._text_or_none(" ") is None
    assert ingest_state._parse_iso("bad") is None
    assert ingest_state._parse_iso("2026-01-01T00:00:00Z") is not None
    assert ingest_state._parse_iso("2026-01-01T00:00:00") is not None
    assert ingest_state._parse_iso("  ") is None
    assert ingest_state._text_or_none(None) is None

    filtered, before, skipped_count = ingest_state.filter_incremental(
        [
            {"source_job_id": "1", "posted_at": "2026-01-03T00:00:00+00:00"},
            {"source_job_id": "2", "posted_at": "2025-01-01T00:00:00+00:00"},
            {"source_job_id": "3", "posted_at": "2026-02-01T00:00:00+00:00"},
        ],
        entry=entry,
        full_refresh=False,
    )
    assert before == "2026-01-01T00:00:00+00:00"
    assert [row["source_job_id"] for row in filtered] == ["3"]
    assert skipped_count == 2

    no_filter, _, skipped_refresh = ingest_state.filter_incremental(
        [{"source_job_id": "1"}],
        entry=entry,
        full_refresh=True,
    )
    assert len(no_filter) == 1
    assert skipped_refresh == 0

    unchanged, watermark, skipped_unchanged = ingest_state.filter_incremental(
        [{"source_job_id": "x"}],
        entry=IngestionStateEntry(),
        full_refresh=False,
    )
    assert len(unchanged) == 1
    assert watermark is None
    assert skipped_unchanged == 0

    updated = ingest_state.update_state_entry(
        entry,
        records=[{"source_job_id": str(i), "posted_at": "2026-03-01T00:00:00+00:00"} for i in range(520)],
        finished_at_utc="2026-03-01T00:00:01+00:00",
        coverage_complete=False,
    )
    assert len(updated.recent_source_job_ids) == 500
    assert updated.high_watermark_posted_at == "2026-03-01T00:00:00+00:00"
    updated_without_id = ingest_state.update_state_entry(
        IngestionStateEntry(),
        records=[{"posted_at": "2026-03-01T00:00:00+00:00"}],
        finished_at_utc="2026-03-01T00:00:01+00:00",
        coverage_complete=True,
    )
    assert updated_without_id.recent_source_job_ids == ()
    assert updated_without_id.last_coverage_complete is True

    bad = tmp_path / "bad.json"
    bad.write_text("{", encoding="utf-8")
    with pytest.raises(ConfigValidationError, match="invalid ingestion state file"):
        ingest_state.load_state(bad)
    bad.write_text("[]", encoding="utf-8")
    with pytest.raises(ConfigValidationError, match="root must be an object"):
        ingest_state.load_state(bad)
    bad.write_text('{"entries":[]}', encoding="utf-8")
    with pytest.raises(ConfigValidationError, match="'entries' must be an object"):
        ingest_state.load_state(bad)
    bad.write_text('{"entries":{"a":1}}', encoding="utf-8")
    assert ingest_state.load_state(bad) == {}

    original_loads = ingest_state.json.loads

    def fake_loads(_text: str):
        return {"entries": {1: {"recent_source_job_ids": ["x"]}, "ok": {"recent_source_job_ids": ["1"]}}}

    ingest_state.json.loads = fake_loads  # type: ignore[assignment]
    try:
        loaded_fake = ingest_state.load_state(state_path)
    finally:
        ingest_state.json.loads = original_loads  # type: ignore[assignment]
    assert list(loaded_fake) == ["ok"]


def test_service_sync_source_success_and_full_refresh(tmp_path: Path) -> None:
    def fake_fetcher(source_ref: str, *, max_pages: int, max_jobs: int, http_get_json):
        assert source_ref == "acme"
        assert max_pages == 2
        assert max_jobs == 10
        return (
            [
                {
                    "id": "1",
                    "title": "A",
                    "absolute_url": "https://x/jobs/search?gh_jid=1&utm=aaa",
                    "updated_at": "2026-01-02T00:00:00+00:00",
                },
                {
                    "id": "1",
                    "title": "A",
                    "absolute_url": "https://x/jobs/search?utm=bbb&gh_jid=1",
                    "updated_at": "2026-01-02T00:00:00+00:00",
                },
                {
                    "id": "2",
                    "title": "B",
                    "absolute_url": "https://x/jobs/search?gh_jid=2",
                    "updated_at": "2026-01-03T00:00:00+00:00",
                },
            ],
            2,
        )

    original = ingest_service._SOURCE_FETCHERS["greenhouse"]
    ingest_service._SOURCE_FETCHERS["greenhouse"] = fake_fetcher
    try:
        result = ingest_service.sync_source(
            source="greenhouse",
            source_ref="acme",
            output_parquet=tmp_path / "jobs.parquet",
            report_file=tmp_path / "sync_report.json",
            state_file=tmp_path / "state.json",
            write_raw=True,
            max_pages=2,
            max_jobs=10,
            full_refresh=False,
            http_get_json=lambda _url: {},
        )
    finally:
        ingest_service._SOURCE_FETCHERS["greenhouse"] = original

    payload = result.to_payload()
    assert payload["status"] == "pass"
    assert payload["rows_written"] == 2
    assert Path(payload["output_parquet"]).exists()
    assert Path(payload["report_file"]).exists()
    assert Path(payload["raw_file"]).exists()
    assert Path(payload["raw_file"]).parent == Path(payload["output_parquet"]).parent

    # Incremental drop via state.
    original = ingest_service._SOURCE_FETCHERS["greenhouse"]
    ingest_service._SOURCE_FETCHERS["greenhouse"] = fake_fetcher
    try:
        second = ingest_service.sync_source(
            source="greenhouse",
            source_ref="acme",
            output_parquet=tmp_path / "jobs2.parquet",
            report_file=tmp_path / "sync_report2.json",
            state_file=tmp_path / "state.json",
            write_raw=False,
            max_pages=2,
            max_jobs=10,
            full_refresh=False,
            http_get_json=lambda _url: {},
        )
    finally:
        ingest_service._SOURCE_FETCHERS["greenhouse"] = original
    assert second.rows_written == 2
    assert second.report.skipped_by_state >= 1

    # Full refresh bypasses state filter.
    original = ingest_service._SOURCE_FETCHERS["greenhouse"]
    ingest_service._SOURCE_FETCHERS["greenhouse"] = fake_fetcher
    try:
        third = ingest_service.sync_source(
            source="greenhouse",
            source_ref="acme",
            output_parquet=tmp_path / "jobs3.parquet",
            report_file=tmp_path / "sync_report3.json",
            state_file=tmp_path / "state.json",
            write_raw=False,
            max_pages=2,
            max_jobs=10,
            full_refresh=True,
            http_get_json=lambda _url: {},
        )
    finally:
        ingest_service._SOURCE_FETCHERS["greenhouse"] = original
    assert third.rows_written == 2


def test_service_sync_source_failures_and_validation(tmp_path: Path) -> None:
    with pytest.raises(ConfigValidationError, match="unsupported source"):
        ingest_service.sync_source(source="bad", source_ref="acme")
    with pytest.raises(ConfigValidationError, match="source-ref must be non-empty"):
        ingest_service.sync_source(source="lever", source_ref="")
    with pytest.raises(ConfigValidationError, match="source-ref may only contain"):
        ingest_service.sync_source(source="lever", source_ref="bad/ref")
    with pytest.raises(ConfigValidationError, match="max-pages"):
        ingest_service.sync_source(source="lever", source_ref="acme", max_pages=0)
    with pytest.raises(ConfigValidationError, match="max-jobs"):
        ingest_service.sync_source(source="lever", source_ref="acme", max_jobs=0)

    bad_state = tmp_path / "bad_state.json"
    bad_state.write_text("{", encoding="utf-8")
    bad_state_report = tmp_path / "bad_state_report.json"
    with pytest.raises(ConfigValidationError, match="invalid ingestion state file"):
        ingest_service.sync_source(
            source="lever",
            source_ref="acme",
            state_file=bad_state,
            report_file=bad_state_report,
            max_pages=1,
            max_jobs=1,
            http_get_json=lambda _u: [],
        )
    assert bad_state_report.exists()
    assert json.loads(bad_state_report.read_text(encoding="utf-8"))["status"] == "fail"

    def bad_fetcher(*args, **kwargs):
        raise ConfigValidationError("bad source")

    original = ingest_service._SOURCE_FETCHERS["lever"]
    ingest_service._SOURCE_FETCHERS["lever"] = bad_fetcher
    report_file = tmp_path / "fail_report.json"
    try:
        with pytest.raises(ConfigValidationError, match="bad source"):
            ingest_service.sync_source(
                source="lever",
                source_ref="acme",
                report_file=report_file,
            )
    finally:
        ingest_service._SOURCE_FETCHERS["lever"] = original
    assert report_file.exists()
    assert json.loads(report_file.read_text(encoding="utf-8"))["status"] == "fail"

    def crash_fetcher(*args, **kwargs):
        raise RuntimeError("boom")

    original = ingest_service._SOURCE_FETCHERS["lever"]
    ingest_service._SOURCE_FETCHERS["lever"] = crash_fetcher
    try:
        with pytest.raises(HonestRolesError, match="ingestion sync failed"):
            ingest_service.sync_source(
                source="lever",
                source_ref="acme",
                report_file=tmp_path / "fail2_report.json",
            )
    finally:
        ingest_service._SOURCE_FETCHERS["lever"] = original

    def honest_fail_fetcher(*args, **kwargs):
        raise HonestRolesError("network")

    original = ingest_service._SOURCE_FETCHERS["lever"]
    ingest_service._SOURCE_FETCHERS["lever"] = honest_fail_fetcher
    try:
        with pytest.raises(HonestRolesError, match="network"):
            ingest_service.sync_source(
                source="lever",
                source_ref="acme",
                report_file=tmp_path / "fail3_report.json",
            )
    finally:
        ingest_service._SOURCE_FETCHERS["lever"] = original


def test_service_helpers_and_cli_handler(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    output_path, report_path, raw_path = ingest_service._resolve_paths(
        source="lever",
        source_ref="acme",
        output_parquet=None,
        report_file=None,
        write_raw=True,
    )
    assert "dist/ingest/lever/acme" in str(output_path)
    assert raw_path is not None
    explicit_output = tmp_path / "custom" / "jobs.parquet"
    _, _, explicit_raw = ingest_service._resolve_paths(
        source="lever",
        source_ref="acme",
        output_parquet=explicit_output,
        report_file=None,
        write_raw=True,
    )
    assert explicit_raw == explicit_output.with_name("raw.jsonl").resolve()
    assert ingest_service._duration_ms(
        datetime.now(UTC), datetime.now(UTC) + timedelta(milliseconds=1)
    ) >= 1
    raw_target = tmp_path / "raw.jsonl"
    ingest_service._write_raw_jsonl(raw_target, [])
    assert raw_target.read_text(encoding="utf-8") == ""

    dummy_report = IngestionReport(
        schema_version=INGEST_SCHEMA_VERSION,
        status="pass",
        source="lever",
        source_ref="acme",
        started_at_utc="2026-01-01T00:00:00+00:00",
        finished_at_utc="2026-01-01T00:00:01+00:00",
        duration_ms=1000,
        request_count=1,
        fetched_count=1,
        normalized_count=1,
        dedup_dropped=0,
        high_watermark_before=None,
        high_watermark_after=None,
        output_paths={"parquet": str(tmp_path / "jobs.parquet"), "report": str(tmp_path / "r.json")},
    )
    dummy_result = IngestionResult(
        report=dummy_report,
        output_parquet=tmp_path / "jobs.parquet",
        report_file=tmp_path / "r.json",
        rows_written=1,
    )
    monkeypatch.setattr(handlers, "sync_source", lambda **_kwargs: dummy_result)
    result = handlers.handle_ingest_sync(
        argparse.Namespace(
            source="lever",
            source_ref="acme",
            output_parquet=None,
            report_file=None,
            state_file=str(tmp_path / "state.json"),
            write_raw=False,
            max_pages=1,
            max_jobs=10,
            full_refresh=False,
        )
    )
    assert result.payload["rows_written"] == 1

    # top-level API export
    assert sync_source is ingest_service.sync_source


def test_cli_parser_main_dispatch_output_and_lineage_for_ingest(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    parser = importlib.import_module("honestroles.cli.parser").build_parser()
    parsed = parser.parse_args(
        [
            "ingest",
            "sync",
            "--source",
            "lever",
            "--source-ref",
            "acme",
            "--max-pages",
            "2",
            "--max-jobs",
            "100",
        ]
    )
    assert parsed.ingest_command == "sync"
    assert parsed.source == "lever"
    assert parsed.max_pages == 2

    main_mod = importlib.import_module("honestroles.cli.main")
    main_mod.handle_ingest_sync = lambda _args: handlers.CommandResult(
        payload={
            "schema_version": INGEST_SCHEMA_VERSION,
            "status": "pass",
            "source": "lever",
            "source_ref": "acme",
            "rows_written": 1,
            "fetched_count": 1,
            "normalized_count": 1,
            "dedup_dropped": 0,
            "output_paths": {"parquet": "x", "report": "y"},
        }
    )
    code = main_mod.main(
        [
            "ingest",
            "sync",
            "--source",
            "lever",
            "--source-ref",
            "acme",
            "--format",
            "table",
        ]
    )
    assert code == 0
    rendered = capsys.readouterr().out
    assert "SYNC source=lever" in rendered

    assert lineage._command_key({"command": "ingest", "ingest_command": "sync"}) == "ingest.sync"
    assert lineage.should_track({"command": "ingest", "ingest_command": "sync"})
    _, input_hashes, config_hash = lineage.compute_hashes(
        {"command": "ingest", "ingest_command": "sync", "source": "lever", "source_ref": "acme"}
    )
    assert input_hashes == {}
    assert config_hash
    state_path = tmp_path / "state.json"
    state_path.write_text("{}", encoding="utf-8")
    _, hashes_with_state, _ = lineage.compute_hashes(
        {
            "command": "ingest",
            "ingest_command": "sync",
            "source": "lever",
            "source_ref": "acme",
            "state_file": str(state_path),
        }
    )
    assert "state_file" in hashes_with_state
    artifacts = lineage.build_artifact_paths(
        {"command": "ingest", "ingest_command": "sync", "source": "lever", "source_ref": "acme", "write_raw": True},
        payload=None,
    )
    assert "output_parquet" in artifacts
    assert "report_file" in artifacts
    assert "raw_file" in artifacts
    artifacts_custom_output = lineage.build_artifact_paths(
        {
            "command": "ingest",
            "ingest_command": "sync",
            "source": "lever",
            "source_ref": "acme",
            "write_raw": True,
            "output_parquet": str(tmp_path / "custom" / "jobs.parquet"),
        },
        payload=None,
    )
    assert artifacts_custom_output["raw_file"].endswith("custom/raw.jsonl")
    artifacts_no_raw = lineage.build_artifact_paths(
        {"command": "ingest", "ingest_command": "sync", "source": "lever", "source_ref": "acme", "write_raw": False},
        payload=None,
    )
    assert "raw_file" not in artifacts_no_raw
    sanitized_artifacts = lineage.build_artifact_paths(
        {
            "command": "ingest",
            "ingest_command": "sync",
            "source": "lever",
            "source_ref": "bad/ref",
            "write_raw": False,
        },
        payload=None,
    )
    assert "dist/ingest/lever/bad_ref/" in sanitized_artifacts["output_parquet"]

    output.emit_payload(
        {
            "schema_version": INGEST_SCHEMA_VERSION,
            "status": "pass",
            "source": "lever",
            "source_ref": "acme",
            "rows_written": 3,
            "fetched_count": 4,
            "normalized_count": 4,
            "dedup_dropped": 1,
            "output_paths": {"parquet": "/tmp/jobs.parquet"},
        },
        "table",
    )
    assert "parquet" in capsys.readouterr().out
    output.emit_payload(
        {
            "schema_version": INGEST_SCHEMA_VERSION,
            "status": "pass",
            "source": "lever",
            "source_ref": "acme",
            "rows_written": 0,
            "fetched_count": 0,
            "normalized_count": 0,
            "dedup_dropped": 0,
            "output_paths": None,
        },
        "table",
    )


def test_load_ingest_manifest_success_and_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    abs_output = (tmp_path / "absolute-jobs.parquet").resolve()
    manifest_path = tmp_path / "ingest.toml"
    manifest_path.write_text(
        f"""
[defaults]
state_file = "state.json"
write_raw = true
max_pages = 7
max_jobs = 123
full_refresh = false
timeout_seconds = 9.5
max_retries = 2
base_backoff_seconds = 0.4
user_agent = "ua-test"

[[sources]]
source = "greenhouse"
source_ref = "stripe"
enabled = true
output_parquet = "{abs_output}"
report_file = "out/report.json"
state_file = "per-source-state.json"
write_raw = false
max_pages = 2
max_jobs = 42
full_refresh = true
timeout_seconds = 2.5
max_retries = 1
base_backoff_seconds = 0.1
user_agent = "ua-override"

[[sources]]
source = "lever"
source_ref = "netflix"
enabled = false
""".strip(),
        encoding="utf-8",
    )
    manifest = load_ingest_manifest(manifest_path)
    assert manifest.path == manifest_path.resolve()
    assert manifest.defaults.max_pages == 7
    assert manifest.defaults.user_agent == "ua-test"
    assert manifest.defaults.state_file == (tmp_path / "state.json").resolve()
    assert len(manifest.sources) == 2
    first = manifest.sources[0]
    assert first.source == "greenhouse"
    assert first.output_parquet == abs_output
    assert first.state_file == (tmp_path / "per-source-state.json").resolve()
    assert first.timeout_seconds == 2.5
    assert first.max_retries == 1
    assert manifest.sources[1].enabled is False

    with pytest.raises(ConfigValidationError, match="cannot read ingest manifest"):
        load_ingest_manifest(tmp_path / "missing.toml")

    bad = tmp_path / "bad.toml"
    bad.write_text("invalid = [", encoding="utf-8")
    with pytest.raises(ConfigValidationError, match="invalid TOML"):
        load_ingest_manifest(bad)

    bad.write_text(
        """
[defaults]
oops = 1
[[sources]]
source = "lever"
source_ref = "x"
""".strip(),
        encoding="utf-8",
    )
    with pytest.raises(ConfigValidationError, match="unknown defaults keys"):
        load_ingest_manifest(bad)

    bad.write_text(
        """
defaults = []
[[sources]]
source = "lever"
source_ref = "x"
""".strip(),
        encoding="utf-8",
    )
    with pytest.raises(ConfigValidationError, match="\\[defaults\\] must be a table"):
        load_ingest_manifest(bad)

    bad.write_text("sources = []", encoding="utf-8")
    with pytest.raises(ConfigValidationError, match="at least one"):
        load_ingest_manifest(bad)

    bad.write_text(
        """
sources = "bad"
""".strip(),
        encoding="utf-8",
    )
    with pytest.raises(ConfigValidationError, match="\\[\\[sources\\]\\] must be provided"):
        load_ingest_manifest(bad)

    bad.write_text(
        """
sources = [1]
""".strip(),
        encoding="utf-8",
    )
    with pytest.raises(ConfigValidationError, match="entry 0 must be a table"):
        load_ingest_manifest(bad)

    bad.write_text(
        """
[[sources]]
source_ref = "x"
""".strip(),
        encoding="utf-8",
    )
    with pytest.raises(ConfigValidationError, match="sources\\[0\\].source is required"):
        load_ingest_manifest(bad)

    bad.write_text(
        """
[defaults]
write_raw = "no"
[[sources]]
source = "lever"
source_ref = "x"
""".strip(),
        encoding="utf-8",
    )
    with pytest.raises(ConfigValidationError, match="defaults.write_raw must be a boolean"):
        load_ingest_manifest(bad)

    bad.write_text(
        """
[defaults]
timeout_seconds = 0
[[sources]]
source = "lever"
source_ref = "x"
""".strip(),
        encoding="utf-8",
    )
    with pytest.raises(ConfigValidationError, match="defaults.timeout_seconds must be >= 0.1"):
        load_ingest_manifest(bad)

    bad.write_text(
        """
[defaults]
max_pages = 0
[[sources]]
source = "lever"
source_ref = "x"
""".strip(),
        encoding="utf-8",
    )
    with pytest.raises(ConfigValidationError, match="defaults.max_pages must be >= 1"):
        load_ingest_manifest(bad)

    bad.write_text(
        """
[defaults]
max_retries = -1
[[sources]]
source = "lever"
source_ref = "x"
""".strip(),
        encoding="utf-8",
    )
    with pytest.raises(ConfigValidationError, match="defaults.max_retries must be >= 0"):
        load_ingest_manifest(bad)

    bad.write_text(
        """
[defaults]
state_file = 1
[[sources]]
source = "lever"
source_ref = "x"
""".strip(),
        encoding="utf-8",
    )
    with pytest.raises(ConfigValidationError, match="defaults.state_file must be a string path"):
        load_ingest_manifest(bad)

    bad.write_text(
        """
[defaults]
max_jobs = "x"
[[sources]]
source = "lever"
source_ref = "x"
""".strip(),
        encoding="utf-8",
    )
    with pytest.raises(ConfigValidationError, match="defaults.max_jobs must be an integer"):
        load_ingest_manifest(bad)

    bad.write_text(
        """
[[sources]]
source = "lever"
source_ref = "x"
max_jobs = "x"
""".strip(),
        encoding="utf-8",
    )
    with pytest.raises(
        ConfigValidationError,
        match="sources\\[0\\].max_jobs must be an integer",
    ):
        load_ingest_manifest(bad)

    bad.write_text(
        """
[defaults]
base_backoff_seconds = "x"
[[sources]]
source = "lever"
source_ref = "x"
""".strip(),
        encoding="utf-8",
    )
    with pytest.raises(
        ConfigValidationError,
        match="defaults.base_backoff_seconds must be numeric",
    ):
        load_ingest_manifest(bad)

    bad.write_text(
        """
[[sources]]
source = "bad"
source_ref = "x"
""".strip(),
        encoding="utf-8",
    )
    with pytest.raises(ConfigValidationError, match="must be one of"):
        load_ingest_manifest(bad)

    bad.write_text(
        """
[[sources]]
source = "lever"
""".strip(),
        encoding="utf-8",
    )
    with pytest.raises(ConfigValidationError, match="source_ref is required"):
        load_ingest_manifest(bad)

    bad.write_text(
        """
[[sources]]
source = "lever"
source_ref = " "
""".strip(),
        encoding="utf-8",
    )
    with pytest.raises(ConfigValidationError, match="must be non-empty"):
        load_ingest_manifest(bad)

    bad.write_text(
        """
[[sources]]
source = "lever"
source_ref = "x"
enabled = "yes"
""".strip(),
        encoding="utf-8",
    )
    with pytest.raises(ConfigValidationError, match="sources\\[0\\].enabled must be a boolean"):
        load_ingest_manifest(bad)

    bad.write_text(
        """
[[sources]]
source = "lever"
source_ref = "x"
write_raw = "bad"
""".strip(),
        encoding="utf-8",
    )
    with pytest.raises(
        ConfigValidationError,
        match="sources\\[0\\].write_raw must be a boolean",
    ):
        load_ingest_manifest(bad)

    bad.write_text(
        """
[[sources]]
source = "lever"
source_ref = "x"
max_pages = 0
""".strip(),
        encoding="utf-8",
    )
    with pytest.raises(ConfigValidationError, match="sources\\[0\\].max_pages must be >= 1"):
        load_ingest_manifest(bad)

    bad.write_text(
        """
[[sources]]
source = "lever"
source_ref = "x"
max_pages = "x"
""".strip(),
        encoding="utf-8",
    )
    with pytest.raises(
        ConfigValidationError,
        match="sources\\[0\\].max_pages must be an integer",
    ):
        load_ingest_manifest(bad)

    bad.write_text(
        """
[[sources]]
source = "lever"
source_ref = "x"
max_retries = -1
""".strip(),
        encoding="utf-8",
    )
    with pytest.raises(ConfigValidationError, match="sources\\[0\\].max_retries must be >= 0"):
        load_ingest_manifest(bad)

    bad.write_text(
        """
[[sources]]
source = "lever"
source_ref = "x"
base_backoff_seconds = -1
""".strip(),
        encoding="utf-8",
    )
    with pytest.raises(
        ConfigValidationError,
        match="sources\\[0\\].base_backoff_seconds must be >= 0.0",
    ):
        load_ingest_manifest(bad)

    bad.write_text(
        """
[[sources]]
source = "lever"
source_ref = "x"
timeout_seconds = "x"
""".strip(),
        encoding="utf-8",
    )
    with pytest.raises(
        ConfigValidationError,
        match="sources\\[0\\].timeout_seconds must be numeric",
    ):
        load_ingest_manifest(bad)

    bad.write_text(
        """
[[sources]]
source = "lever"
source_ref = "x"
user_agent = 1
""".strip(),
        encoding="utf-8",
    )
    with pytest.raises(ConfigValidationError, match="sources\\[0\\].user_agent must be a string"):
        load_ingest_manifest(bad)

    bad.write_text(
        """
[[sources]]
source = "lever"
source_ref = "x"
output_parquet = 1
""".strip(),
        encoding="utf-8",
    )
    with pytest.raises(
        ConfigValidationError,
        match="sources\\[0\\].output_parquet must be a string path",
    ):
        load_ingest_manifest(bad)

    bad.write_text(
        """
[[sources]]
source = "lever"
source_ref = "x"
unknown = 1
""".strip(),
        encoding="utf-8",
    )
    with pytest.raises(ConfigValidationError, match="unknown keys in \\[\\[sources\\]\\] 0"):
        load_ingest_manifest(bad)

    monkeypatch.setattr(
        importlib.import_module("honestroles.ingest.manifest").tomllib,
        "loads",
        lambda _text: [],
    )
    bad.write_text("x = 1", encoding="utf-8")
    with pytest.raises(ConfigValidationError, match="root must be a table"):
        load_ingest_manifest(bad)


def test_http_getter_wrapper_and_callbacks(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    original_fetch_json = ingest_http.fetch_json

    def fake_fetch_json(url: str, **kwargs: Any) -> dict[str, bool]:
        captured["url"] = url
        captured["kwargs"] = kwargs
        callback = kwargs["on_request"]
        callback(200, False)
        return {"ok": True}

    monkeypatch.setattr(ingest_http, "fetch_json", fake_fetch_json)
    events: list[tuple[int | None, bool]] = []
    getter = ingest_http.build_http_getter(
        timeout_seconds=3.0,
        max_retries=4,
        base_backoff_seconds=0.5,
        user_agent="ua",
        on_request=lambda code, retry: events.append((code, retry)),
    )
    assert getter("https://example.com") == {"ok": True}
    assert captured["url"] == "https://example.com"
    kwargs = captured["kwargs"]
    assert kwargs["timeout_seconds"] == 3.0
    assert kwargs["max_retries"] == 4
    assert kwargs["base_backoff_seconds"] == 0.5
    assert kwargs["headers"]["User-Agent"] == "ua"
    assert events == [(200, False)]
    monkeypatch.setattr(ingest_http, "fetch_json", original_fetch_json)

    callback_events: list[tuple[int | None, bool]] = []
    monkeypatch.setattr(
        ingest_http.request,
        "urlopen",
        lambda _req, timeout=0: _DummyResponse("{}"),
    )
    ingest_http.fetch_json(
        "https://ok.example.com",
        on_request=lambda code, retry: callback_events.append((code, retry)),
    )
    assert callback_events == [(200, False)]

    def http_fail(_req, timeout=0):
        raise error.HTTPError(
            "https://bad",
            400,
            "bad",
            hdrs=None,
            fp=_DummyResponse("x"),
        )

    callback_events.clear()
    monkeypatch.setattr(ingest_http.request, "urlopen", http_fail)
    with pytest.raises(HonestRolesError, match="HTTP 400"):
        ingest_http.fetch_json(
            "https://bad.example.com",
            max_retries=0,
            on_request=lambda code, retry: callback_events.append((code, retry)),
        )
    assert callback_events == [(400, False)]

    def url_fail(_req, timeout=0):
        raise error.URLError("down")

    callback_events.clear()
    monkeypatch.setattr(ingest_http.request, "urlopen", url_fail)
    monkeypatch.setattr(ingest_http.time, "sleep", lambda _s: None)
    with pytest.raises(HonestRolesError, match="down"):
        ingest_http.fetch_json(
            "https://down.example.com",
            max_retries=0,
            on_request=lambda code, retry: callback_events.append((code, retry)),
        )
    assert callback_events == [(None, False)]


def test_ingest_service_v2_helpers_and_batch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    telemetry = ingest_service._HttpTelemetry()
    telemetry.observe(200, False)
    telemetry.observe(None, True)
    assert telemetry.http_status_counts["200"] == 1
    assert telemetry.http_status_counts["network_error"] == 1
    assert telemetry.retry_count == 1

    observed_policy: dict[str, Any] = {}

    def fake_builder(**kwargs: Any):
        observed_policy.update(kwargs)
        return lambda _url: {"ok": True}

    def truncated_fetcher(
        source_ref: str, *, max_pages: int, max_jobs: int, http_get_json
    ) -> tuple[list[dict[str, Any]], int]:
        assert source_ref == "stripe"
        assert http_get_json("https://policy.example.com") == {"ok": True}
        return (
            [{"id": "1", "title": "A", "absolute_url": "https://x/1"}],
            max_pages,
        )

    monkeypatch.setattr(ingest_service, "build_http_getter", fake_builder)
    original_fetcher = ingest_service._SOURCE_FETCHERS["greenhouse"]
    ingest_service._SOURCE_FETCHERS["greenhouse"] = truncated_fetcher
    try:
        result = ingest_service.sync_source(
            source="greenhouse",
            source_ref="stripe",
            output_parquet=tmp_path / "latest.parquet",
            report_file=tmp_path / "sync_report.json",
            state_file=tmp_path / "state.json",
            timeout_seconds=2.0,
            max_retries=5,
            base_backoff_seconds=0.75,
            user_agent="ua-v2",
            max_pages=1,
            max_jobs=100,
        )
    finally:
        ingest_service._SOURCE_FETCHERS["greenhouse"] = original_fetcher

    assert result.report.coverage_complete is False
    assert "INGEST_TRUNCATED" in result.check_codes
    assert result.snapshot_file is not None and result.snapshot_file.exists()
    assert result.catalog_file is not None and result.catalog_file.exists()
    assert result.state_file is not None and result.state_file.exists()
    assert observed_policy["timeout_seconds"] == 2.0
    assert observed_policy["max_retries"] == 5
    assert observed_policy["base_backoff_seconds"] == 0.75
    assert observed_policy["user_agent"] == "ua-v2"
    assert callable(observed_policy["on_request"])

    def empty_fetcher(
        _source_ref: str, *, max_pages: int, max_jobs: int, http_get_json
    ) -> tuple[list[dict[str, Any]], int]:
        return ([], 0)

    ingest_service._SOURCE_FETCHERS["greenhouse"] = empty_fetcher
    try:
        empty_result = ingest_service.sync_source(
            source="greenhouse",
            source_ref="stripe",
            output_parquet=tmp_path / "latest_empty.parquet",
            report_file=tmp_path / "sync_report_empty.json",
            state_file=tmp_path / "state_empty.json",
            max_pages=2,
            max_jobs=2,
        )
    finally:
        ingest_service._SOURCE_FETCHERS["greenhouse"] = original_fetcher
    assert empty_result.rows_written == 0
    catalog_frame = pl.read_parquet(tmp_path / "catalog.parquet")
    assert "stable_key" in catalog_frame.columns

    with pytest.raises(ConfigValidationError, match="timeout-seconds"):
        ingest_service.sync_source(
            source="lever",
            source_ref="ok",
            timeout_seconds=0,
        )
    with pytest.raises(ConfigValidationError, match="max-retries"):
        ingest_service.sync_source(
            source="lever",
            source_ref="ok",
            max_retries=-1,
        )
    with pytest.raises(ConfigValidationError, match="base-backoff-seconds"):
        ingest_service.sync_source(
            source="lever",
            source_ref="ok",
            base_backoff_seconds=-0.1,
        )
    with pytest.raises(ConfigValidationError, match="user-agent must be non-empty"):
        ingest_service.sync_source(
            source="lever",
            source_ref="ok",
            user_agent=" ",
        )

    row = {
        "source": "lever",
        "source_job_id": "1",
        "source_payload_hash": "new",
        "posted_at": "2026-01-01T00:00:00+00:00",
        "source_updated_at": "2026-01-03T00:00:00+00:00",
        "title": "Role",
        "company": "C",
        "location": "L",
    }
    key = dedup_key(row)
    old = {
        "stable_key": key,
        "first_seen_at_utc": "2026-01-01T00:00:00+00:00",
        "last_seen_at_utc": "2026-01-01T00:00:00+00:00",
        "is_active": True,
        "last_payload_hash": "old",
        "latest_posted_at": "2026-01-01T00:00:00+00:00",
        "latest_updated_at": "2026-01-01T00:00:00+00:00",
        "latest_record_json": json.dumps(row, sort_keys=True),
    }
    stale = {
        "stable_key": "source-id:lever::gone",
        "first_seen_at_utc": "2026-01-01T00:00:00+00:00",
        "last_seen_at_utc": "2026-01-01T00:00:00+00:00",
        "is_active": False,
        "last_payload_hash": "gone",
        "latest_posted_at": None,
        "latest_updated_at": None,
        "latest_record_json": json.dumps({"id": "gone"}),
    }
    tombstone_candidate = {
        "stable_key": "source-id:lever::tombstone",
        "first_seen_at_utc": "2026-01-01T00:00:00+00:00",
        "last_seen_at_utc": "2026-01-01T00:00:00+00:00",
        "is_active": True,
        "last_payload_hash": "old",
        "latest_posted_at": None,
        "latest_updated_at": None,
        "latest_record_json": json.dumps({"id": "t"}),
    }
    missing_key = {"stable_key": " ", "is_active": True}
    catalog, summary = ingest_service._apply_catalog_updates(
        catalog=[old, stale, tombstone_candidate, missing_key],
        records=[row],
        seen_at_utc="2026-01-04T00:00:00+00:00",
        coverage_complete=True,
    )
    assert summary.updated_count == 1
    assert summary.tombstoned_count == 1
    assert len(catalog) == 3

    active = ingest_service._active_records_from_catalog(
        [
            {"is_active": False, "latest_record_json": json.dumps({"id": "x"})},
            {"is_active": True, "latest_record_json": None},
            {"is_active": True, "latest_record_json": "{"},
            {"is_active": True, "latest_record_json": json.dumps([1, 2])},
            {
                "is_active": True,
                "latest_record_json": json.dumps(
                    {"source": "lever", "source_job_id": "2"}
                ),
            },
        ]
    )
    assert active == [{"source": "lever", "source_job_id": "2"}]
    assert ingest_service._is_coverage_complete(
        request_count=2,
        max_pages=2,
        fetched_count=1,
        max_jobs=10,
    ) is False
    assert ingest_service._is_coverage_complete(
        request_count=1,
        max_pages=5,
        fetched_count=10,
        max_jobs=10,
    ) is False
    assert ingest_service._is_coverage_complete(
        request_count=1,
        max_pages=5,
        fetched_count=1,
        max_jobs=10,
    ) is True
    assert ingest_service._text_or_none(None) is None
    ingest_service._write_catalog(tmp_path / "empty_catalog.parquet", [])
    empty_catalog = pl.read_parquet(tmp_path / "empty_catalog.parquet")
    assert empty_catalog.columns[0] == "stable_key"

    manifest_path = tmp_path / "manifest.toml"
    manifest_path.write_text(
        """
[defaults]
state_file = "state.json"
write_raw = false
max_pages = 3
max_jobs = 11
timeout_seconds = 4.0
max_retries = 1
base_backoff_seconds = 0.2
user_agent = "ua-default"

[[sources]]
source = "greenhouse"
source_ref = "stripe"
enabled = false

[[sources]]
source = "greenhouse"
source_ref = "stripe"
enabled = true
max_pages = 2

    [[sources]]
    source = "lever"
    source_ref = "netflix"
    enabled = true

    [[sources]]
    source = "ashby"
    source_ref = "notion"
    enabled = true
""".strip(),
        encoding="utf-8",
    )

    calls: list[dict[str, Any]] = []

    def fake_sync_source(**kwargs: Any) -> IngestionResult:
        calls.append(kwargs)
        if kwargs["source"] == "lever":
            raise HonestRolesError("boom")
        report = IngestionReport(
            schema_version=INGEST_SCHEMA_VERSION,
            status="pass",
            source="greenhouse",
            source_ref="stripe",
            started_at_utc="2026-01-01T00:00:00+00:00",
            finished_at_utc="2026-01-01T00:00:01+00:00",
            duration_ms=1000,
            request_count=2,
            fetched_count=3,
            normalized_count=3,
            dedup_dropped=0,
            high_watermark_before=None,
            high_watermark_after="2026-01-01T00:00:00+00:00",
            output_paths={"parquet": str(tmp_path / "jobs.parquet")},
        )
        return IngestionResult(
            report=report,
            output_parquet=tmp_path / "jobs.parquet",
            report_file=tmp_path / "sync.json",
            rows_written=2,
            check_codes=("INGEST_TRUNCATED",),
        )

    monkeypatch.setattr(ingest_service, "sync_source", fake_sync_source)
    batch = sync_sources_from_manifest(
        manifest_path=manifest_path,
        report_file=tmp_path / "batch_report.json",
        fail_fast=False,
    )
    assert batch.status == "fail"
    assert batch.pass_count == 2
    assert batch.fail_count == 1
    assert batch.total_rows_written == 4
    assert batch.check_codes == ("INGEST_TRUNCATED",)
    assert batch.report_file.exists()
    assert calls[0]["max_pages"] == 2
    assert calls[0]["max_jobs"] == 11
    assert calls[0]["timeout_seconds"] == 4.0
    assert len(calls) == 3

    calls.clear()
    batch = sync_sources_from_manifest(
        manifest_path=manifest_path,
        report_file=tmp_path / "batch_report_ff.json",
        fail_fast=True,
    )
    assert batch.status == "fail"
    assert len(batch.sources) == 2
    assert [call["source"] for call in calls] == ["greenhouse", "lever"]


def test_cli_and_lineage_paths_for_sync_all(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    manifest = tmp_path / "ingest.toml"
    manifest.write_text(
        """
[[sources]]
source = "lever"
source_ref = "netflix"
""".strip(),
        encoding="utf-8",
    )
    parser = importlib.import_module("honestroles.cli.parser").build_parser()
    parsed = parser.parse_args(
        [
            "ingest",
            "sync-all",
            "--manifest",
            str(manifest),
            "--fail-fast",
            "--format",
            "table",
        ]
    )
    assert parsed.ingest_command == "sync-all"
    assert parsed.fail_fast is True

    main_mod = importlib.import_module("honestroles.cli.main")
    monkeypatch.setattr(main_mod, "should_track", lambda _args: False)
    monkeypatch.setattr(
        main_mod,
        "handle_ingest_sync_all",
        lambda _args: handlers.CommandResult(
            payload={
                "schema_version": INGEST_SCHEMA_VERSION,
                "status": "pass",
                "total_sources": 1,
                "pass_count": 1,
                "fail_count": 0,
                "total_rows_written": 3,
                "total_fetched_count": 4,
                "total_request_count": 2,
                "sources": [
                    {
                        "source": "lever",
                        "source_ref": "netflix",
                        "status": "pass",
                        "rows_written": 3,
                        "fetched_count": 4,
                        "request_count": 2,
                    }
                ],
                "report_file": str(tmp_path / "batch_report.json"),
            },
            exit_code=0,
        ),
    )
    code = main_mod.main(
        [
            "ingest",
            "sync-all",
            "--manifest",
            str(manifest),
            "--format",
            "table",
        ]
    )
    assert code == 0
    rendered = capsys.readouterr().out
    assert "BATCH total_sources=1" in rendered
    assert "SOURCE       REF" in rendered
    assert "report_file" in rendered

    pass_result = BatchIngestionResult(
        schema_version=INGEST_SCHEMA_VERSION,
        status="pass",
        started_at_utc="2026-01-01T00:00:00+00:00",
        finished_at_utc="2026-01-01T00:00:01+00:00",
        duration_ms=1000,
        total_sources=1,
        pass_count=1,
        fail_count=0,
        total_rows_written=1,
        total_fetched_count=1,
        total_request_count=1,
        sources=(),
        report_file=tmp_path / "batch_report.json",
        check_codes=(),
    )
    fail_result = BatchIngestionResult(
        schema_version=INGEST_SCHEMA_VERSION,
        status="fail",
        started_at_utc="2026-01-01T00:00:00+00:00",
        finished_at_utc="2026-01-01T00:00:01+00:00",
        duration_ms=1000,
        total_sources=1,
        pass_count=0,
        fail_count=1,
        total_rows_written=0,
        total_fetched_count=0,
        total_request_count=0,
        sources=(),
        report_file=tmp_path / "batch_fail_report.json",
        check_codes=(),
    )
    monkeypatch.setattr(handlers, "sync_sources_from_manifest", lambda **_kwargs: pass_result)
    assert handlers.handle_ingest_sync_all(parsed).exit_code == 0
    monkeypatch.setattr(handlers, "sync_sources_from_manifest", lambda **_kwargs: fail_result)
    assert handlers.handle_ingest_sync_all(parsed).exit_code == 1

    assert lineage.should_track({"command": "ingest", "ingest_command": "sync-all"})
    input_hash, input_hashes, config_hash = lineage.compute_hashes(
        {
            "command": "ingest",
            "ingest_command": "sync-all",
            "manifest": str(manifest),
        }
    )
    assert input_hash is None
    assert "manifest" in input_hashes
    assert config_hash
    artifacts = lineage.build_artifact_paths(
        {
            "command": "ingest",
            "ingest_command": "sync-all",
        },
        payload=None,
    )
    assert artifacts["report_file"].endswith("dist/ingest/sync_all_report.json")


def test_state_helper_branches() -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    later = datetime(2026, 1, 2, tzinfo=UTC)
    assert ingest_state._max_dt(None, later) == later
    assert ingest_state._max_dt(now, None) == now
    assert ingest_state._max_dt(now, later) == later


def test_output_batch_and_normalize_work_mode_branches(
    capsys: pytest.CaptureFixture[str],
) -> None:
    output.emit_payload(
        {
            "status": "pass",
            "total_sources": 1,
            "pass_count": 1,
            "fail_count": 0,
            "total_rows_written": 1,
            "total_fetched_count": 1,
            "total_request_count": 1,
            "sources": [1],
        },
        "table",
    )
    rendered = capsys.readouterr().out
    assert "BATCH total_sources=1" in rendered
    assert "report_file" not in rendered
    output._print_ingest_batch_table(
        {
            "total_sources": 0,
            "pass_count": 0,
            "fail_count": 0,
            "total_rows_written": 0,
            "total_fetched_count": 0,
            "total_request_count": 0,
            "sources": {"bad": "shape"},
        }
    )
    assert "SOURCE       REF" in capsys.readouterr().out

    assert ingest_normalize._infer_work_mode("on-site") == "onsite"
    assert (
        ingest_normalize._normalize_work_mode("unknown", "Hybrid")
        == "hybrid"
    )
    assert ingest_normalize._normalize_work_mode(None, "Remote") == "remote"
