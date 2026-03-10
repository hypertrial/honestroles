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
    INGEST_SCHEMA_VERSION,
    IngestionReport,
    IngestionRequest,
    IngestionResult,
    IngestionStateEntry,
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
            "last_success_at_utc": "2026-01-02T00:00:00+00:00",
            "recent_source_job_ids": ["1", "", "2"],
        }
    )
    assert entry.recent_source_job_ids == ("1", "2")
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

    jobs, requests = fetch_greenhouse_jobs(
        "acme",
        max_pages=3,
        max_jobs=10,
        http_get_json=greenhouse_get,
    )
    assert len(jobs) == 2
    assert requests == 2

    with pytest.raises(ConfigValidationError, match="must include 'jobs'"):
        fetch_greenhouse_jobs("acme", max_pages=1, max_jobs=1, http_get_json=lambda _u: {})
    with pytest.raises(ConfigValidationError, match="must be an array"):
        fetch_greenhouse_jobs("acme", max_pages=1, max_jobs=1, http_get_json=lambda _u: {"jobs": {}})
    with pytest.raises(ConfigValidationError, match="non-empty"):
        fetch_greenhouse_jobs("", max_pages=1, max_jobs=1, http_get_json=lambda _u: {"jobs": []})
    capped_jobs, _ = fetch_greenhouse_jobs(
        "acme",
        max_pages=3,
        max_jobs=1,
        http_get_json=lambda _u: {"jobs": [{"id": 1}, {"id": 2}]},
    )
    assert len(capped_jobs) == 1
    exhausted_greenhouse_jobs, exhausted_greenhouse_requests = fetch_greenhouse_jobs(
        "acme",
        max_pages=2,
        max_jobs=10,
        http_get_json=lambda _u: {"jobs": [{"id": 1}]},
    )
    assert len(exhausted_greenhouse_jobs) == 2
    assert exhausted_greenhouse_requests == 2

    lever_calls = {"n": 0}

    def lever_get(_url: str) -> Any:
        lever_calls["n"] += 1
        if lever_calls["n"] == 1:
            return [{"id": "1"}]
        return []

    lever_jobs, lever_requests = fetch_lever_jobs(
        "acme",
        max_pages=3,
        max_jobs=10,
        http_get_json=lever_get,
    )
    assert len(lever_jobs) == 1
    assert lever_requests == 2
    lever_data_jobs, _ = fetch_lever_jobs(
        "acme",
        max_pages=1,
        max_jobs=1,
        http_get_json=lambda _u: {"data": [{"id": "1"}, {"id": "2"}]},
    )
    assert len(lever_data_jobs) == 1
    exhausted_lever_jobs, exhausted_lever_requests = fetch_lever_jobs(
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
    ashby_jobs, ashby_requests = fetch_ashby_jobs(
        "acme",
        max_pages=5,
        max_jobs=10,
        http_get_json=lambda _u: next(ashby_pages),
    )
    assert len(ashby_jobs) == 2
    assert ashby_requests == 2
    ashby_no_cursor_jobs, ashby_no_cursor_requests = fetch_ashby_jobs(
        "acme",
        max_pages=2,
        max_jobs=10,
        http_get_json=lambda _u: {"postings": [{"id": "1"}]},
    )
    assert len(ashby_no_cursor_jobs) == 1
    assert ashby_no_cursor_requests == 1
    ashby_empty_jobs, ashby_empty_requests = fetch_ashby_jobs(
        "acme",
        max_pages=2,
        max_jobs=10,
        http_get_json=lambda _u: {"results": []},
    )
    assert ashby_empty_jobs == []
    assert ashby_empty_requests == 1
    ashby_limited_jobs, _ = fetch_ashby_jobs(
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

    exhausted_ashby_jobs, exhausted_ashby_requests = fetch_ashby_jobs(
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

    workable_jobs, workable_requests = fetch_workable_jobs(
        "acme",
        max_pages=1,
        max_jobs=10,
        http_get_json=workable_get,
    )
    assert len(workable_jobs) == 1
    assert workable_requests == 3
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
    workable_results_jobs, _ = fetch_workable_jobs(
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


def test_normalize_records_and_dataframe_paths() -> None:
    now = datetime.now(UTC).isoformat()
    greenhouse = {"id": 1, "title": "T", "location": {"name": "Remote"}, "absolute_url": "https://a"}
    lever = {
        "id": "2",
        "text": "L",
        "categories": {"location": "NYC", "team": "Ops"},
        "description": "<p>x</p>",
        "hostedUrl": "https://b",
        "createdAt": 1_700_000_000_000,
    }
    ashby = {"jobId": "3", "jobTitle": "A", "jobUrl": "https://c", "publishedDate": now}
    workable = {"shortcode": "4", "title": "W", "url": "https://d", "location": {"location_str": "Hybrid"}}
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

    assert ingest_normalize._extract_lever(lever)["company"] == "Ops"
    assert ingest_normalize._extract_ashby(ashby)["title"] == "A"
    assert ingest_normalize._extract_workable(workable)["remote"] is False
    assert ingest_normalize._extract_workable(workable_text_location)["remote"] is True
    assert ingest_normalize._extract_generic(generic)["posted_at"] == "bad-time"
    assert ingest_normalize._ensure_metadata_columns(pl.DataFrame({"id": ["1"]})).height == 1
    assert ingest_normalize._coerce_float("x") is None
    assert ingest_normalize._coerce_float(1) == 1.0
    assert ingest_normalize._coerce_float(" ") is None
    assert ingest_normalize._coerce_timestamp("2026-01-01T00:00:00Z") is not None
    assert ingest_normalize._coerce_timestamp("2026-01-01T00:00:00").endswith("+00:00")
    assert ingest_normalize._coerce_timestamp("bad") == "bad"
    assert ingest_normalize._infer_remote("onsite") is False
    assert ingest_normalize._infer_remote("unknown") is None
    assert ingest_normalize._text_or_none(" ") is None

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

    filtered, before = ingest_state.filter_incremental(
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

    no_filter, _ = ingest_state.filter_incremental(
        [{"source_job_id": "1"}],
        entry=entry,
        full_refresh=True,
    )
    assert len(no_filter) == 1

    unchanged, watermark = ingest_state.filter_incremental(
        [{"source_job_id": "x"}],
        entry=IngestionStateEntry(),
        full_refresh=False,
    )
    assert len(unchanged) == 1
    assert watermark is None

    updated = ingest_state.update_state_entry(
        entry,
        records=[{"source_job_id": str(i), "posted_at": "2026-03-01T00:00:00+00:00"} for i in range(520)],
        finished_at_utc="2026-03-01T00:00:01+00:00",
    )
    assert len(updated.recent_source_job_ids) == 500
    assert updated.high_watermark_posted_at == "2026-03-01T00:00:00+00:00"
    updated_without_id = ingest_state.update_state_entry(
        IngestionStateEntry(),
        records=[{"posted_at": "2026-03-01T00:00:00+00:00"}],
        finished_at_utc="2026-03-01T00:00:01+00:00",
    )
    assert updated_without_id.recent_source_job_ids == ()

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
                {"id": "1", "title": "A", "absolute_url": "https://x/1", "updated_at": "2026-01-02T00:00:00+00:00"},
                {"id": "1", "title": "A", "absolute_url": "https://x/1?utm=1", "updated_at": "2026-01-02T00:00:00+00:00"},
                {"id": "2", "title": "B", "absolute_url": "https://x/2", "updated_at": "2026-01-03T00:00:00+00:00"},
            ],
            3,
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
    assert second.rows_written == 0

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
