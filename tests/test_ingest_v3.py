from __future__ import annotations

import argparse
from datetime import UTC, datetime, timedelta
import importlib
import json
from pathlib import Path
from time import perf_counter
from typing import Any

import pytest

from honestroles.cli import handlers, lineage, output
from honestroles.errors import ConfigValidationError, HonestRolesError
from honestroles.ingest import manifest as ingest_manifest
from honestroles.ingest import models as ingest_models
from honestroles.ingest import quality as ingest_quality
from honestroles.ingest import service as ingest_service
from honestroles.ingest.models import (
    INGEST_SCHEMA_VERSION,
    IngestionReport,
    IngestionResult,
    IngestionValidationResult,
)


def _record(*, source_job_id: str = "1", title: str = "Role", posted_at: str | None = None) -> dict[str, Any]:
    return {
        "id": source_job_id,
        "title": title,
        "company": "Acme",
        "location": "Remote",
        "remote": True,
        "description_text": "desc",
        "description_html": "<p>desc</p>",
        "skills": [],
        "salary_min": None,
        "salary_max": None,
        "apply_url": f"https://jobs.example/{source_job_id}",
        "posted_at": posted_at or datetime.now(UTC).isoformat(),
        "source": "greenhouse",
        "source_ref": "stripe",
        "source_job_id": source_job_id,
        "job_url": f"https://jobs.example/{source_job_id}",
        "ingested_at_utc": datetime.now(UTC).isoformat(),
        "source_payload_hash": f"hash-{source_job_id}",
        "source_updated_at": datetime.now(UTC).isoformat(),
    }


def test_quality_policy_loader_and_hash(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    policy, source, policy_hash = ingest_quality.load_ingest_quality_policy(None)
    assert source == "builtin"
    assert len(policy_hash) == 64
    assert policy.to_dict()["schema_version"] == ingest_quality.INGEST_QUALITY_POLICY_SCHEMA_VERSION

    policy_file = tmp_path / "ingest_quality.toml"
    policy_file.write_text(
        """
schema_version = "1.0"
min_rows = 2
required_columns = ["id", "title", "posted_at"]
location_or_remote_signal_min = 0.75
[null_thresholds]
title = 0.5
[freshness]
posted_at_max_age_days = 30
source_updated_at_max_age_days = 60
""".strip(),
        encoding="utf-8",
    )
    loaded, loaded_source, loaded_hash = ingest_quality.load_ingest_quality_policy(policy_file)
    assert loaded.min_rows == 2
    assert loaded.required_columns == ("id", "title", "posted_at")
    assert loaded.location_or_remote_signal_min == 0.75
    assert loaded_source == str(policy_file.resolve())
    assert len(loaded_hash) == 64

    with pytest.raises(ConfigValidationError, match="cannot read ingest quality policy"):
        ingest_quality.load_ingest_quality_policy(tmp_path / "missing.toml")

    broken = tmp_path / "broken.toml"
    broken.write_text("a = [", encoding="utf-8")
    with pytest.raises(ConfigValidationError, match="invalid TOML"):
        ingest_quality.load_ingest_quality_policy(broken)

    monkeypatch.setattr(ingest_quality.tomllib, "loads", lambda _text: ["bad"])
    with pytest.raises(ConfigValidationError, match="root must be a table"):
        ingest_quality.load_ingest_quality_policy(policy_file)


def test_quality_policy_validation_errors(tmp_path: Path) -> None:
    cases = [
        ('schema_version = "2.0"', "schema_version must be"),
        ('unknown = 1', "unknown keys"),
        ('min_rows = "x"', "min_rows must be an integer"),
        ("min_rows = 0", "min_rows must be >= 1"),
        ('required_columns = "x"', "required_columns must be an array"),
        ('required_columns = [""]', "required_columns must contain non-empty strings"),
        ("null_thresholds = 1", "null_thresholds must be a table"),
        ('[null_thresholds]\na = "x"', "null_thresholds.a must be numeric"),
        ('[null_thresholds]\na = -1', "null_thresholds.a must be within"),
        ("freshness = 1", "freshness must be a table"),
        ('[freshness]\nunknown = 1', "unknown freshness keys"),
        ('[freshness]\nposted_at_max_age_days = "x"', "freshness.posted_at_max_age_days must be integer"),
        ('[freshness]\nposted_at_max_age_days = -1', "freshness.posted_at_max_age_days must be >= 0"),
        ('location_or_remote_signal_min = "x"', "location_or_remote_signal_min must be numeric"),
        ("location_or_remote_signal_min = 2", "location_or_remote_signal_min must be within"),
    ]
    for index, (body, message) in enumerate(cases):
        path = tmp_path / f"case-{index}.toml"
        path.write_text(body, encoding="utf-8")
        with pytest.raises(ConfigValidationError, match=message):
            ingest_quality.load_ingest_quality_policy(path)


def test_quality_evaluator_and_helpers() -> None:
    now = datetime(2026, 3, 10, tzinfo=UTC)
    policy = ingest_quality.IngestQualityPolicy(
        min_rows=2,
        required_columns=("id", "title", "posted_at", "source_updated_at"),
        null_thresholds={"title": 0.25},
        posted_at_max_age_days=10,
        source_updated_at_max_age_days=10,
    )
    recent = (now - timedelta(days=1)).isoformat()
    stale = (now - timedelta(days=40)).isoformat()
    records = [
        _record(source_job_id="1", title="", posted_at=stale),
        _record(source_job_id="2", title="ok", posted_at="not-a-date"),
    ]
    records[0]["source_updated_at"] = " "
    records[1]["source_updated_at"] = recent
    result = ingest_quality.evaluate_ingest_quality(records, policy=policy, now_utc=now)
    assert result.status == "warn"
    assert "INGEST_QUALITY_NULL_RATE_TITLE" in result.check_codes
    assert "INGEST_QUALITY_POSTED_AT_PARSEABLE" in result.check_codes
    assert result.summary["warn"] > 0

    none_fresh_policy = ingest_quality.IngestQualityPolicy(
        posted_at_max_age_days=None,
        source_updated_at_max_age_days=None,
    )
    none_fresh = ingest_quality.evaluate_ingest_quality(
        [_record()], policy=none_fresh_policy, now_utc=now
    )
    assert "INGEST_QUALITY_POSTED_AT_FRESHNESS" not in none_fresh.check_codes

    signal_warn = ingest_quality.evaluate_ingest_quality(
        [
            {
                "id": "1",
                "title": "t",
                "apply_url": "https://jobs.example/1",
                "source": "greenhouse",
                "source_ref": "stripe",
                "source_job_id": "1",
                "source_payload_hash": "h1",
                "location": None,
                "remote": None,
                "work_mode": "unknown",
            }
        ],
        policy=ingest_quality.IngestQualityPolicy(
            required_columns=(),
            null_thresholds={},
            location_or_remote_signal_min=0.85,
            posted_at_max_age_days=None,
            source_updated_at_max_age_days=None,
        ),
        now_utc=now,
    )
    assert "INGEST_QUALITY_LOCATION_OR_REMOTE_SIGNAL" in signal_warn.check_codes

    empty_parse = ingest_quality.evaluate_ingest_quality(
        [{"posted_at": " ", "source_updated_at": " "}],
        policy=ingest_quality.IngestQualityPolicy(
            required_columns=(),
            null_thresholds={},
            posted_at_max_age_days=1,
            source_updated_at_max_age_days=1,
        ),
        now_utc=now,
    )
    assert "INGEST_QUALITY_POSTED_AT_FRESHNESS" in empty_parse.check_codes
    assert ingest_quality._is_missing(None) is True
    assert ingest_quality._is_missing(" ") is True
    assert ingest_quality._is_missing(1) is False
    assert ingest_quality._parse_datetime(" ") is None
    assert ingest_quality._parse_datetime("not-date") is None
    assert ingest_quality._parse_datetime("2026-01-01T00:00:00Z") is not None
    assert ingest_quality._text_or_none(" ") is None
    assert ingest_quality._text_or_none(123) == "123"
    check = ingest_quality._quality_check(
        check_id="q", code="CODE", ok=False, message="m", fix="f"
    )
    assert check["status"] == "warn"
    assert check["fix"] == "f"
    assert ingest_quality._parse_datetime("2026-01-01T00:00:00") is not None
    with pytest.raises(ConfigValidationError, match="null_thresholds keys"):
        ingest_quality._parse_null_thresholds({1: 0.1})  # type: ignore[arg-type]
    assert ingest_quality._parse_freshness(None)["posted_at_max_age_days"] is not None
    parsed_freshness = ingest_quality._parse_freshness({"posted_at_max_age_days": None})
    assert parsed_freshness["posted_at_max_age_days"] is not None
    assert ingest_quality._parse_ratio(0.5, field_name="x", default=0.1) == 0.5


def test_quality_fail_status_branch(monkeypatch: pytest.MonkeyPatch) -> None:
    original = ingest_quality._quality_check
    call_counter = {"n": 0}

    def fake_quality_check(**kwargs: Any) -> dict[str, Any]:
        call_counter["n"] += 1
        out = original(**kwargs)
        if call_counter["n"] == 1:
            out["status"] = "fail"
            out["severity"] = "fail"
        if call_counter["n"] == 2:
            out["status"] = "unknown"
        return out

    monkeypatch.setattr(ingest_quality, "_quality_check", fake_quality_check)
    result = ingest_quality.evaluate_ingest_quality(
        [_record()],
        policy=ingest_quality.IngestQualityPolicy(required_columns=("id",), null_thresholds={}),
        now_utc=datetime.now(UTC),
    )
    assert result.status == "fail"


def test_quality_check_codes_are_unique_when_duplicate_codes_emitted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = ingest_quality._quality_check

    def fake_quality_check(**kwargs: Any) -> dict[str, Any]:
        out = original(**kwargs)
        out["status"] = "warn"
        out["severity"] = "warn"
        out["code"] = "DUPLICATE_CODE"
        return out

    monkeypatch.setattr(ingest_quality, "_quality_check", fake_quality_check)
    result = ingest_quality.evaluate_ingest_quality(
        [_record()],
        policy=ingest_quality.IngestQualityPolicy(required_columns=("id",), null_thresholds={}),
        now_utc=datetime.now(UTC),
    )
    assert result.check_codes == ("DUPLICATE_CODE",)


def test_sync_source_strict_quality_gate_and_validate(tmp_path: Path) -> None:
    def fetcher(
        _source_ref: str, *, max_pages: int, max_jobs: int, http_get_json
    ) -> tuple[list[dict[str, Any]], int, tuple[str, ...]]:
        return ([{"id": "1", "absolute_url": "https://jobs/1"}], 1, ())

    original = ingest_service._SOURCE_FETCHERS["greenhouse"]
    ingest_service._SOURCE_FETCHERS["greenhouse"] = fetcher
    try:
        warn_policy = tmp_path / "warn_policy.toml"
        warn_policy.write_text(
            """
schema_version = "1.0"
min_rows = 1
[null_thresholds]
title = 0.0
""".strip(),
            encoding="utf-8",
        )
        strict = ingest_service.sync_source(
            source="greenhouse",
            source_ref="stripe",
            output_parquet=tmp_path / "latest.parquet",
            report_file=tmp_path / "strict_report.json",
            state_file=tmp_path / "state.json",
            strict_quality=True,
            quality_policy_file=None,
            write_raw=True,
        )
        assert strict.report.status == "fail"
        assert not (tmp_path / "latest.parquet").exists()
        assert strict.report_file.exists()
        assert strict.raw_file is not None and strict.raw_file.exists()
        assert "INGEST_QUALITY_NULL_RATE_POSTED_AT" in strict.check_codes

        strict_no_raw = ingest_service.sync_source(
            source="greenhouse",
            source_ref="stripe",
            output_parquet=tmp_path / "latest-no-raw.parquet",
            report_file=tmp_path / "strict-no-raw-report.json",
            state_file=tmp_path / "state-no-raw.json",
            strict_quality=True,
            quality_policy_file=None,
            write_raw=False,
        )
        assert strict_no_raw.report.status == "fail"
        assert strict_no_raw.raw_file is None

        validate_warn = ingest_service.validate_ingestion_source(
            source="greenhouse",
            source_ref="stripe",
            report_file=tmp_path / "validate_report.json",
            strict_quality=False,
            write_raw=True,
            quality_policy_file=warn_policy,
        )
        assert validate_warn.report.status == "warn"
        assert validate_warn.report_file.exists()
        assert validate_warn.raw_file is not None and validate_warn.raw_file.exists()

        pass_policy = tmp_path / "pass_policy.toml"
        pass_policy.write_text(
            """
schema_version = "1.0"
min_rows = 1
required_columns = ["id"]
null_thresholds = {}
location_or_remote_signal_min = 0.0
[freshness]
posted_at_max_age_days = 9999
source_updated_at_max_age_days = 9999
""".strip(),
            encoding="utf-8",
        )
        ingest_service._SOURCE_FETCHERS["greenhouse"] = (
            lambda _source_ref, *, max_pages, max_jobs, http_get_json: (
                [
                    {
                        "id": "2",
                        "title": "Engineer",
                        "absolute_url": "https://jobs/2",
                        "updated_at": datetime.now(UTC).isoformat(),
                    }
                ],
                1,
                (),
            )
        )
        validate_pass = ingest_service.validate_ingestion_source(
            source="greenhouse",
            source_ref="stripe",
            report_file=tmp_path / "validate_pass_report.json",
            strict_quality=False,
            quality_policy_file=pass_policy,
        )
        assert validate_pass.report.status == "pass"
        ingest_service._SOURCE_FETCHERS["greenhouse"] = fetcher

        validate_fail = ingest_service.validate_ingestion_source(
            source="greenhouse",
            source_ref="stripe",
            report_file=tmp_path / "validate_strict_report.json",
            strict_quality=True,
        )
        assert validate_fail.report.status == "fail"
    finally:
        ingest_service._SOURCE_FETCHERS["greenhouse"] = original


@pytest.mark.parametrize(
    ("source", "raw_record"),
    (
        (
            "greenhouse",
            {
                "id": "g-1",
                "title": "Engineer",
                "absolute_url": "https://jobs.example/g-1",
                "company_name": "Stripe",
                "first_published": "2026-01-01T00:00:00Z",
                "updated_at": "2026-01-01T00:00:00Z",
                "content": "<p>desc</p>",
                "location": {"name": "Remote"},
            },
        ),
        (
            "lever",
            {
                "id": "l-1",
                "text": "Engineer",
                "hostedUrl": "https://jobs.lever.co/acme/l-1",
                "categories": {"location": "Remote", "team": "Eng"},
                "workplaceType": "remote",
                "descriptionPlain": "desc",
                "createdAt": 1767225600000,
            },
        ),
        (
            "ashby",
            {
                "id": "a-1",
                "title": "Engineer",
                "jobUrl": "https://jobs.ashbyhq.com/acme/a-1",
                "publishedAt": "2026-01-01T00:00:00Z",
                "descriptionPlain": "desc",
                "isRemote": True,
                "workplaceType": "remote",
            },
        ),
        (
            "workable",
            {
                "code": "w-1",
                "title": "Engineer",
                "url": "https://jobs.workable.com/acme/j/w-1",
                "application_url": "https://apply.workable.com/acme/j/w-1",
                "published_on": "2026-01-01T00:00:00Z",
                "description": "<p>desc</p>",
                "city": "Athens",
                "country": "GR",
                "telecommuting": True,
            },
        ),
    ),
)
def test_sync_source_strict_quality_passes_with_smoke_policy(
    tmp_path: Path,
    source: str,
    raw_record: dict[str, Any],
) -> None:
    original = ingest_service._SOURCE_FETCHERS[source]
    ingest_service._SOURCE_FETCHERS[source] = (
        lambda _source_ref, *, max_pages, max_jobs, http_get_json: ([raw_record], 1, ())
    )
    policy_file = Path(__file__).resolve().parents[1] / "ingest_quality_smoke.toml"
    try:
        result = ingest_service.sync_source(
            source=source,
            source_ref="acme",
            output_parquet=tmp_path / f"{source}.parquet",
            report_file=tmp_path / f"{source}.json",
            state_file=tmp_path / f"{source}.state.json",
            strict_quality=True,
            quality_policy_file=policy_file,
            full_refresh=True,
        )
    finally:
        ingest_service._SOURCE_FETCHERS[source] = original
    assert result.report.status == "pass"
    assert result.report.quality_status == "pass"
    assert result.report.key_field_completeness["company_non_null_pct"] >= 95.0
    assert result.report.key_field_completeness["posted_at_non_null_pct"] >= 95.0


def test_sync_source_page_repeat_warning_marks_coverage_incomplete(tmp_path: Path) -> None:
    def fetcher(
        _source_ref: str, *, max_pages: int, max_jobs: int, http_get_json
    ) -> tuple[list[dict[str, Any]], int, tuple[str, ...]]:
        return (
            [
                {
                    "id": "1",
                    "title": "Engineer",
                    "absolute_url": "https://jobs.example/search?gh_jid=1",
                    "updated_at": datetime.now(UTC).isoformat(),
                }
            ],
            1,
            ("INGEST_PAGE_REPEAT_DETECTED",),
        )

    original = ingest_service._SOURCE_FETCHERS["greenhouse"]
    ingest_service._SOURCE_FETCHERS["greenhouse"] = fetcher
    try:
        result = ingest_service.sync_source(
            source="greenhouse",
            source_ref="stripe",
            output_parquet=tmp_path / "latest.parquet",
            report_file=tmp_path / "report.json",
            state_file=tmp_path / "state.json",
            max_pages=10,
            max_jobs=100,
            full_refresh=True,
            quality_policy_file=None,
        )
    finally:
        ingest_service._SOURCE_FETCHERS["greenhouse"] = original

    assert result.report.coverage_complete is False
    assert "INGEST_PAGE_REPEAT_DETECTED" in result.report.warnings
    assert "INGEST_TRUNCATED" in result.report.warnings


def test_validate_ingestion_source_failure_writes_report(tmp_path: Path) -> None:
    with pytest.raises(ConfigValidationError):
        ingest_service.validate_ingestion_source(
            source="greenhouse",
            source_ref="stripe",
            report_file=tmp_path / "validate_fail.json",
            quality_policy_file=tmp_path / "missing_policy.toml",
        )
    payload = json.loads((tmp_path / "validate_fail.json").read_text(encoding="utf-8"))
    assert payload["status"] == "fail"


def test_service_private_branches_and_merge_policy(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(HonestRolesError, match="invalid source fetcher result"):
        original = ingest_service._SOURCE_FETCHERS["greenhouse"]
        ingest_service._SOURCE_FETCHERS["greenhouse"] = lambda *_a, **_k: "bad"  # type: ignore[assignment]
        try:
            ingest_service._fetch_source_records(
                source="greenhouse",
                source_ref="x",
                max_pages=1,
                max_jobs=1,
                fetch_fn=lambda _url: {},
            )
        finally:
            ingest_service._SOURCE_FETCHERS["greenhouse"] = original

    for kwargs, msg in [
        ({"merge_policy": "bad"}, "merge-policy"),
        ({"retain_snapshots": 0}, "retain-snapshots"),
        ({"prune_inactive_days": -1}, "prune-inactive-days"),
    ]:
        base = dict(
            source="greenhouse",
            source_ref="stripe",
            max_pages=1,
            max_jobs=1,
            timeout_seconds=1.0,
            max_retries=0,
            base_backoff_seconds=0.0,
            user_agent="ua",
            merge_policy="updated_hash",
            retain_snapshots=1,
            prune_inactive_days=0,
        )
        base.update(kwargs)
        with pytest.raises(ConfigValidationError, match=msg):
            ingest_service._validate_inputs(**base)

    existing = {
        "last_payload_hash": "b",
        "latest_posted_at": "2026-01-01T00:00:00+00:00",
        "latest_updated_at": "2026-01-01T00:00:00+00:00",
    }
    assert ingest_service._should_replace_record(
        existing=existing,
        incoming_payload_hash="a",
        incoming_posted_at="2026-01-02T00:00:00+00:00",
        incoming_updated_at="2026-01-02T00:00:00+00:00",
        merge_policy="first_seen",
    ) is False
    assert ingest_service._should_replace_record(
        existing=existing,
        incoming_payload_hash="a",
        incoming_posted_at=None,
        incoming_updated_at=None,
        merge_policy="last_seen",
    ) is True

    assert ingest_service._compare_optional_datetimes(None, None) == 0
    assert ingest_service._compare_optional_datetimes(None, datetime.now(UTC)) == -1
    assert ingest_service._compare_optional_datetimes(datetime.now(UTC), None) == 1
    now = datetime.now(UTC)
    assert ingest_service._compare_optional_datetimes(now, now) == 0

    assert ingest_service._parse_iso(None) is None
    assert ingest_service._parse_iso(" ") is None
    assert ingest_service._parse_iso("not-date") is None
    assert ingest_service._parse_iso("2026-01-01T00:00:00") is not None
    assert ingest_service._parse_iso("2026-01-01T00:00:00Z") is not None

    # Exercise compaction branch where row remains because timestamp cannot be parsed.
    catalog, summary = ingest_service._apply_catalog_updates(
        catalog=[
            {
                "stable_key": "k1",
                "first_seen_at_utc": "2026-01-01T00:00:00+00:00",
                "last_seen_at_utc": "bad",
                "is_active": False,
                "last_payload_hash": "x",
                "latest_posted_at": None,
                "latest_updated_at": None,
                "latest_record_json": json.dumps({"id": "x"}),
            }
        ],
        records=[],
        seen_at_utc="2026-03-01T00:00:00+00:00",
        coverage_complete=True,
        prune_inactive_days=1,
    )
    assert len(catalog) == 1
    assert summary.pruned_inactive_count == 0

    # Exercise prune skip branches.
    catalog_no_prune, _ = ingest_service._apply_catalog_updates(
        catalog=[{"stable_key": "k2", "is_active": False, "last_seen_at_utc": "2026-01-01T00:00:00+00:00"}],
        records=[],
        seen_at_utc="bad-date",
        coverage_complete=False,
        prune_inactive_days=2,
    )
    assert len(catalog_no_prune) == 1
    catalog_prune_disabled, _ = ingest_service._apply_catalog_updates(
        catalog=[{"stable_key": "k3", "is_active": False, "last_seen_at_utc": "2026-01-01T00:00:00+00:00"}],
        records=[],
        seen_at_utc="2026-03-01T00:00:00+00:00",
        coverage_complete=False,
        prune_inactive_days=-1,
    )
    assert len(catalog_prune_disabled) == 1

    # Exercise prune removal branch.
    catalog_pruned, summary_pruned = ingest_service._apply_catalog_updates(
        catalog=[
            {
                "stable_key": "k4",
                "is_active": False,
                "last_seen_at_utc": "2026-01-01T00:00:00+00:00",
                "first_seen_at_utc": "2026-01-01T00:00:00+00:00",
                "last_payload_hash": "x",
                "latest_posted_at": None,
                "latest_updated_at": None,
                "latest_record_json": json.dumps({"id": "x"}),
            }
        ],
        records=[],
        seen_at_utc="2026-03-01T00:00:00+00:00",
        coverage_complete=False,
        prune_inactive_days=1,
    )
    assert catalog_pruned == []
    assert summary_pruned.pruned_inactive_count == 1

    # Exercise unchanged_count branch in _apply_catalog_updates when replacement is rejected.
    row = _record(source_job_id="a")
    key = ingest_service.dedup_key(row)
    existing = {
        "stable_key": key,
        "first_seen_at_utc": "2026-01-01T00:00:00+00:00",
        "last_seen_at_utc": "2026-01-01T00:00:00+00:00",
        "is_active": True,
        "last_payload_hash": "z",
        "latest_posted_at": "2026-02-01T00:00:00+00:00",
        "latest_updated_at": "2026-02-01T00:00:00+00:00",
        "latest_record_json": json.dumps(_record(source_job_id="a"), sort_keys=True),
    }
    incoming = _record(source_job_id="a")
    incoming["source_payload_hash"] = "a"
    incoming["posted_at"] = "2026-01-01T00:00:00+00:00"
    incoming["source_updated_at"] = "2026-01-01T00:00:00+00:00"
    _, rejected_summary = ingest_service._apply_catalog_updates(
        catalog=[existing],
        records=[incoming],
        seen_at_utc="2026-03-01T00:00:00+00:00",
        coverage_complete=False,
        merge_policy="updated_hash",
    )
    assert rejected_summary.unchanged_count >= 1

    # Snapshot prune no-dir and OSError branch.
    retained, pruned = ingest_service._prune_snapshots(
        snapshot_path=tmp_path / "missing" / "snapshots" / "x.parquet",
        retain_snapshots=1,
    )
    assert (retained, pruned) == (0, 0)

    snap_dir = tmp_path / "snapshots"
    snap_dir.mkdir()
    files = []
    for idx in range(3):
        p = snap_dir / f"20260101T00000{idx}-abc.parquet"
        p.write_text("x", encoding="utf-8")
        files.append(p)
    monkeypatch.setattr(Path, "unlink", lambda self: (_ for _ in ()).throw(OSError("x")))
    retained, pruned = ingest_service._prune_snapshots(
        snapshot_path=files[-1],
        retain_snapshots=1,
    )
    assert retained == 1
    assert pruned == 2

    assert ingest_service._elapsed_ms(perf_counter() - 0.001) >= 0

    # _should_replace_record updated_hash branches.
    assert (
        ingest_service._should_replace_record(
            existing={
                "last_payload_hash": "x",
                "latest_posted_at": "2026-02-01T00:00:00+00:00",
                "latest_updated_at": "2026-02-01T00:00:00+00:00",
            },
            incoming_payload_hash="y",
            incoming_posted_at="2026-01-01T00:00:00+00:00",
            incoming_updated_at="2026-01-01T00:00:00+00:00",
            merge_policy="updated_hash",
        )
        is False
    )
    assert (
        ingest_service._should_replace_record(
            existing={
                "last_payload_hash": "z",
                "latest_posted_at": "2026-01-01T00:00:00+00:00",
                "latest_updated_at": "2026-01-01T00:00:00+00:00",
            },
            incoming_payload_hash="a",
            incoming_posted_at="2026-01-01T00:00:00+00:00",
            incoming_updated_at="2026-01-01T00:00:00+00:00",
            merge_policy="updated_hash",
        )
        is False
    )
    assert ingest_service._compare_optional_datetimes(
        datetime.now(UTC), datetime.now(UTC) - timedelta(days=1)
    ) == 1
    assert (
        ingest_service._should_replace_record(
            existing={
                "last_payload_hash": "a",
                "latest_posted_at": "2026-01-01T00:00:00+00:00",
                "latest_updated_at": "2026-01-01T00:00:00+00:00",
            },
            incoming_payload_hash="b",
            incoming_posted_at="2026-01-02T00:00:00+00:00",
            incoming_updated_at="2026-01-01T00:00:00+00:00",
            merge_policy="updated_hash",
        )
        is True
    )
    assert (
        ingest_service._should_replace_record(
            existing={
                "last_payload_hash": "a",
                "latest_posted_at": "2026-01-02T00:00:00+00:00",
                "latest_updated_at": "2026-01-01T00:00:00+00:00",
            },
            incoming_payload_hash="z",
            incoming_posted_at="2026-01-01T00:00:00+00:00",
            incoming_updated_at="2026-01-01T00:00:00+00:00",
            merge_policy="updated_hash",
        )
        is False
    )


def test_manifest_merge_policy_validation(tmp_path: Path) -> None:
    defaults_bad = tmp_path / "defaults_bad.toml"
    defaults_bad.write_text(
        """
[defaults]
merge_policy = "bad"
[[sources]]
source = "greenhouse"
source_ref = "stripe"
""".strip(),
        encoding="utf-8",
    )
    with pytest.raises(ConfigValidationError, match="defaults.merge_policy"):
        ingest_manifest.load_ingest_manifest(defaults_bad)

    source_bad = tmp_path / "source_bad.toml"
    source_bad.write_text(
        """
[[sources]]
source = "greenhouse"
source_ref = "stripe"
merge_policy = "bad"
""".strip(),
        encoding="utf-8",
    )
    with pytest.raises(ConfigValidationError, match="sources\\[0\\]\\.merge_policy"):
        ingest_manifest.load_ingest_manifest(source_bad)


def test_http_and_model_and_output_and_lineage_v3(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert ingest_service._duration_ms(datetime.now(UTC), datetime.now(UTC)) >= 0
    assert ingest_service._utc_now_iso().endswith("+00:00")
    assert ingest_service._text_or_none(" ") is None
    assert ingest_service._text_or_none(1) == "1"
    assert ingest_service._is_coverage_complete(
        request_count=0, max_pages=2, fetched_count=0, max_jobs=2
    )
    assert not ingest_service._is_coverage_complete(
        request_count=2, max_pages=2, fetched_count=0, max_jobs=2
    )
    assert not ingest_service._is_coverage_complete(
        request_count=1, max_pages=2, fetched_count=2, max_jobs=2
    )

    assert ingest_service._active_records_from_catalog(
        [{"is_active": True, "latest_record_json": "{"}]
    ) == []
    assert ingest_service._active_records_from_catalog(
        [{"is_active": True, "latest_record_json": json.dumps([1, 2])}]
    ) == []
    assert ingest_service._active_records_from_catalog(
        [{"is_active": False, "latest_record_json": json.dumps({"id": "1"})}]
    ) == []

    assert ingest_service._resolve_paths(
        source="greenhouse",
        source_ref="a b",
        output_parquet=None,
        report_file=None,
        write_raw=False,
    )[0].name == "jobs.parquet"

    delay = importlib.import_module("honestroles.ingest.http")._retry_delay_seconds(
        "https://example.com", 0, 1.0
    )
    assert delay > 0

    report = IngestionReport(
        schema_version=INGEST_SCHEMA_VERSION,
        status="pass",
        source="greenhouse",
        source_ref="stripe",
        started_at_utc=datetime.now(UTC).isoformat(),
        finished_at_utc=datetime.now(UTC).isoformat(),
        duration_ms=1,
        request_count=1,
        fetched_count=1,
        normalized_count=1,
        dedup_dropped=0,
        high_watermark_before=None,
        high_watermark_after=None,
        output_paths={"report": str(tmp_path / "x.json")},
    )
    validation = IngestionValidationResult(
        report=report,
        report_file=tmp_path / "validate.json",
        raw_file=tmp_path / "raw.jsonl",
        rows_evaluated=1,
        check_codes=("A",),
    )
    payload = validation.to_payload()
    assert payload["raw_file"].endswith("raw.jsonl")

    output.emit_payload(
        {
            "status": "warn",
            "schema_version": "1.0",
            "source": "greenhouse",
            "source_ref": "stripe",
            "rows_evaluated": 1,
            "fetched_count": 1,
            "normalized_count": 1,
            "dedup_dropped": 0,
            "quality_status": "warn",
            "quality_summary": {"pass": 1, "warn": 1, "fail": 0},
            "warnings": ["A"],
            "output_paths": {"report": "/tmp/report.json"},
        },
        "table",
    )
    output.emit_payload(
        {
            "status": "pass",
            "sources": [],
            "total_sources": 0,
            "pass_count": 0,
            "fail_count": 0,
            "total_rows_written": 0,
            "total_fetched_count": 0,
            "total_request_count": 0,
            "quality_summary": {"pass": 0, "warn": 0, "fail": 0},
        },
        "table",
    )
    rendered = capsys.readouterr().out
    assert "QUALITY pass=" in rendered
    assert "warnings" in rendered

    args_validate = {
        "command": "ingest",
        "ingest_command": "validate",
        "source": "greenhouse",
        "source_ref": "stripe",
        "write_raw": True,
        "report_file": None,
    }
    artifacts = lineage.build_artifact_paths(args_validate, None)
    assert "report_file" in artifacts
    assert "raw_file" in artifacts
    artifacts_no_raw = lineage.build_artifact_paths(
        {**args_validate, "write_raw": False},
        None,
    )
    assert "raw_file" not in artifacts_no_raw

    record_validate = lineage.create_record(
        args=args_validate,
        exit_code=0,
        started_at=datetime.now(UTC),
        finished_at=datetime.now(UTC),
        payload={
            "check_codes": ["X"],
            "request_count": 1,
            "fetched_count": 1,
            "normalized_count": 1,
            "rows_evaluated": 1,
            "quality_status": "warn",
            "quality_summary": {"pass": 0, "warn": 1, "fail": 0},
            "stage_timings_ms": {"total": 1},
            "warnings": ["X"],
        },
    )
    assert record_validate["ingest_metrics"] is not None
    assert record_validate["check_codes"] == ["X"]

    record_sync_all = lineage.create_record(
        args={"command": "ingest", "ingest_command": "sync-all", "manifest": ""},
        exit_code=0,
        started_at=datetime.now(UTC),
        finished_at=datetime.now(UTC),
        payload={
            "check_codes": [],
            "total_sources": 1,
            "pass_count": 1,
            "fail_count": 0,
            "total_request_count": 1,
            "total_fetched_count": 1,
            "total_rows_written": 1,
            "quality_summary": {"pass": 1, "warn": 0, "fail": 0},
            "stage_timings_ms": {"total": 1},
        },
    )
    assert record_sync_all["ingest_metrics"] is not None
    assert lineage._safe_int(None) == 0
    assert lineage._safe_int("x") == 0
    assert lineage._safe_int("12") == 12

    safe_metrics = lineage._ingest_metrics(
        "ingest.sync",
        {
            "request_count": "x",
            "fetched_count": None,
            "normalized_count": "7",
            "rows_written": "bad",
            "quality_status": "warn",
        },
    )
    assert safe_metrics is not None
    assert safe_metrics["request_count"] == 0
    assert safe_metrics["normalized_count"] == 7


def test_cli_validate_dispatch_and_handler(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    report = IngestionReport(
        schema_version=INGEST_SCHEMA_VERSION,
        status="fail",
        source="greenhouse",
        source_ref="stripe",
        started_at_utc=datetime.now(UTC).isoformat(),
        finished_at_utc=datetime.now(UTC).isoformat(),
        duration_ms=1,
        request_count=1,
        fetched_count=1,
        normalized_count=1,
        dedup_dropped=0,
        high_watermark_before=None,
        high_watermark_after=None,
        output_paths={"report": str(tmp_path / "validate.json")},
    )
    validate_result = IngestionValidationResult(
        report=report,
        report_file=tmp_path / "validate.json",
        rows_evaluated=1,
    )
    monkeypatch.setattr(handlers, "validate_ingestion_source", lambda **_k: validate_result)
    cmd = handlers.handle_ingest_validate(
        argparse.Namespace(
            source="greenhouse",
            source_ref="stripe",
            report_file=None,
            write_raw=False,
            max_pages=1,
            max_jobs=1,
            timeout_seconds=1.0,
            max_retries=0,
            base_backoff_seconds=0.0,
            user_agent="ua",
            quality_policy_file=None,
            strict_quality=True,
        )
    )
    assert cmd.exit_code == 1

    main_mod = importlib.import_module("honestroles.cli.main")
    monkeypatch.setattr(main_mod, "should_track", lambda _args: False)
    monkeypatch.setattr(
        main_mod,
        "handle_ingest_validate",
        lambda _args: handlers.CommandResult(payload={"status": "pass"}, exit_code=0),
    )
    code = main_mod.main(["ingest", "validate", "--source", "greenhouse", "--source-ref", "stripe"])
    assert code == 0


def test_sync_all_quality_status_not_counted(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    manifest_path = tmp_path / "ingest.toml"
    manifest_path.write_text(
        """
[[sources]]
source = "greenhouse"
source_ref = "stripe"
""".strip(),
        encoding="utf-8",
    )

    report = IngestionReport(
        schema_version=INGEST_SCHEMA_VERSION,
        status="pass",
        source="greenhouse",
        source_ref="stripe",
        started_at_utc=datetime.now(UTC).isoformat(),
        finished_at_utc=datetime.now(UTC).isoformat(),
        duration_ms=1,
        request_count=1,
        fetched_count=1,
        normalized_count=1,
        dedup_dropped=0,
        high_watermark_before=None,
        high_watermark_after=None,
        output_paths={"report": str(tmp_path / "sync_report.json")},
        quality_status="unknown",
    )
    sync_result = IngestionResult(
        report=report,
        output_parquet=tmp_path / "jobs.parquet",
        report_file=tmp_path / "sync_report.json",
        rows_written=1,
    )
    monkeypatch.setattr(ingest_service, "sync_source", lambda **_kwargs: sync_result)
    batch = ingest_service.sync_sources_from_manifest(manifest_path=manifest_path)
    assert batch.quality_summary == {"pass": 0, "warn": 0, "fail": 0}


def test_sync_all_key_field_completeness_aggregation_edges(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    manifest_path = tmp_path / "ingest.toml"
    manifest_path.write_text(
        """
[[sources]]
source = "greenhouse"
source_ref = "stripe"

[[sources]]
source = "lever"
source_ref = "netflix"

[[sources]]
source = "ashby"
source_ref = "notion"
""".strip(),
        encoding="utf-8",
    )

    class _FakeResult:
        def __init__(self, payload: dict[str, Any]) -> None:
            self._payload = payload

        def to_payload(self) -> dict[str, Any]:
            return dict(self._payload)

    def fake_sync_source(**kwargs: Any) -> _FakeResult:
        source = kwargs["source"]
        base = {
            "schema_version": INGEST_SCHEMA_VERSION,
            "status": "pass",
            "source": source,
            "source_ref": kwargs["source_ref"],
            "rows_written": 1,
            "fetched_count": 1,
            "request_count": 1,
            "normalized_count": 1,
            "quality_status": "pass",
            "quality_summary": {"pass": 1, "warn": 0, "fail": 0},
            "check_codes": [],
        }
        if source == "greenhouse":
            base["key_field_completeness"] = "bad"
        elif source == "lever":
            base["rows_written"] = 0
            base["normalized_count"] = 0
            base["key_field_completeness"] = {"company_non_null_pct": 50.0}
        else:
            base["rows_written"] = 2
            base["key_field_completeness"] = {
                "company_non_null_pct": "bad",
                "posted_at_non_null_pct": "80.0",
            }
        return _FakeResult(base)

    monkeypatch.setattr(ingest_service, "sync_source", fake_sync_source)
    batch = ingest_service.sync_sources_from_manifest(manifest_path=manifest_path)
    assert batch.status == "pass"
    assert batch.fail_count == 0
    assert batch.key_field_completeness == {"posted_at_non_null_pct": 80.0}
    assert ingest_service._safe_int(None) == 0
    assert ingest_service._safe_int("bad") == 0
    assert ingest_service._aggregate_key_field_completeness({}, total_weight=0) == {}
    assert (
        ingest_service._key_field_completeness(
            [
                {
                    "company": None,
                    "posted_at": "2026-01-01T00:00:00Z",
                    "description_text": "desc",
                    "location": None,
                    "remote": None,
                    "work_mode": None,
                }
            ]
        )["location_or_remote_signal_pct"]
        == 0.0
    )


def test_validate_ingestion_source_wraps_unexpected_errors(tmp_path: Path) -> None:
    original = ingest_service._SOURCE_FETCHERS["greenhouse"]
    ingest_service._SOURCE_FETCHERS["greenhouse"] = (
        lambda _source_ref, *, max_pages, max_jobs, http_get_json: (_ for _ in ()).throw(
            RuntimeError("boom")
        )
    )
    try:
        with pytest.raises(HonestRolesError, match="ingestion validate failed"):
            ingest_service.validate_ingestion_source(
                source="greenhouse",
                source_ref="stripe",
                report_file=tmp_path / "validate_unexpected_error.json",
            )
    finally:
        ingest_service._SOURCE_FETCHERS["greenhouse"] = original
    payload = json.loads(
        (tmp_path / "validate_unexpected_error.json").read_text(encoding="utf-8")
    )
    assert payload["status"] == "fail"
    assert payload["error"]["type"] == "HonestRolesError"
