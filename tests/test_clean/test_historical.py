from __future__ import annotations

import pandas as pd

from honestroles.clean import (
    HistoricalCleanOptions,
    clean_historical_jobs,
    detect_historical_listing_pages,
)


def _historical_fixture() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "job_key": "acme::lever::acme",
                "company": "Acme",
                "source": "lever",
                "job_id": "acme",
                "title": "Acme jobs",
                "location_raw": "Unknown",
                "apply_url": "https://acme.com/jobs",
                "description_text": "Landing page",
                "ingested_at": "2025-01-01T00:00:00Z",
                "content_hash": "landing",
                "remote_flag": False,
            },
            {
                "job_key": "acme::greenhouse::1",
                "company": "Acme",
                "source": "greenhouse",
                "job_id": "1",
                "title": "Software Engineer",
                "location_raw": "Remote, US",
                "apply_url": "https://acme.com/jobs/1",
                "description_text": "Build systems.\n- APIs\n- Services",
                "ingested_at": "2025-01-01T01:00:00Z",
                "content_hash": "role-1",
                "remote_flag": True,
            },
            {
                "job_key": "acme::greenhouse::1",
                "company": "Acme",
                "source": "greenhouse",
                "job_id": "1",
                "title": "Software Engineer",
                "location_raw": "Remote, US",
                "apply_url": "https://acme.com/jobs/1",
                "description_text": "Build systems.\n- APIs\n- Services",
                "ingested_at": "2025-01-02T01:00:00Z",
                "content_hash": "role-1",
                "remote_flag": True,
            },
        ]
    )


def test_detect_historical_listing_pages() -> None:
    df = _historical_fixture()
    mask = detect_historical_listing_pages(df)
    assert mask.tolist() == [True, False, False]


def test_detect_historical_listing_pages_missing_columns_returns_false_mask() -> None:
    df = pd.DataFrame({"title": ["Acme jobs"]})
    mask = detect_historical_listing_pages(df)
    assert mask.tolist() == [False]


def test_clean_historical_jobs_default_drops_listing_and_compacts() -> None:
    df = _historical_fixture()
    cleaned = clean_historical_jobs(df)
    assert len(cleaned) == 1
    assert "historical_is_listing_page" in cleaned.columns
    assert "snapshot_count" in cleaned.columns
    assert int(cleaned.loc[0, "snapshot_count"]) == 2
    assert isinstance(cleaned["first_seen"].dtype, pd.DatetimeTZDtype)
    assert isinstance(cleaned["last_seen"].dtype, pd.DatetimeTZDtype)
    assert bool(cleaned.loc[0, "historical_is_listing_page"]) is False


def test_clean_historical_jobs_opt_out_flags() -> None:
    df = _historical_fixture()
    cleaned = clean_historical_jobs(
        df,
        options=HistoricalCleanOptions(
            detect_listing_pages=False,
            drop_listing_pages=False,
            compact_snapshots=False,
        ),
    )
    assert len(cleaned) == 3
    assert "snapshot_count" not in cleaned.columns
    assert cleaned["historical_is_listing_page"].tolist() == [False, False, False]


def test_clean_historical_jobs_iso8601_snapshot_override() -> None:
    df = _historical_fixture()
    cleaned = clean_historical_jobs(
        df,
        options=HistoricalCleanOptions(snapshot_timestamp_output="iso8601"),
    )
    assert cleaned.loc[0, "first_seen"] == "2025-01-01T01:00:00Z"
    assert cleaned.loc[0, "last_seen"] == "2025-01-02T01:00:00Z"


def test_clean_historical_jobs_with_missing_compaction_keys_keeps_rows() -> None:
    df = _historical_fixture().drop(columns=["job_key", "content_hash"])
    cleaned = clean_historical_jobs(
        df,
        options=HistoricalCleanOptions(
            detect_listing_pages=False,
            drop_listing_pages=False,
            compact_snapshots=True,
            compaction_keys=("job_key", "content_hash"),
        ),
    )
    assert len(cleaned) == 3
    assert cleaned["snapshot_count"].tolist() == [1, 1, 1]


def test_clean_historical_jobs_prefers_existing_description_text_by_default() -> None:
    df = _historical_fixture().copy()
    df["description_html"] = ["<p>Landing</p>", "<p>From HTML</p>", "<p>From HTML</p>"]
    df.loc[1:, "description_text"] = "From source text"

    cleaned = clean_historical_jobs(df)

    assert cleaned.loc[0, "description_text"] == "From source text"
