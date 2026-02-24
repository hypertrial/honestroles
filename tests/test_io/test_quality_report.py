from __future__ import annotations

import pandas as pd

from honestroles.io import DataQualityAccumulator, build_data_quality_report


def _report_df() -> pd.DataFrame:
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
                "content_hash": "h1",
                "remote_flag": False,
                "salary_min": None,
                "skills": None,
            },
            {
                "job_key": "acme::greenhouse::1",
                "company": "Acme",
                "source": "greenhouse",
                "job_id": "1",
                "title": "Engineer",
                "location_raw": "Remote, US",
                "apply_url": "https://acme.com/jobs/1",
                "description_text": "Build systems",
                "ingested_at": "2025-01-01T01:00:00Z",
                "content_hash": "h2",
                "remote_flag": True,
                "salary_min": 120000.0,
                "skills": ["Python"],
            },
            {
                "job_key": "acme::greenhouse::1",
                "company": "Acme",
                "source": "greenhouse",
                "job_id": "1",
                "title": "Engineer",
                "location_raw": "Remote, US",
                "apply_url": "https://acme.com/jobs/1",
                "description_text": "Build systems",
                "ingested_at": "2025-01-02T01:00:00Z",
                "content_hash": "h2",
                "remote_flag": True,
                "salary_min": 120000.0,
                "skills": ["Python"],
            },
        ]
    )


def test_build_data_quality_report_basic_sections() -> None:
    report = build_data_quality_report(_report_df(), dataset_name="fixture").to_dict()
    assert report["dataset_name"] == "fixture"
    assert report["row_count"] == 3
    assert report["listing_page_rows"] == 1
    assert report["unknown_location_count"] == 1
    assert report["required_field_null_counts"]["job_key"] == 0
    assert report["top_duplicate_job_keys"][0]["value"] == "acme::greenhouse::1"
    assert report["top_duplicate_job_keys"][0]["count"] == 2
    assert "greenhouse" in report["source_quality"]
    assert "skills" in report["enrichment_sparsity_pct"]


def test_data_quality_accumulator_streaming_matches_batch() -> None:
    df = _report_df()
    batch = build_data_quality_report(df, dataset_name="batch").to_dict()

    acc = DataQualityAccumulator(dataset_name="batch")
    acc.update(df.iloc[:2].copy())
    acc.update(df.iloc[2:].copy())
    streamed = acc.finalize().to_dict()

    assert streamed["row_count"] == batch["row_count"]
    assert streamed["listing_page_rows"] == batch["listing_page_rows"]
    assert streamed["invalid_apply_url_count"] == batch["invalid_apply_url_count"]
    assert streamed["unknown_location_count"] == batch["unknown_location_count"]
    assert streamed["top_duplicate_job_keys"] == batch["top_duplicate_job_keys"]


def test_data_quality_report_handles_missing_columns_and_object_enrichment() -> None:
    df = pd.DataFrame(
        {
            "job_key": ["jk-1", "jk-2"],
            "salary_text": pd.Series([" ", "USD 100k"], dtype="object"),
        }
    )
    report = build_data_quality_report(df).to_dict()
    assert report["listing_page_rows"] == 0
    assert report["required_field_null_counts"]["apply_url"] == 2
    assert report["required_field_empty_counts"]["apply_url"] == 2
    assert report["source_row_counts"] == {}
    assert report["unknown_location_count"] == 0
    assert report["enrichment_sparsity_pct"]["salary_text"] == 50.0


def test_data_quality_report_counts_invalid_url_string_and_type() -> None:
    df = pd.DataFrame(
        [
            {
                "job_key": "acme::1",
                "company": "Acme",
                "source": "lever",
                "job_id": "1",
                "title": "Engineer",
                "location_raw": "Remote, US",
                "apply_url": "ftp://acme.example/jobs/1",
                "ingested_at": "2025-01-01T00:00:00Z",
                "content_hash": "h1",
            },
            {
                "job_key": "acme::2",
                "company": "Acme",
                "source": "lever",
                "job_id": "2",
                "title": "Engineer II",
                "location_raw": "Remote, US",
                "apply_url": 42,
                "ingested_at": "2025-01-01T00:00:00Z",
                "content_hash": "h2",
            },
        ]
    )
    report = build_data_quality_report(df).to_dict()
    assert report["invalid_apply_url_count"] == 2


def test_data_quality_accumulator_empty_update_has_zero_percentages() -> None:
    acc = DataQualityAccumulator()
    acc.update(pd.DataFrame(columns=["job_key", "salary_text"]))
    report = acc.finalize().to_dict()
    assert report["row_count"] == 0
    assert report["column_count"] == 2
    assert report["listing_page_ratio"] == 0.0
    assert report["enrichment_sparsity_pct"]["salary_text"] == 0.0


def test_data_quality_report_counts_remote_true_only_for_real_booleans() -> None:
    df = pd.DataFrame(
        [
            {
                "job_key": "acme::1",
                "company": "Acme",
                "source": "lever",
                "job_id": "1",
                "title": "Engineer",
                "location_raw": "Remote, US",
                "apply_url": "https://example.com/apply/1",
                "ingested_at": "2025-01-01T00:00:00Z",
                "content_hash": "h1",
                "remote_flag": "False",
            },
            {
                "job_key": "acme::2",
                "company": "Acme",
                "source": "lever",
                "job_id": "2",
                "title": "Engineer II",
                "location_raw": "Remote, US",
                "apply_url": "https://example.com/apply/2",
                "ingested_at": "2025-01-01T00:00:00Z",
                "content_hash": "h2",
                "remote_flag": True,
            },
        ]
    )
    report = build_data_quality_report(df).to_dict()
    assert report["source_quality"]["lever"]["remote_true_pct"] == 50.0
