from __future__ import annotations

import pytest
from hypothesis import given

from honestroles.io.quality_report import DataQualityAccumulator, build_data_quality_report

from .strategies import ARRAY_LIKE_VALUES, MIXED_SCALARS, URL_LIKE_VALUES, dataframe_for_columns


@pytest.mark.fuzz
@given(
    df=dataframe_for_columns(
        {
            "job_key": MIXED_SCALARS,
            "content_hash": MIXED_SCALARS,
            "company": MIXED_SCALARS,
            "source": MIXED_SCALARS,
            "job_id": MIXED_SCALARS,
            "title": MIXED_SCALARS,
            "location_raw": MIXED_SCALARS,
            "apply_url": URL_LIKE_VALUES,
            "description_text": MIXED_SCALARS,
            "ingested_at": MIXED_SCALARS,
            "salary_text": MIXED_SCALARS,
            "salary_min": MIXED_SCALARS,
            "salary_max": MIXED_SCALARS,
            "salary_currency": MIXED_SCALARS,
            "salary_interval": MIXED_SCALARS,
            "skills": ARRAY_LIKE_VALUES,
            "languages": ARRAY_LIKE_VALUES,
            "benefits": ARRAY_LIKE_VALUES,
            "keywords": ARRAY_LIKE_VALUES,
            "remote_flag": MIXED_SCALARS,
            "visa_sponsorship": MIXED_SCALARS,
        },
        max_rows=16,
    )
)
def test_fuzz_data_quality_accumulator_finalize_safe(df) -> None:
    accumulator = DataQualityAccumulator(dataset_name="fuzz_dataset", top_n_duplicates=5)
    midpoint = len(df) // 2
    accumulator.update(df.iloc[:midpoint])
    accumulator.update(df.iloc[midpoint:])
    report = accumulator.finalize()

    assert report.row_count >= 0
    assert report.column_count >= 0
    assert report.invalid_apply_url_count >= 0
    assert report.unknown_location_count >= 0

    for value in report.enrichment_sparsity_pct.values():
        assert 0.0 <= value <= 100.0
    for source_metrics in report.source_quality.values():
        for metric in source_metrics.values():
            assert 0.0 <= metric <= 100.0


@pytest.mark.fuzz
@given(
    df=dataframe_for_columns(
        {
            "job_key": MIXED_SCALARS,
            "content_hash": MIXED_SCALARS,
            "company": MIXED_SCALARS,
            "source": MIXED_SCALARS,
            "job_id": MIXED_SCALARS,
            "title": MIXED_SCALARS,
            "location_raw": MIXED_SCALARS,
            "apply_url": URL_LIKE_VALUES,
            "ingested_at": MIXED_SCALARS,
        },
        max_rows=16,
    )
)
def test_fuzz_build_data_quality_report_safe(df) -> None:
    report = build_data_quality_report(df, dataset_name="fuzz", top_n_duplicates=3)
    payload = report.to_dict()
    assert payload["row_count"] >= 0
    assert payload["column_count"] >= 0
