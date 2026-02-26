from __future__ import annotations

import argparse
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import tempfile

import pytest
from hypothesis import given
from hypothesis import strategies as st

import honestroles.cli.report_data_quality as report_cli
from honestroles.cli.report_data_quality import build_report, report_to_text

from .strategies import TEXT_VALUES


@pytest.mark.fuzz
@given(
    dataset_name=TEXT_VALUES,
    row_count=st.integers(min_value=0, max_value=1000),
    column_count=st.integers(min_value=0, max_value=200),
    unknown_pct=st.floats(min_value=0.0, max_value=100.0, allow_nan=False),
)
def test_fuzz_report_to_text_stable_output(
    dataset_name: str,
    row_count: int,
    column_count: int,
    unknown_pct: float,
) -> None:
    report = {
        "dataset_name": dataset_name,
        "row_count": row_count,
        "column_count": column_count,
        "required_field_null_counts": {"job_key": 0},
        "required_field_empty_counts": {"job_key": 0},
        "top_duplicate_job_keys": [],
        "top_duplicate_content_hashes": [],
        "listing_page_rows": 0,
        "listing_page_ratio": 0.0,
        "source_row_counts": {"src": row_count},
        "source_quality": {
            "src": {
                "unknown_location_pct": unknown_pct,
                "missing_description_pct": 0.0,
                "remote_true_pct": 0.0,
            }
        },
        "enrichment_sparsity_pct": {"skills": 0.0},
        "invalid_apply_url_count": 0,
        "unknown_location_count": 0,
    }

    text = report_to_text(report)
    assert "Dataset:" in text
    assert "Rows:" in text
    assert "Columns:" in text


@pytest.mark.fuzz
@given(
    suffix=st.sampled_from([".txt", ".csv", ".json"]),
    missing=st.booleans(),
)
def test_fuzz_build_report_validation_errors_are_controlled(
    suffix: str,
    missing: bool,
) -> None:
    with tempfile.TemporaryDirectory() as tempdir:
        input_path = Path(tempdir) / f"sample{suffix}"
        if not missing:
            input_path.write_text("stub", encoding="utf-8")

        args = argparse.Namespace(
            input=input_path,
            format="text",
            dataset_name=None,
            stream=False,
            table=None,
            query=None,
            top_n_duplicates=10,
        )

        with pytest.raises((FileNotFoundError, ValueError)):
            build_report(args)


@pytest.mark.fuzz
@given(output_format=st.sampled_from(["text", "json"]))
def test_fuzz_cli_main_dispatch_stable(
    output_format: str,
) -> None:
    report = {
        "dataset_name": "dataset",
        "row_count": 1,
        "column_count": 1,
        "required_field_null_counts": {"job_key": 0},
        "required_field_empty_counts": {"job_key": 0},
        "top_duplicate_job_keys": [],
        "top_duplicate_content_hashes": [],
        "listing_page_rows": 0,
        "listing_page_ratio": 0.0,
        "source_row_counts": {},
        "source_quality": {},
        "enrichment_sparsity_pct": {},
        "invalid_apply_url_count": 0,
        "unknown_location_count": 0,
    }
    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(report_cli, "build_report", lambda args: report)
        with tempfile.TemporaryDirectory() as tempdir:
            output_buffer = io.StringIO()
            with redirect_stdout(output_buffer):
                code = report_cli.main(
                    [str(Path(tempdir) / "input.parquet"), "--format", output_format]
                )

    output_text = output_buffer.getvalue()
    assert code == 0
    if output_format == "json":
        payload = json.loads(output_text)
        assert payload["row_count"] == 1
    else:
        assert "Dataset:" in output_text
