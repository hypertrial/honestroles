from __future__ import annotations

import polars as pl
import pytest

from honestroles.diagnostics import (
    InputAdapterDiagnostics,
    InputAliasingDiagnostics,
    NonFatalStageError,
    PluginExecutionCounts,
    RuntimeDiagnostics,
    RuntimeSettingsSnapshot,
    StageRowCounts,
)
from honestroles.domain import ApplicationPlanEntry, CanonicalJobRecord, JobDataset


def test_canonical_job_record_validation_and_conversion() -> None:
    record = CanonicalJobRecord.from_mapping(
        {
            "id": "1",
            "title": "Engineer",
            "company": "A",
            "location": "Remote",
            "remote": True,
            "description_text": "desc",
            "description_html": None,
            "skills": ["python", "sql"],
            "salary_min": 100000,
            "salary_max": "120000",
            "apply_url": "https://example.com",
            "posted_at": "2026-01-01",
        }
    )
    assert record.skills == ("python", "sql")
    assert record.salary_max == 120000.0
    assert record.to_dict()["skills"] == ["python", "sql"]

    with pytest.raises(TypeError):
        CanonicalJobRecord(remote="yes")  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        CanonicalJobRecord(title=1)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        CanonicalJobRecord(salary_min="x")  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        CanonicalJobRecord(skills=["python"])  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        CanonicalJobRecord(skills=("python", 1))  # type: ignore[arg-type]


def test_canonical_job_record_coercion_branches() -> None:
    from honestroles.domain import _coerce_canonical_value

    assert CanonicalJobRecord.from_mapping({"remote": "yes"}).remote is True
    assert CanonicalJobRecord.from_mapping({"remote": "no"}).remote is False
    assert CanonicalJobRecord.from_mapping({"skills": "python, sql"}).skills == ("python", "sql")
    assert _coerce_canonical_value("salary_min", 10) == 10.0

    with pytest.raises(TypeError):
        CanonicalJobRecord.from_mapping({"title": 123})
    with pytest.raises(TypeError):
        CanonicalJobRecord.from_mapping({"remote": "maybe"})
    with pytest.raises(TypeError):
        CanonicalJobRecord.from_mapping({"remote": 1})
    with pytest.raises(TypeError):
        CanonicalJobRecord.from_mapping({"skills": 3})
    with pytest.raises(TypeError):
        CanonicalJobRecord.from_mapping({"salary_min": object()})


def test_job_dataset_polars_roundtrip_and_rows() -> None:
    frame = pl.DataFrame(
        {
            "id": ["1"],
            "title": ["Engineer"],
            "company": ["A"],
            "location": ["Remote"],
            "remote": [True],
            "description_text": ["desc"],
            "description_html": [None],
            "skills": [["python", "sql"]],
            "salary_min": [100000.0],
            "salary_max": [120000.0],
            "apply_url": ["https://example.com"],
            "posted_at": ["2026-01-01"],
            "fit_score": [0.9],
        }
    )
    dataset = JobDataset.from_polars(frame)
    assert dataset.row_count() == 1
    assert "fit_score" in dataset.columns()
    assert dataset.to_polars().equals(frame)
    assert dataset.rows()[0].title == "Engineer"
    assert dataset.select("title").columns() == ("title",)
    partial = JobDataset.from_polars(pl.DataFrame({"title": ["Engineer"]}))
    partial_row = partial.rows()[0]
    assert partial_row.company is None
    assert partial.missing_canonical_fields()
    with pytest.raises(ValueError):
        partial.validate_canonical_schema()

    with pytest.raises(TypeError):
        JobDataset(frame="not-a-frame")  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        JobDataset(frame=frame, schema_version="")  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        JobDataset(frame=frame, canonical_fields=["title"])  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        JobDataset(frame=frame, canonical_fields=("title", 1))  # type: ignore[arg-type]


def test_application_plan_entry_and_runtime_diagnostics_to_dict() -> None:
    entry = ApplicationPlanEntry(
        fit_rank=1,
        title="Engineer",
        company="A",
        apply_url="https://example.com",
        fit_score=0.8,
        estimated_effort_minutes=15,
    )
    diagnostics = RuntimeDiagnostics(
        input_path="/tmp/jobs.parquet",
        stage_rows=StageRowCounts({"input": 2, "match": 1}),
        plugin_counts=PluginExecutionCounts(filter=1, label=2, rate=3),
        runtime=RuntimeSettingsSnapshot(fail_fast=True, random_seed=0),
        input_adapter=InputAdapterDiagnostics.from_mapping(
            {
                "enabled": True,
                "unresolved": ["salary_max"],
                "error_samples": [
                    {
                        "field": "posted_at",
                        "source": "date_posted",
                        "value": "bad",
                        "reason": "date_parse_failed",
                    }
                ],
            }
        ),
        input_aliasing=InputAliasingDiagnostics.from_mapping({"applied": {"remote": "remote_flag"}}),
        output_path="/tmp/out.parquet",
        final_rows=1,
        non_fatal_errors=(NonFatalStageError(stage="rate", error_type="X", detail="boom"),),
    )
    payload = diagnostics.to_dict()
    assert payload["stage_rows"] == {"input": 2, "match": 1}
    assert payload["plugin_counts"] == {"filter": 1, "label": 2, "rate": 3}
    assert payload["input_aliasing"]["applied"] == {"remote": "remote_flag"}
    assert payload["non_fatal_errors"][0]["stage"] == "rate"
    assert payload["input_adapter"]["error_samples"][0]["reason"] == "date_parse_failed"
    assert entry.to_dict()["fit_rank"] == 1

    with pytest.raises(ValueError):
        ApplicationPlanEntry(
            fit_rank=0,
            title=None,
            company=None,
            apply_url=None,
            fit_score=0.0,
            estimated_effort_minutes=0,
        )
    with pytest.raises(ValueError):
        ApplicationPlanEntry(
            fit_rank=1,
            title=None,
            company=None,
            apply_url=None,
            fit_score=0.0,
            estimated_effort_minutes=-1,
        )
    with pytest.raises(TypeError):
        ApplicationPlanEntry(
            fit_rank=1,
            title=None,
            company=None,
            apply_url=None,
            fit_score="high",  # type: ignore[arg-type]
            estimated_effort_minutes=0,
        )
    with pytest.raises(TypeError):
        ApplicationPlanEntry(
            fit_rank=1,
            title=1,  # type: ignore[arg-type]
            company=None,
            apply_url=None,
            fit_score=0.0,
            estimated_effort_minutes=0,
        )
