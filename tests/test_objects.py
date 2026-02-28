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
from honestroles.schema import CANONICAL_JOB_SCHEMA, CanonicalFieldSpec


def _canonical_frame() -> pl.DataFrame:
    return pl.DataFrame(
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


def test_job_dataset_polars_roundtrip_record_iteration_and_copy_behavior() -> None:
    frame = _canonical_frame()
    dataset = JobDataset.from_polars(frame)

    assert dataset.row_count() == 1
    assert "fit_score" in dataset.columns()
    assert dataset.to_polars().equals(frame)
    assert dataset.materialize_records()[0].title == "Engineer"
    assert list(dataset.iter_records())[0].title == "Engineer"
    assert dataset.materialize_records(limit=0) == []

    copied = dataset.to_polars()
    copied = copied.with_columns(pl.lit("mutated").alias("title"))
    assert dataset.to_polars()["title"].to_list() == ["Engineer"]

    live = dataset.to_polars(copy=False)
    assert live is dataset.to_polars(copy=False)

    with pytest.raises(TypeError):
        JobDataset(_frame="not-a-frame")  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        JobDataset(_frame=frame, canonical_fields=["title"])  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        JobDataset(_frame=frame, canonical_fields=("title", 1))  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        JobDataset(_frame=frame, canonical_fields=("id",) * 12)
    with pytest.raises(TypeError):
        JobDataset(_frame=frame, canonical_fields=("title",))


def test_job_dataset_from_polars_rejects_missing_or_invalid_types() -> None:
    with pytest.raises(ValueError, match="missing canonical fields"):
        JobDataset.from_polars(pl.DataFrame({"title": ["Engineer"]}))

    invalid_title = _canonical_frame().with_columns(pl.lit(1).alias("title"))
    with pytest.raises(TypeError, match="title"):
        JobDataset.from_polars(invalid_title)

    invalid_remote = _canonical_frame().with_columns(pl.lit("yes").alias("remote"))
    with pytest.raises(TypeError, match="remote"):
        JobDataset.from_polars(invalid_remote)

    invalid_skills = _canonical_frame().with_columns(pl.lit("python,sql").alias("skills"))
    with pytest.raises(TypeError, match="skills"):
        JobDataset.from_polars(invalid_skills)

    invalid_salary = _canonical_frame().with_columns(pl.lit("100000").alias("salary_min"))
    with pytest.raises(TypeError, match="salary_min"):
        JobDataset.from_polars(invalid_salary)


def test_job_dataset_transform_and_materialize_validation() -> None:
    dataset = JobDataset.from_polars(_canonical_frame())
    transformed = dataset.transform(lambda frame: frame.with_columns(pl.lit("Platform").alias("title")))

    assert transformed.to_polars()["title"].to_list() == ["Platform"]
    assert dataset.to_polars()["title"].to_list() == ["Engineer"]

    with pytest.raises(TypeError):
        dataset.transform(lambda frame: "not-a-frame")  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        dataset.materialize_records(limit=-1)
    with pytest.raises(TypeError):
        dataset.materialize_records(limit="x")  # type: ignore[arg-type]
    assert dataset.materialize_records(limit=1)[0].title == "Engineer"

    assert not hasattr(dataset, "rows")
    assert not hasattr(dataset, "select")


def test_job_dataset_accepts_null_skills_dtype() -> None:
    dataset = JobDataset.from_polars(
        pl.DataFrame(
            {
                "id": ["1"],
                "title": ["Engineer"],
                "company": ["A"],
                "location": ["Remote"],
                "remote": [True],
                "description_text": ["desc"],
                "description_html": [None],
                "skills": [[]],
                "salary_min": [100000.0],
                "salary_max": [120000.0],
                "apply_url": ["https://example.com"],
                "posted_at": ["2026-01-01"],
            }
        )
    )
    dataset.validate()


def test_job_dataset_accepts_null_column_for_skills() -> None:
    dataset = JobDataset.from_polars(
        pl.DataFrame(
            {
                "id": ["1"],
                "title": ["Engineer"],
                "company": ["A"],
                "location": ["Remote"],
                "remote": [True],
                "description_text": ["desc"],
                "description_html": [None],
                "skills": [None],
                "salary_min": [100000.0],
                "salary_max": [120000.0],
                "apply_url": ["https://example.com"],
                "posted_at": ["2026-01-01"],
            }
        )
    )
    dataset.validate()


def test_job_dataset_rejects_unknown_logical_type(monkeypatch: pytest.MonkeyPatch) -> None:
    dataset = JobDataset.from_polars(_canonical_frame())

    monkeypatch.setitem(
        CANONICAL_JOB_SCHEMA,
        "title",
        CanonicalFieldSpec(name="title", logical_type="mystery"),  # type: ignore[arg-type]
    )
    with pytest.raises(TypeError, match="unsupported logical type"):
        dataset.validate_canonical_types()


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
