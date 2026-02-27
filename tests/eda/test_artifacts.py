from __future__ import annotations

from pathlib import Path
import json

import polars as pl
import pytest

from honestroles.eda import (
    generate_eda_artifacts,
    generate_eda_diff_artifacts,
    load_eda_artifacts,
)
from honestroles.eda.models import EDAProfileResult
from honestroles.errors import ConfigValidationError


def test_generate_eda_artifacts_and_load(sample_parquet: Path, tmp_path: Path) -> None:
    output_dir = tmp_path / "eda"
    manifest = generate_eda_artifacts(
        input_parquet=sample_parquet,
        output_dir=output_dir,
        quality_profile="core_fields_weighted",
        top_k=5,
    )

    assert manifest.schema_version == "1.1"
    assert manifest.artifact_kind == "profile"
    assert manifest.rules_context is not None
    assert (output_dir / "manifest.json").exists()
    assert (output_dir / "summary.json").exists()
    assert (output_dir / "report.md").exists()

    expected_tables = [
        "tables/null_percentages.parquet",
        "tables/column_profile.parquet",
        "tables/source_profile.parquet",
        "tables/top_values_source.parquet",
        "tables/top_values_company.parquet",
        "tables/top_values_title.parquet",
        "tables/top_values_location.parquet",
        "tables/numeric_quantiles.parquet",
        "tables/categorical_distribution.parquet",
    ]
    for rel in expected_tables:
        assert (output_dir / rel).exists()

    expected_figures = [
        "figures/nulls_by_column.png",
        "figures/completeness_by_source.png",
        "figures/remote_by_source.png",
        "figures/posted_at_timeseries.png",
        "figures/top_locations.png",
    ]
    for rel in expected_figures:
        assert (output_dir / rel).exists()

    bundle = load_eda_artifacts(output_dir)
    assert bundle.manifest.row_count_raw == 3
    assert bundle.summary is not None
    assert bundle.summary["shape"]["runtime"]["rows"] == 3


def test_generate_eda_diff_artifacts(sample_parquet: Path, tmp_path: Path) -> None:
    baseline_dir = tmp_path / "baseline"
    candidate_dir = tmp_path / "candidate"
    diff_dir = tmp_path / "diff"

    generate_eda_artifacts(input_parquet=sample_parquet, output_dir=baseline_dir)
    generate_eda_artifacts(input_parquet=sample_parquet, output_dir=candidate_dir)

    manifest = generate_eda_diff_artifacts(
        baseline_dir=baseline_dir,
        candidate_dir=candidate_dir,
        output_dir=diff_dir,
    )

    assert manifest.artifact_kind == "diff"
    assert manifest.schema_version == "1.1"
    assert (diff_dir / "diff.json").exists()
    assert (diff_dir / "tables" / "drift_metrics.parquet").exists()

    bundle = load_eda_artifacts(diff_dir)
    assert bundle.diff is not None
    assert "shape_diff" in bundle.diff
    assert "gate_evaluation" in bundle.diff


def test_load_eda_artifacts_requires_manifest(tmp_path: Path) -> None:
    missing = tmp_path / "missing_artifacts"
    missing.mkdir(parents=True, exist_ok=True)

    with pytest.raises(ConfigValidationError, match="manifest"):
        load_eda_artifacts(missing)


def test_generate_eda_artifacts_input_and_quality_validation_errors(tmp_path: Path) -> None:
    with pytest.raises(ConfigValidationError, match="does not exist"):
        generate_eda_artifacts(input_parquet=tmp_path / "missing.parquet")

    directory = tmp_path / "dir_input"
    directory.mkdir()
    with pytest.raises(ConfigValidationError, match="not a file"):
        generate_eda_artifacts(input_parquet=directory)

    parquet = tmp_path / "jobs.parquet"
    pl.DataFrame({"id": ["1"]}).write_parquet(parquet)
    with pytest.raises(ConfigValidationError, match="invalid EDA quality configuration"):
        generate_eda_artifacts(
            input_parquet=parquet,
            quality_profile="core_fields_weighted",
            field_weights={"x": -1.0},
        )


def test_load_eda_artifacts_manifest_validation_errors(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    root.mkdir()
    manifest_path = root / "manifest.json"

    manifest_path.write_text("{bad", encoding="utf-8")
    with pytest.raises(ConfigValidationError, match="invalid artifacts manifest JSON"):
        load_eda_artifacts(root)

    manifest_path.write_text(json.dumps({"artifact_kind": "profile"}), encoding="utf-8")
    with pytest.raises(ConfigValidationError, match="missing key"):
        load_eda_artifacts(root)

    payload = {
        "schema_version": "1.1",
        "artifact_kind": "profile",
        "generated_at_utc": "2026-01-01T00:00:00+00:00",
        "input_path": "/tmp/jobs.parquet",
        "row_count_raw": 1,
        "row_count_runtime": 1,
        "quality_profile": "core_fields_weighted",
        "files": [],
    }
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ConfigValidationError, match="'files' must be an object"):
        load_eda_artifacts(root)

    payload["files"] = {}
    payload["rules_context"] = []
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ConfigValidationError, match="'rules_context' must be an object"):
        load_eda_artifacts(root)


def test_load_eda_artifacts_profile_and_diff_specific_errors(tmp_path: Path) -> None:
    root = tmp_path / "artifacts2"
    root.mkdir()
    manifest_path = root / "manifest.json"

    profile_payload = {
        "schema_version": "1.1",
        "artifact_kind": "profile",
        "generated_at_utc": "2026-01-01T00:00:00+00:00",
        "input_path": "/tmp/jobs.parquet",
        "row_count_raw": 1,
        "row_count_runtime": 1,
        "quality_profile": "core_fields_weighted",
        "files": {"summary_json": "summary.json"},
    }
    manifest_path.write_text(json.dumps(profile_payload), encoding="utf-8")
    with pytest.raises(ConfigValidationError, match="report_md"):
        load_eda_artifacts(root)

    profile_payload["files"]["report_md"] = "report.md"
    manifest_path.write_text(json.dumps(profile_payload), encoding="utf-8")
    with pytest.raises(ConfigValidationError, match="summary missing"):
        load_eda_artifacts(root)

    (root / "summary.json").write_text("{bad", encoding="utf-8")
    (root / "report.md").write_text("ok", encoding="utf-8")
    with pytest.raises(ConfigValidationError, match="invalid summary JSON"):
        load_eda_artifacts(root)

    diff_payload = {
        **profile_payload,
        "artifact_kind": "diff",
        "files": {},
    }
    manifest_path.write_text(json.dumps(diff_payload), encoding="utf-8")
    with pytest.raises(ConfigValidationError, match="diff_json"):
        load_eda_artifacts(root)

    diff_payload["files"]["diff_json"] = "diff.json"
    manifest_path.write_text(json.dumps(diff_payload), encoding="utf-8")
    with pytest.raises(ConfigValidationError, match="diff JSON missing"):
        load_eda_artifacts(root)

    (root / "diff.json").write_text("{bad", encoding="utf-8")
    with pytest.raises(ConfigValidationError, match="invalid diff JSON"):
        load_eda_artifacts(root)


def test_load_eda_artifacts_rejects_unknown_kind_and_missing_file_map(
    tmp_path: Path,
) -> None:
    root = tmp_path / "artifacts3"
    root.mkdir()
    manifest_path = root / "manifest.json"
    payload = {
        "schema_version": "1.1",
        "artifact_kind": "unknown",
        "generated_at_utc": "2026-01-01T00:00:00+00:00",
        "input_path": "/tmp/jobs.parquet",
        "row_count_raw": 1,
        "row_count_runtime": 1,
        "quality_profile": "core_fields_weighted",
        "files": {"x": "missing.file"},
    }
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ConfigValidationError, match="unsupported artifact_kind"):
        load_eda_artifacts(root)

    payload["artifact_kind"] = "profile"
    payload["files"] = {"summary_json": "summary.json", "report_md": "report.md", "x": "missing.file"}
    (root / "summary.json").write_text("{}", encoding="utf-8")
    (root / "report.md").write_text("ok", encoding="utf-8")
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ConfigValidationError, match="artifacts file missing"):
        load_eda_artifacts(root)


def test_generate_eda_artifacts_skips_missing_tables(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    input_path = tmp_path / "jobs.parquet"
    pl.DataFrame({"id": ["1"]}).write_parquet(input_path)

    summary = {
        "shape": {"raw": {"rows": 1}, "runtime": {"rows": 1}},
        "quality": {"profile": "core_fields_weighted"},
    }
    monkeypatch.setattr(
        "honestroles.eda.artifacts.build_eda_profile",
        lambda **_kwargs: EDAProfileResult(summary=summary, tables={}),
    )
    monkeypatch.setattr(
        "honestroles.eda.artifacts.write_chart_figures",
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(
        "honestroles.eda.artifacts.write_report_markdown",
        lambda *_args, **_kwargs: None,
    )

    manifest = generate_eda_artifacts(input_parquet=input_path, output_dir=tmp_path / "out")
    assert manifest.files["summary_json"] == "summary.json"


def test_generate_eda_diff_artifacts_skips_missing_tables(
    sample_parquet: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    baseline_dir = tmp_path / "baseline"
    candidate_dir = tmp_path / "candidate"
    generate_eda_artifacts(input_parquet=sample_parquet, output_dir=baseline_dir)
    generate_eda_artifacts(input_parquet=sample_parquet, output_dir=candidate_dir)

    def fake_build_diff(**_kwargs):
        return (
            {"gate_evaluation": {}},
            {},
            {},
            {
                "shape": {"input_path": "x", "raw": {"rows": 1}, "runtime": {"rows": 1}},
                "quality": {"profile": "core_fields_weighted"},
            },
        )

    monkeypatch.setattr("honestroles.eda.diff.build_eda_diff", fake_build_diff)
    manifest = generate_eda_diff_artifacts(
        baseline_dir=baseline_dir,
        candidate_dir=candidate_dir,
        output_dir=tmp_path / "diff",
    )
    assert manifest.artifact_kind == "diff"
