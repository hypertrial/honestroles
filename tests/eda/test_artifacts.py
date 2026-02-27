from __future__ import annotations

from pathlib import Path

import pytest

from honestroles.eda import generate_eda_artifacts, load_eda_artifacts
from honestroles.errors import ConfigValidationError


def test_generate_eda_artifacts_and_load(sample_parquet: Path, tmp_path: Path) -> None:
    output_dir = tmp_path / "eda"
    manifest = generate_eda_artifacts(
        input_parquet=sample_parquet,
        output_dir=output_dir,
        quality_profile="core_fields_weighted",
        top_k=5,
    )

    assert manifest.schema_version == "1.0"
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
    assert bundle.summary["shape"]["runtime"]["rows"] == 3


def test_load_eda_artifacts_requires_manifest(tmp_path: Path) -> None:
    missing = tmp_path / "missing_artifacts"
    missing.mkdir(parents=True, exist_ok=True)

    with pytest.raises(ConfigValidationError, match="manifest"):
        load_eda_artifacts(missing)
