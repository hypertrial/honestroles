from __future__ import annotations

import argparse
import sys
from pathlib import Path

import polars as pl
import pytest

from honestroles.eda.dashboard_app import _parse_args, main
from honestroles.eda.models import EDAArtifactsBundle, EDAArtifactsManifest


class _DummyCol:
    def metric(self, *_args, **_kwargs) -> None:
        return None


class _DummyStreamlit:
    def set_page_config(self, **_kwargs) -> None:
        return None

    def title(self, *_args, **_kwargs) -> None:
        return None

    def caption(self, *_args, **_kwargs) -> None:
        return None

    def columns(self, count: int):
        return tuple(_DummyCol() for _ in range(count))

    def subheader(self, *_args, **_kwargs) -> None:
        return None

    def write(self, *_args, **_kwargs) -> None:
        return None

    def markdown(self, *_args, **_kwargs) -> None:
        return None

    def image(self, *_args, **_kwargs) -> None:
        return None

    def dataframe(self, *_args, **_kwargs) -> None:
        return None

    def json(self, *_args, **_kwargs) -> None:
        return None


def _profile_bundle(root: Path) -> EDAArtifactsBundle:
    (root / "figures").mkdir(parents=True, exist_ok=True)
    (root / "tables").mkdir(parents=True, exist_ok=True)
    (root / "figures" / "nulls_by_column.png").write_bytes(b"x")
    pl.DataFrame({"x": [1]}).write_parquet(root / "tables" / "null_percentages.parquet")
    manifest = EDAArtifactsManifest(
        schema_version="1.1",
        artifact_kind="profile",
        generated_at_utc="2026-01-01T00:00:00+00:00",
        input_path="/tmp/jobs.parquet",
        row_count_raw=1,
        row_count_runtime=1,
        quality_profile="core_fields_weighted",
        files={
            "nulls_by_column": "figures/nulls_by_column.png",
            "top_locations": "figures/missing.png",
            "null_percentages": "tables/null_percentages.parquet",
            "top_values_source": "tables/missing.parquet",
        },
    )
    summary = {
        "shape": {"raw": {"rows": 1}, "runtime": {"rows": 1}},
        "quality": {"score_percent": 90.0, "weighted_null_percent": 10.0},
        "findings": [
            {
                "severity": "P1",
                "title": "x",
                "detail": "y",
                "recommendation": "z",
            }
        ],
    }
    return EDAArtifactsBundle(artifacts_dir=root, manifest=manifest, summary=summary)


def _diff_bundle(root: Path) -> EDAArtifactsBundle:
    (root / "tables").mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"x": [1]}).write_parquet(root / "tables" / "drift_metrics.parquet")
    pl.DataFrame({"x": [1]}).write_parquet(root / "tables" / "findings_delta.parquet")
    manifest = EDAArtifactsManifest(
        schema_version="1.1",
        artifact_kind="diff",
        generated_at_utc="2026-01-01T00:00:00+00:00",
        input_path="/tmp/jobs.parquet",
        row_count_raw=1,
        row_count_runtime=1,
        quality_profile="core_fields_weighted",
        files={
            "drift_metrics": "tables/drift_metrics.parquet",
            "findings_delta": "tables/findings_delta.parquet",
        },
    )
    diff = {
        "gate_evaluation": {"status": "pass", "failures": [], "warnings": []},
        "shape_diff": {},
        "quality_diff": {},
    }
    return EDAArtifactsBundle(artifacts_dir=root, manifest=manifest, diff=diff)


def test_parse_args(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        sys, "argv", ["dashboard", "--artifacts-dir", "/tmp/a", "--diff-dir", "/tmp/b"]
    )
    args = _parse_args()
    assert args.artifacts_dir == "/tmp/a"
    assert args.diff_dir == "/tmp/b"


def test_dashboard_main_profile_and_diff(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    profile = _profile_bundle(tmp_path / "profile")
    diff = _diff_bundle(tmp_path / "diff")

    monkeypatch.setitem(sys.modules, "streamlit", _DummyStreamlit())
    monkeypatch.setattr(
        "honestroles.eda.dashboard_app._parse_args",
        lambda: argparse.Namespace(
            artifacts_dir=str(profile.artifacts_dir),
            diff_dir=str(diff.artifacts_dir),
        ),
    )

    def fake_load(path: Path):
        if Path(path) == diff.artifacts_dir:
            return diff
        return profile

    monkeypatch.setattr("honestroles.eda.dashboard_app.load_eda_artifacts", fake_load)
    main()


def test_dashboard_main_requires_profile_summary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest = EDAArtifactsManifest(
        schema_version="1.1",
        artifact_kind="profile",
        generated_at_utc="2026-01-01T00:00:00+00:00",
        input_path="/tmp/jobs.parquet",
        row_count_raw=1,
        row_count_runtime=1,
        quality_profile="core_fields_weighted",
        files={},
    )
    bundle = EDAArtifactsBundle(artifacts_dir=tmp_path, manifest=manifest, summary=None)
    monkeypatch.setitem(sys.modules, "streamlit", _DummyStreamlit())
    monkeypatch.setattr(
        "honestroles.eda.dashboard_app._parse_args",
        lambda: argparse.Namespace(artifacts_dir=str(tmp_path), diff_dir=None),
    )
    monkeypatch.setattr("honestroles.eda.dashboard_app.load_eda_artifacts", lambda _p: bundle)

    with pytest.raises(ValueError, match="profile artifacts"):
        main()


def test_dashboard_main_with_no_findings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    profile = _profile_bundle(tmp_path / "profile_empty")
    assert profile.summary is not None
    profile.summary["findings"] = []
    monkeypatch.setitem(sys.modules, "streamlit", _DummyStreamlit())
    monkeypatch.setattr(
        "honestroles.eda.dashboard_app._parse_args",
        lambda: argparse.Namespace(artifacts_dir=str(profile.artifacts_dir), diff_dir=None),
    )
    monkeypatch.setattr("honestroles.eda.dashboard_app.load_eda_artifacts", lambda _p: profile)
    main()


def test_dashboard_main_diff_paths_optional(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    profile = _profile_bundle(tmp_path / "profile")
    diff = _diff_bundle(tmp_path / "diff")
    diff.manifest.files.pop("drift_metrics")
    assert diff.manifest.files["findings_delta"]
    (diff.artifacts_dir / diff.manifest.files["findings_delta"]).unlink()

    monkeypatch.setitem(sys.modules, "streamlit", _DummyStreamlit())
    monkeypatch.setattr(
        "honestroles.eda.dashboard_app._parse_args",
        lambda: argparse.Namespace(
            artifacts_dir=str(profile.artifacts_dir),
            diff_dir=str(diff.artifacts_dir),
        ),
    )

    def fake_load(path: Path):
        if Path(path) == diff.artifacts_dir:
            return diff
        return profile

    monkeypatch.setattr("honestroles.eda.dashboard_app.load_eda_artifacts", fake_load)
    main()


def test_dashboard_main_diff_paths_with_missing_drift_and_no_findings_delta(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    profile = _profile_bundle(tmp_path / "profile2")
    diff = _diff_bundle(tmp_path / "diff2")
    assert diff.manifest.files["drift_metrics"]
    (diff.artifacts_dir / diff.manifest.files["drift_metrics"]).unlink()
    diff.manifest.files.pop("findings_delta")

    monkeypatch.setitem(sys.modules, "streamlit", _DummyStreamlit())
    monkeypatch.setattr(
        "honestroles.eda.dashboard_app._parse_args",
        lambda: argparse.Namespace(
            artifacts_dir=str(profile.artifacts_dir),
            diff_dir=str(diff.artifacts_dir),
        ),
    )

    def fake_load(path: Path):
        if Path(path) == diff.artifacts_dir:
            return diff
        return profile

    monkeypatch.setattr("honestroles.eda.dashboard_app.load_eda_artifacts", fake_load)
    main()
