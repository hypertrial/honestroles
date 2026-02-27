from __future__ import annotations

import json
import importlib
import subprocess
from pathlib import Path

import pytest

import polars as pl

from honestroles.eda import generate_eda_artifacts
from honestroles.eda.models import EDAArtifactsBundle, EDAArtifactsManifest
from honestroles.errors import ConfigValidationError, HonestRolesError, StageExecutionError
from honestroles.cli.main import main


def _cli_main_module():
    return importlib.import_module("honestroles.cli.main")


def test_cli_run(pipeline_config_path: Path, plugin_manifest_path: Path) -> None:
    code = main(
        [
            "run",
            "--pipeline-config",
            str(pipeline_config_path),
            "--plugins",
            str(plugin_manifest_path),
        ]
    )
    assert code == 0


def test_cli_plugins_validate(plugin_manifest_path: Path) -> None:
    code = main(["plugins", "validate", "--manifest", str(plugin_manifest_path)])
    assert code == 0


def test_cli_config_validate(pipeline_config_path: Path) -> None:
    code = main(["config", "validate", "--pipeline", str(pipeline_config_path)])
    assert code == 0


def test_cli_report_quality(pipeline_config_path: Path, plugin_manifest_path: Path) -> None:
    code = main(
        [
            "report-quality",
            "--pipeline-config",
            str(pipeline_config_path),
            "--plugins",
            str(plugin_manifest_path),
        ]
    )
    assert code == 0


def test_cli_report_quality_includes_profile_metadata(
    pipeline_config_path: Path,
    plugin_manifest_path: Path,
    capsys,
) -> None:
    code = main(
        [
            "report-quality",
            "--pipeline-config",
            str(pipeline_config_path),
            "--plugins",
            str(plugin_manifest_path),
        ]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert "profile" in payload
    assert "weighted_null_percent" in payload
    assert "effective_weights" in payload
    assert payload["profile"] == "core_fields_weighted"


def test_cli_report_quality_score_uses_weighted_profile(tmp_path: Path, capsys) -> None:
    parquet_path = tmp_path / "jobs.parquet"

    pl.DataFrame({"a": [1, None], "b": [None, None], "title": ["x", "y"], "description_text": ["d", "d"]}).write_parquet(parquet_path)

    pipeline_path = tmp_path / "pipeline.toml"
    pipeline_path.write_text(
        f"""
[input]
kind = "parquet"
path = "{parquet_path}"

[stages.clean]
enabled = false

[stages.filter]
enabled = false

[stages.label]
enabled = false

[stages.rate]
enabled = false

[stages.match]
enabled = false

[runtime.quality]
profile = "equal_weight_all"
""".strip(),
        encoding="utf-8",
    )

    code = main(["report-quality", "--pipeline-config", str(pipeline_path)])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["profile"] == "equal_weight_all"
    # equal weight across output columns includes a (50 null) and b (100 null), score must be < 100
    assert payload["score_percent"] < 100.0


def test_cli_exit_code_for_bad_config(tmp_path: Path) -> None:
    bad_path = tmp_path / "missing.toml"
    code = main(["config", "validate", "--pipeline", str(bad_path)])
    assert code == 2


def test_cli_run_bad_config_returns_config_exit(tmp_path: Path) -> None:
    bad_path = tmp_path / "missing_pipeline.toml"
    code = main(["run", "--pipeline-config", str(bad_path)])
    assert code == 2


def test_cli_run_bad_plugin_manifest_returns_plugin_exit(
    pipeline_config_path: Path,
    tmp_path: Path,
) -> None:
    manifest = tmp_path / "bad_plugins.toml"
    manifest.write_text(
        """
[[plugins]]
name = "bad"
kind = "label"
callable = "nope.module:missing"
""".strip(),
        encoding="utf-8",
    )
    code = main(
        [
            "run",
            "--pipeline-config",
            str(pipeline_config_path),
            "--plugins",
            str(manifest),
        ]
    )
    assert code == 3


def test_cli_scaffold_plugin(tmp_path: Path) -> None:
    code = main(
        [
            "scaffold-plugin",
            "--name",
            "my-demo-plugin",
            "--output-dir",
            str(tmp_path),
        ]
    )
    assert code == 0
    assert (tmp_path / "my-demo-plugin").exists()


def test_cli_scaffold_plugin_target_exists_returns_config_error(tmp_path: Path) -> None:
    target = tmp_path / "my-demo-plugin"
    target.mkdir()
    code = main(
        [
            "scaffold-plugin",
            "--name",
            "my-demo-plugin",
            "--output-dir",
            str(tmp_path),
        ]
    )
    assert code == 2


def test_cli_scaffold_plugin_invalid_name_returns_config_error(tmp_path: Path) -> None:
    code = main(
        [
            "scaffold-plugin",
            "--name",
            "bad!name",
            "--output-dir",
            str(tmp_path),
        ]
    )
    assert code == 2


def test_cli_scaffold_plugin_numeric_name_returns_config_error(tmp_path: Path) -> None:
    code = main(
        [
            "scaffold-plugin",
            "--name",
            "1plugin",
            "--output-dir",
            str(tmp_path),
        ]
    )
    assert code == 2


def test_cli_adapter_infer_writes_draft_and_report(tmp_path: Path, capsys) -> None:
    parquet_path = tmp_path / "jobs.parquet"
    output_file = tmp_path / "adapter.toml"
    pl.DataFrame(
        {
            "job_location": ["Remote", "NYC"],
            "is_remote": ["true", "false"],
            "date_posted": ["2026-01-01", "2026-01-02"],
        }
    ).write_parquet(parquet_path)

    code = main(
        [
            "adapter",
            "infer",
            "--input-parquet",
            str(parquet_path),
            "--output-file",
            str(output_file),
            "--sample-rows",
            "100",
            "--top-candidates",
            "2",
            "--min-confidence",
            "0.5",
        ]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["adapter_draft"] == str(output_file.resolve())
    assert Path(payload["adapter_draft"]).exists()
    assert Path(payload["inference_report"]).exists()
    assert payload["field_suggestions"] >= 1


def test_cli_adapter_infer_invalid_input_returns_config_error(tmp_path: Path) -> None:
    code = main(
        [
            "adapter",
            "infer",
            "--input-parquet",
            str(tmp_path / "missing.parquet"),
        ]
    )
    assert code == 2


def test_cli_adapter_infer_invalid_thresholds_return_config_error(
    sample_parquet: Path,
) -> None:
    code = main(
        [
            "adapter",
            "infer",
            "--input-parquet",
            str(sample_parquet),
            "--sample-rows",
            "0",
        ]
    )
    assert code == 2

    code = main(
        [
            "adapter",
            "infer",
            "--input-parquet",
            str(sample_parquet),
            "--top-candidates",
            "0",
        ]
    )
    assert code == 2

    code = main(
        [
            "adapter",
            "infer",
            "--input-parquet",
            str(sample_parquet),
            "--min-confidence",
            "2",
        ]
    )
    assert code == 2


def test_cli_adapter_infer_print_fragment(sample_parquet: Path, tmp_path: Path, capsys) -> None:
    output_file = tmp_path / "adapter.toml"
    code = main(
        [
            "adapter",
            "infer",
            "--input-parquet",
            str(sample_parquet),
            "--output-file",
            str(output_file),
            "--print",
        ]
    )
    assert code == 0
    out = capsys.readouterr().out
    assert "[input.adapter]" in out


def test_cli_template_resolution_packaged_fallback(monkeypatch) -> None:
    cli_main = _cli_main_module()

    repo_template = Path(cli_main.__file__).resolve().parents[3] / "plugin_template"
    packaged_template = (
        Path(cli_main.__file__).resolve().parents[1] / "_templates" / "plugin_template"
    )
    original_exists = Path.exists

    def fake_exists(self: Path) -> bool:
        if self == repo_template:
            return False
        if self == packaged_template:
            return True
        return original_exists(self)

    monkeypatch.setattr(Path, "exists", fake_exists)
    resolved = cli_main._resolve_plugin_template_root()
    assert resolved == packaged_template


def test_cli_template_resolution_hard_failure(monkeypatch) -> None:
    cli_main = _cli_main_module()

    repo_template = Path(cli_main.__file__).resolve().parents[3] / "plugin_template"
    packaged_template = (
        Path(cli_main.__file__).resolve().parents[1] / "_templates" / "plugin_template"
    )
    original_exists = Path.exists

    def fake_exists(self: Path) -> bool:
        if self in {repo_template, packaged_template}:
            return False
        return original_exists(self)

    monkeypatch.setattr(Path, "exists", fake_exists)
    with pytest.raises(ConfigValidationError):
        cli_main._resolve_plugin_template_root()


def test_cli_stage_execution_error_mapping(monkeypatch) -> None:
    cli_main = _cli_main_module()

    def fail(_args):
        raise StageExecutionError("filter", "boom")

    monkeypatch.setattr(cli_main, "_handle_run", fail)
    code = cli_main.main(["run", "--pipeline-config", "x.toml"])
    assert code == 4


def test_cli_generic_honestroles_error_mapping(monkeypatch) -> None:
    cli_main = _cli_main_module()

    def fail(_args):
        raise HonestRolesError("oops")

    monkeypatch.setattr(cli_main, "_handle_run", fail)
    code = cli_main.main(["run", "--pipeline-config", "x.toml"])
    assert code == 1


def test_cli_parser_fallback_unhandled_command(monkeypatch) -> None:
    import argparse
    cli_main = _cli_main_module()

    class FakeParser:
        def parse_args(self, _argv):
            return argparse.Namespace(command="unknown")

        def error(self, _msg):
            return None

    monkeypatch.setattr(cli_main, "build_parser", lambda: FakeParser())
    code = cli_main.main([])
    assert code == 1


def test_cli_eda_generate(sample_parquet: Path, tmp_path: Path, capsys) -> None:
    output_dir = tmp_path / "eda_artifacts"
    code = main(
        [
            "eda",
            "generate",
            "--input-parquet",
            str(sample_parquet),
            "--output-dir",
            str(output_dir),
            "--top-k",
            "5",
        ]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["artifacts_dir"] == str(output_dir.resolve())
    assert (output_dir / "manifest.json").exists()
    assert (output_dir / "summary.json").exists()
    assert (output_dir / "report.md").exists()


def test_cli_eda_dashboard_missing_artifacts_returns_config_error(tmp_path: Path) -> None:
    code = main(
        [
            "eda",
            "dashboard",
            "--artifacts-dir",
            str(tmp_path / "missing"),
        ]
    )
    assert code == 2


def test_cli_eda_dashboard_invalid_diff_dir_returns_config_error(
    sample_parquet: Path, tmp_path: Path
) -> None:
    profile_dir = tmp_path / "profile"
    generate_eda_artifacts(input_parquet=sample_parquet, output_dir=profile_dir)
    code = main(
        [
            "eda",
            "dashboard",
            "--artifacts-dir",
            str(profile_dir),
            "--diff-dir",
            str(tmp_path / "missing_diff"),
        ]
    )
    assert code == 2


def test_cli_eda_dashboard_requires_streamlit(
    sample_parquet: Path, tmp_path: Path, monkeypatch
) -> None:
    output_dir = tmp_path / "eda_artifacts"
    generate_eda_artifacts(input_parquet=sample_parquet, output_dir=output_dir)

    cli_main = _cli_main_module()
    monkeypatch.setattr(cli_main.importlib.util, "find_spec", lambda _name: None)

    code = cli_main.main(
        [
            "eda",
            "dashboard",
            "--artifacts-dir",
            str(output_dir),
        ]
    )
    assert code == 2


def test_cli_eda_dashboard_launches_streamlit(tmp_path: Path, monkeypatch) -> None:
    cli_main = _cli_main_module()
    manifest = EDAArtifactsManifest(
        schema_version="1.1",
        artifact_kind="profile",
        generated_at_utc="2026-02-27T00:00:00+00:00",
        input_path="/tmp/jobs.parquet",
        row_count_raw=1,
        row_count_runtime=1,
        quality_profile="core_fields_weighted",
        files={"summary_json": "summary.json", "report_md": "report.md"},
    )
    bundle = EDAArtifactsBundle(artifacts_dir=tmp_path, manifest=manifest, summary={})

    monkeypatch.setattr(cli_main, "load_eda_artifacts", lambda _path: bundle)
    monkeypatch.setattr(cli_main.importlib.util, "find_spec", lambda _name: object())

    calls: dict[str, list[str]] = {}

    def fake_run(cmd, check=False):
        calls["cmd"] = cmd
        return subprocess.CompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(cli_main.subprocess, "run", fake_run)

    code = cli_main.main(
        [
            "eda",
            "dashboard",
            "--artifacts-dir",
            str(tmp_path),
            "--host",
            "0.0.0.0",
            "--port",
            "9000",
        ]
    )
    assert code == 0
    assert calls["cmd"][1:4] == ["-m", "streamlit", "run"]
    assert "--artifacts-dir" in calls["cmd"]


def test_cli_eda_dashboard_launches_streamlit_with_diff(tmp_path: Path, monkeypatch) -> None:
    cli_main = _cli_main_module()
    profile_manifest = EDAArtifactsManifest(
        schema_version="1.1",
        artifact_kind="profile",
        generated_at_utc="2026-02-27T00:00:00+00:00",
        input_path="/tmp/jobs.parquet",
        row_count_raw=1,
        row_count_runtime=1,
        quality_profile="core_fields_weighted",
        files={"summary_json": "summary.json", "report_md": "report.md"},
    )
    diff_manifest = EDAArtifactsManifest(
        schema_version="1.1",
        artifact_kind="diff",
        generated_at_utc="2026-02-27T00:00:00+00:00",
        input_path="/tmp/jobs.parquet",
        row_count_raw=1,
        row_count_runtime=1,
        quality_profile="core_fields_weighted",
        files={"diff_json": "diff.json"},
    )
    profile_bundle = EDAArtifactsBundle(artifacts_dir=tmp_path / "profile", manifest=profile_manifest, summary={})
    diff_bundle = EDAArtifactsBundle(artifacts_dir=tmp_path / "diff", manifest=diff_manifest, diff={})

    def fake_load(path):
        return diff_bundle if Path(path) == diff_bundle.artifacts_dir else profile_bundle

    monkeypatch.setattr(cli_main, "load_eda_artifacts", fake_load)
    monkeypatch.setattr(cli_main.importlib.util, "find_spec", lambda _name: object())

    calls: dict[str, list[str]] = {}

    def fake_run(cmd, check=False):
        calls["cmd"] = cmd
        return subprocess.CompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(cli_main.subprocess, "run", fake_run)
    code = cli_main.main(
        [
            "eda",
            "dashboard",
            "--artifacts-dir",
            str(profile_bundle.artifacts_dir),
            "--diff-dir",
            str(diff_bundle.artifacts_dir),
        ]
    )
    assert code == 0
    assert "--diff-dir" in calls["cmd"]


def test_cli_eda_dashboard_profile_and_diff_validation_errors(tmp_path: Path, monkeypatch) -> None:
    cli_main = _cli_main_module()
    profile_manifest = EDAArtifactsManifest(
        schema_version="1.1",
        artifact_kind="profile",
        generated_at_utc="2026-02-27T00:00:00+00:00",
        input_path="/tmp/jobs.parquet",
        row_count_raw=1,
        row_count_runtime=1,
        quality_profile="core_fields_weighted",
        files={"summary_json": "summary.json", "report_md": "report.md"},
    )
    diff_manifest = EDAArtifactsManifest(
        schema_version="1.1",
        artifact_kind="diff",
        generated_at_utc="2026-02-27T00:00:00+00:00",
        input_path="/tmp/jobs.parquet",
        row_count_raw=1,
        row_count_runtime=1,
        quality_profile="core_fields_weighted",
        files={"diff_json": "diff.json"},
    )
    bad_profile = EDAArtifactsBundle(artifacts_dir=tmp_path / "profile", manifest=profile_manifest, summary=None)
    bad_diff = EDAArtifactsBundle(artifacts_dir=tmp_path / "diff", manifest=diff_manifest, diff=None)

    monkeypatch.setattr(cli_main.importlib.util, "find_spec", lambda _name: object())
    monkeypatch.setattr(cli_main.subprocess, "run", lambda cmd, check=False: subprocess.CompletedProcess(args=cmd, returncode=0))

    monkeypatch.setattr(cli_main, "load_eda_artifacts", lambda _p: bad_profile)
    code = cli_main.main(["eda", "dashboard", "--artifacts-dir", str(tmp_path / "profile")])
    assert code == 2

    def fake_load(path):
        if Path(path) == bad_diff.artifacts_dir:
            return bad_diff
        return EDAArtifactsBundle(
            artifacts_dir=tmp_path / "profile_ok",
            manifest=profile_manifest,
            summary={},
        )

    monkeypatch.setattr(cli_main, "load_eda_artifacts", fake_load)
    code = cli_main.main(
        [
            "eda",
            "dashboard",
            "--artifacts-dir",
            str(tmp_path / "profile_ok"),
            "--diff-dir",
            str(bad_diff.artifacts_dir),
        ]
    )
    assert code == 2


def test_cli_eda_generate_invalid_quality_weight_returns_config_error(
    sample_parquet: Path, tmp_path: Path
) -> None:
    code = main(
        [
            "eda",
            "generate",
            "--input-parquet",
            str(sample_parquet),
            "--output-dir",
            str(tmp_path / "eda"),
            "--quality-weight",
            "posted_at=-1",
        ]
    )
    assert code == 2


def test_cli_eda_diff(sample_parquet: Path, tmp_path: Path, capsys) -> None:
    baseline_dir = tmp_path / "baseline"
    candidate_dir = tmp_path / "candidate"
    diff_dir = tmp_path / "diff"
    generate_eda_artifacts(input_parquet=sample_parquet, output_dir=baseline_dir)
    generate_eda_artifacts(input_parquet=sample_parquet, output_dir=candidate_dir)

    code = main(
        [
            "eda",
            "diff",
            "--baseline-dir",
            str(baseline_dir),
            "--candidate-dir",
            str(candidate_dir),
            "--output-dir",
            str(diff_dir),
        ]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert Path(payload["manifest"]).exists()
    assert Path(payload["diff_json"]).exists()


def test_cli_eda_gate_passes_without_failures(sample_parquet: Path, tmp_path: Path) -> None:
    candidate_dir = tmp_path / "candidate"
    generate_eda_artifacts(input_parquet=sample_parquet, output_dir=candidate_dir)

    code = main(
        [
            "eda",
            "gate",
            "--candidate-dir",
            str(candidate_dir),
        ]
    )
    assert code == 0


def test_cli_eda_gate_fails_with_baseline_drift(tmp_path: Path) -> None:
    baseline_parquet = tmp_path / "baseline.parquet"
    candidate_parquet = tmp_path / "candidate.parquet"

    pl.DataFrame(
        {
            "id": ["1", "2"],
            "source": ["lever", "lever"],
            "title": ["Engineer", "Engineer"],
            "company": ["A", "A"],
            "location_raw": ["Remote", "Remote"],
            "remote_flag": [True, True],
            "description_text": ["x", "x"],
            "apply_url": ["u1", "u2"],
            "posted_at": ["2026-01-01", "2026-01-02"],
            "salary_min": [100000.0, 120000.0],
            "salary_max": [150000.0, 170000.0],
        }
    ).write_parquet(baseline_parquet)

    pl.DataFrame(
        {
            "id": ["1", "2"],
            "source": ["lever", "lever"],
            "title": ["Engineer", "Engineer"],
            "company": ["A", "A"],
            "location_raw": ["Unknown", "Unknown"],
            "remote_flag": [False, False],
            "description_text": ["x", "x"],
            "apply_url": ["u1", "u2"],
            "posted_at": ["2026-01-01", "2026-01-02"],
            "salary_min": [250000.0, 280000.0],
            "salary_max": [300000.0, 320000.0],
        }
    ).write_parquet(candidate_parquet)

    baseline_dir = tmp_path / "baseline_artifacts"
    candidate_dir = tmp_path / "candidate_artifacts"
    generate_eda_artifacts(input_parquet=baseline_parquet, output_dir=baseline_dir)
    generate_eda_artifacts(input_parquet=candidate_parquet, output_dir=candidate_dir)

    rules_file = tmp_path / "rules.toml"
    rules_file.write_text(
        """
[drift]
numeric_warn_psi = 0.01
numeric_fail_psi = 0.02
categorical_warn_jsd = 0.01
categorical_fail_jsd = 0.02
""".strip(),
        encoding="utf-8",
    )

    code = main(
        [
            "eda",
            "gate",
            "--candidate-dir",
            str(candidate_dir),
            "--baseline-dir",
            str(baseline_dir),
            "--rules-file",
            str(rules_file),
        ]
    )
    assert code == 1


def test_cli_eda_gate_requires_profile_candidate(tmp_path: Path) -> None:
    baseline_parquet = tmp_path / "baseline.parquet"
    candidate_parquet = tmp_path / "candidate.parquet"
    pl.DataFrame({"id": ["1"], "title": ["x"], "company": ["y"], "description_text": ["z"]}).write_parquet(
        baseline_parquet
    )
    pl.DataFrame({"id": ["1"], "title": ["x"], "company": ["y"], "description_text": ["z"]}).write_parquet(
        candidate_parquet
    )
    baseline_profile = tmp_path / "baseline_profile"
    candidate_profile = tmp_path / "candidate_profile"
    generate_eda_artifacts(input_parquet=baseline_parquet, output_dir=baseline_profile)
    generate_eda_artifacts(input_parquet=candidate_parquet, output_dir=candidate_profile)
    diff_candidate = tmp_path / "candidate_diff"
    from honestroles.eda import generate_eda_diff_artifacts

    generate_eda_diff_artifacts(
        baseline_dir=baseline_profile,
        candidate_dir=candidate_profile,
        output_dir=diff_candidate,
    )

    code = main(["eda", "gate", "--candidate-dir", str(diff_candidate)])
    assert code == 2


def test_cli_scaffold_plugin_skips_rename_when_package_matches_template(tmp_path: Path) -> None:
    code = main(
        [
            "scaffold-plugin",
            "--name",
            "honestroles-plugin-example",
            "--output-dir",
            str(tmp_path),
        ]
    )
    assert code == 0
