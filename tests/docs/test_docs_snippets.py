from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from honestroles.cli.main import main
from honestroles.plugins.registry import PluginRegistry


def _write_sample_parquet(path: Path) -> None:
    pl.DataFrame(
        {
            "id": ["1", "2", "3"],
            "title": ["Data Engineer", "Senior ML Engineer", "Intern Analyst"],
            "company": ["A", "B", "C"],
            "location": ["Remote", "NYC", "Remote"],
            "remote": ["true", "false", "1"],
            "description_text": [
                "Python SQL data pipelines",
                "Build ML systems with Python and AWS",
                "Excel and reporting",
            ],
            "description_html": ["<p>Python SQL</p>", "<b>ML</b>", "<i>intern</i>"],
            "skills": ["python,sql", "python,aws", None],
            "salary_min": [120000, 180000, None],
            "salary_max": [160000, 220000, None],
            "apply_url": ["https://example.com/1", "https://example.com/2", "https://example.com/3"],
            "posted_at": ["2026-01-01", "2026-01-02", "2026-01-03"],
        }
    ).write_parquet(path)


def _write_pipeline(
    path: Path,
    input_path: Path,
    output_path: Path,
    *,
    fail_fast: bool,
    top_k: int,
) -> None:
    text = f"""
[input]
kind = "parquet"
path = "{input_path}"

[output]
path = "{output_path}"

[stages.clean]
enabled = true

[stages.filter]
enabled = true
remote_only = false

[stages.label]
enabled = true

[stages.rate]
enabled = true

[stages.match]
enabled = true
top_k = {top_k}

[runtime]
fail_fast = {str(fail_fast).lower()}
random_seed = 0
""".strip()
    path.write_text(text, encoding="utf-8")


def test_docs_first_run_cli_snippet(tmp_path: Path, capsys) -> None:
    input_path = tmp_path / "jobs.parquet"
    output_path = tmp_path / "jobs_scored.parquet"
    pipeline_path = tmp_path / "pipeline.toml"
    manifest_path = tmp_path / "plugins.toml"

    _write_sample_parquet(input_path)
    _write_pipeline(pipeline_path, input_path, output_path, fail_fast=True, top_k=10)

    manifest_path.write_text(
        """
[[plugins]]
name = "label_note"
kind = "label"
callable = "tests.plugins.fixture_plugins:label_note"
enabled = true
order = 1
""".strip(),
        encoding="utf-8",
    )

    code = main(
        [
            "run",
            "--pipeline-config",
            str(pipeline_path),
            "--plugins",
            str(manifest_path),
        ]
    )
    assert code == 0

    payload = json.loads(capsys.readouterr().out)
    assert {"stage_rows", "plugin_counts", "runtime", "final_rows"}.issubset(payload.keys())
    assert output_path.exists()


def test_docs_invalid_top_k_config_validation(tmp_path: Path) -> None:
    pipeline_path = tmp_path / "bad_pipeline.toml"
    pipeline_path.write_text(
        """
[input]
kind = "parquet"
path = "./jobs.parquet"

[stages.match]
enabled = true
top_k = 0
""".strip(),
        encoding="utf-8",
    )

    code = main(["config", "validate", "--pipeline", str(pipeline_path)])
    assert code == 2


def test_docs_fail_fast_and_recovery_behavior(tmp_path: Path, capsys) -> None:
    input_path = tmp_path / "jobs.parquet"
    output_path = tmp_path / "jobs_scored.parquet"
    pipeline_fail_fast_path = tmp_path / "pipeline_fail_fast.toml"
    pipeline_recovery_path = tmp_path / "pipeline_recovery.toml"
    manifest_path = tmp_path / "plugins.toml"

    _write_sample_parquet(input_path)
    _write_pipeline(
        pipeline_fail_fast_path,
        input_path,
        output_path,
        fail_fast=True,
        top_k=10,
    )
    _write_pipeline(
        pipeline_recovery_path,
        input_path,
        output_path,
        fail_fast=False,
        top_k=10,
    )

    manifest_path.write_text(
        """
[[plugins]]
name = "failing_filter"
kind = "filter"
callable = "tests.plugins.fixture_plugins:fail_filter"
enabled = true
order = 1
""".strip(),
        encoding="utf-8",
    )

    fail_fast_code = main(
        [
            "run",
            "--pipeline-config",
            str(pipeline_fail_fast_path),
            "--plugins",
            str(manifest_path),
        ]
    )
    assert fail_fast_code == 3
    _ = capsys.readouterr()

    recovery_code = main(
        [
            "run",
            "--pipeline-config",
            str(pipeline_recovery_path),
            "--plugins",
            str(manifest_path),
        ]
    )
    assert recovery_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert "non_fatal_errors" in payload
    assert payload["non_fatal_errors"][0]["stage"] == "filter"


def test_docs_plugin_manifest_disabled_and_ordering(tmp_path: Path) -> None:
    manifest_path = tmp_path / "plugins.toml"
    manifest_path.write_text(
        """
[[plugins]]
name = "label_b"
kind = "label"
callable = "tests.plugins.fixture_plugins:label_note"
enabled = true
order = 20

[[plugins]]
name = "label_a"
kind = "label"
callable = "tests.plugins.fixture_plugins:label_note"
enabled = true
order = 20

[[plugins]]
name = "label_disabled"
kind = "label"
callable = "tests.plugins.fixture_plugins:label_note"
enabled = false
order = 0
""".strip(),
        encoding="utf-8",
    )

    registry = PluginRegistry.from_manifest(manifest_path)
    assert registry.list("label") == ("label_a", "label_b")
    assert "label_disabled" not in registry.list("label")
