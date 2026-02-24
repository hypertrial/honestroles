from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd


def _env_with_pythonpath(repo_root: Path) -> dict[str, str]:
    env = dict(os.environ)
    env["PYTHONPATH"] = str(repo_root / "src")
    return env


def test_report_data_quality_script_json_output(tmp_path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    script = repo_root / "scripts" / "report_data_quality.py"
    parquet_path = tmp_path / "jobs.parquet"
    pd.DataFrame(
        [
            {
                "job_key": "acme::greenhouse::1",
                "company": "Acme",
                "source": "greenhouse",
                "job_id": "1",
                "title": "Engineer",
                "location_raw": "Remote",
                "apply_url": "https://example.com/apply",
                "ingested_at": "2025-01-01T00:00:00Z",
                "content_hash": "h1",
                "description_text": "Build systems.",
            }
        ]
    ).to_parquet(parquet_path, index=False)

    result = subprocess.run(
        [sys.executable, str(script), str(parquet_path), "--format", "json"],
        capture_output=True,
        text=True,
        check=False,
        env=_env_with_pythonpath(repo_root),
    )
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["row_count"] == 1
    assert "required_field_null_counts" in payload


def test_report_data_quality_script_stream_mode(tmp_path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    script = repo_root / "scripts" / "report_data_quality.py"
    parquet_path = tmp_path / "jobs_stream.parquet"
    pd.DataFrame(
        [
            {
                "job_key": "acme::greenhouse::1",
                "company": "Acme",
                "source": "greenhouse",
                "job_id": "1",
                "title": "Engineer",
                "location_raw": "Unknown",
                "apply_url": "https://example.com/apply",
                "ingested_at": "2025-01-01T00:00:00Z",
                "content_hash": "h1",
                "description_text": "Build systems.",
            },
            {
                "job_key": "acme::greenhouse::1",
                "company": "Acme",
                "source": "greenhouse",
                "job_id": "1",
                "title": "Engineer",
                "location_raw": "Unknown",
                "apply_url": "https://example.com/apply",
                "ingested_at": "2025-01-02T00:00:00Z",
                "content_hash": "h1",
                "description_text": "Build systems.",
            },
        ]
    ).to_parquet(parquet_path, index=False, row_group_size=1)

    result = subprocess.run(
        [
            sys.executable,
            str(script),
            str(parquet_path),
            "--format",
            "json",
            "--stream",
            "--top-n-duplicates",
            "5",
        ],
        capture_output=True,
        text=True,
        check=False,
        env=_env_with_pythonpath(repo_root),
    )
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["row_count"] == 2
    assert payload["top_duplicate_job_keys"][0]["count"] == 2


def test_report_data_quality_script_missing_input_fails(tmp_path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    script = repo_root / "scripts" / "report_data_quality.py"
    missing_path = tmp_path / "missing.duckdb"

    result = subprocess.run(
        [sys.executable, str(script), str(missing_path), "--format", "json"],
        capture_output=True,
        text=True,
        check=False,
        env=_env_with_pythonpath(repo_root),
    )
    assert result.returncode != 0
    assert "Input file not found" in result.stderr
