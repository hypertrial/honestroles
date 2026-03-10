from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest

from honestroles.cli import lineage, output
from honestroles.cli.main import build_parser, main


def _jobs_parquet(path: Path) -> Path:
    pl.DataFrame(
        {
            "id": ["1", "2"],
            "title": ["Senior Data Engineer", "Frontend Engineer"],
            "company": ["Acme", "Beta"],
            "location": ["Remote", "Lisbon"],
            "remote": [True, False],
            "description_text": ["python sql aws", "react typescript"],
            "description_html": ["<p>python</p>", "<p>react</p>"],
            "skills": [["python", "sql"], ["react", "typescript"]],
            "salary_min": [150000, 90000],
            "salary_max": [180000, 120000],
            "apply_url": ["https://jobs/1", "https://jobs/2"],
            "posted_at": ["2026-03-01", "2026-03-02"],
            "source": ["greenhouse", "lever"],
            "source_ref": ["stripe", "plaid"],
            "source_job_id": ["g-1", "l-2"],
            "job_url": ["https://jobs/1", "https://jobs/2"],
            "source_updated_at": ["2026-03-01", "2026-03-02"],
            "work_mode": ["remote", "onsite"],
            "salary_currency": ["USD", "EUR"],
            "salary_interval": ["year", "year"],
            "employment_type": ["full_time", "contract"],
            "seniority": ["senior", "mid"],
        }
    ).write_parquet(path)
    return path


def test_recommend_parser_commands() -> None:
    parser = build_parser()

    args = parser.parse_args(["recommend", "build-index", "--input-parquet", "jobs.parquet"])
    assert args.command == "recommend"
    assert args.recommend_command == "build-index"

    args2 = parser.parse_args(
        [
            "recommend",
            "match",
            "--index-dir",
            "dist/recommend/index/x",
            "--candidate-json",
            "candidate.json",
            "--include-excluded",
        ]
    )
    assert args2.recommend_command == "match"
    assert args2.include_excluded is True

    args3 = parser.parse_args(
        [
            "recommend",
            "feedback",
            "add",
            "--profile-id",
            "jane",
            "--job-id",
            "1",
            "--event",
            "applied",
        ]
    )
    assert args3.recommend_command == "feedback"
    assert args3.recommend_feedback_command == "add"


def test_recommend_cli_main_end_to_end(tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    runs_root = tmp_path / ".honestroles" / "runs"
    monkeypatch.setattr("honestroles.cli.lineage.runs_root", lambda: runs_root)

    parquet = _jobs_parquet(tmp_path / "jobs.parquet")

    code_build = main(
        [
            "recommend",
            "build-index",
            "--input-parquet",
            str(parquet),
        ]
    )
    assert code_build == 0
    build_payload = json.loads(capsys.readouterr().out)
    index_dir = Path(build_payload["index_dir"])

    candidate = tmp_path / "candidate.json"
    candidate.write_text(
        json.dumps(
            {
                "profile_id": "jane",
                "skills": ["python", "sql"],
                "titles": ["data engineer"],
                "locations": ["remote"],
                "work_mode_preferences": ["remote"],
                "seniority_targets": ["senior"],
                "salary_targets": {"minimum": 120000},
                "employment_type_preferences": ["full_time"],
            }
        ),
        encoding="utf-8",
    )

    code_match = main(
        [
            "recommend",
            "match",
            "--index-dir",
            str(index_dir),
            "--candidate-json",
            str(candidate),
            "--include-excluded",
            "--format",
            "table",
        ]
    )
    assert code_match == 0
    table_out = capsys.readouterr().out
    assert "MATCH profile" in table_out

    golden = tmp_path / "golden.json"
    golden.write_text(
        json.dumps(
            {
                "cases": [
                    {
                        "candidate": {
                            "profile_id": "jane",
                            "skills": ["python", "sql"],
                            "titles": ["data engineer"],
                            "locations": ["remote"],
                            "work_mode_preferences": ["remote"],
                        },
                        "relevant_job_ids": ["g-1"],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    fail_thresholds = tmp_path / "fail.toml"
    fail_thresholds.write_text("precision_at_10_min=1.0\nrecall_at_25_min=1.0\nks=[10,25]", encoding="utf-8")

    code_eval = main(
        [
            "recommend",
            "evaluate",
            "--index-dir",
            str(index_dir),
            "--golden-set",
            str(golden),
            "--thresholds",
            str(fail_thresholds),
        ]
    )
    assert code_eval == 1
    eval_payload = json.loads(capsys.readouterr().out)
    assert eval_payload["status"] == "fail"

    code_feedback = main(
        [
            "recommend",
            "feedback",
            "add",
            "--profile-id",
            "jane",
            "--job-id",
            "g-1",
            "--event",
            "interviewed",
        ]
    )
    assert code_feedback == 0
    feedback_payload = json.loads(capsys.readouterr().out)
    assert feedback_payload["event"] == "interviewed"

    code_summary = main(
        [
            "recommend",
            "feedback",
            "summarize",
            "--profile-id",
            "jane",
            "--format",
            "table",
        ]
    )
    assert code_summary == 0
    summary_table = capsys.readouterr().out
    assert "FEEDBACK SUMMARY" in summary_table

    run_files = sorted(runs_root.glob("*/run.json"))
    assert run_files
    commands = {json.loads(path.read_text(encoding="utf-8"))["command"] for path in run_files}
    assert "recommend.build-index" in commands
    assert "recommend.match" in commands
    assert "recommend.evaluate" in commands
    assert "recommend.feedback.add" in commands
    assert "recommend.feedback.summarize" in commands


def test_recommend_lineage_helpers(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("honestroles.cli.lineage.runs_root", lambda: tmp_path / ".honestroles" / "runs")

    assert lineage._command_key({"command": "recommend", "recommend_command": "build-index"}) == "recommend.build-index"
    assert (
        lineage._command_key(
            {
                "command": "recommend",
                "recommend_command": "feedback",
                "recommend_feedback_command": "add",
            }
        )
        == "recommend.feedback.add"
    )
    assert lineage.should_track({"command": "recommend", "recommend_command": "evaluate"})

    args = {
        "command": "recommend",
        "recommend_command": "build-index",
        "input_parquet": str(tmp_path / "jobs.parquet"),
        "output_dir": str(tmp_path / "idx"),
    }
    (tmp_path / "jobs.parquet").write_bytes(b"x")
    input_hash, input_hashes, config_hash = lineage.compute_hashes(args)
    assert input_hash is None
    assert "input_parquet" in input_hashes
    assert len(config_hash) == 64

    artifacts = lineage.build_artifact_paths(args, None)
    assert artifacts["manifest_file"].endswith("manifest.json")

    record = lineage.create_record(
        args={"command": "recommend", "recommend_command": "evaluate"},
        exit_code=1,
        started_at=datetime(2026, 1, 1, tzinfo=UTC),
        finished_at=datetime(2026, 1, 1, tzinfo=UTC),
        payload={
            "cases_evaluated": 2,
            "metrics": {"precision_at_10": 0.1},
            "thresholds": {"precision_at_10_min": 0.6},
            "failing_checks": ["precision_at_10"],
            "check_codes": ["EVAL_PRECISION_AT_10_BELOW_THRESHOLD"],
        },
    )
    assert record["recommend_metrics"] is not None
    assert record["status"] == "fail"


def test_recommend_output_table_paths(capsys: pytest.CaptureFixture[str]) -> None:
    output.emit_payload(
        {
            "status": "pass",
            "index_id": "abc",
            "jobs_count": 10,
            "token_count": 20,
            "shard_count": 16,
            "index_dir": "x",
            "manifest_file": "m",
            "jobs_file": "j",
            "facets_file": "f",
            "quality_summary_file": "q",
        },
        "table",
    )
    index_out = capsys.readouterr().out
    assert "INDEX" in index_out

    output.emit_payload(
        {
            "status": "pass",
            "eligible_count": 2,
            "excluded_count": 1,
            "total_jobs": 3,
            "top_k": 2,
            "profile": {"profile_id": "jane"},
            "results": [
                {"job_id": "1", "score": 0.9, "source": "x", "posted_at": "2026-01-01"},
                "bad",
            ],
        },
        "table",
    )
    match_out = capsys.readouterr().out
    assert "MATCH profile" in match_out

    output.emit_payload(
        {
            "status": "fail",
            "cases_evaluated": 1,
            "metrics": {"precision_at_10": 0.1},
            "failing_checks": ["precision_at_10"],
        },
        "table",
    )
    eval_out = capsys.readouterr().out
    assert "EVAL" in eval_out

    output.emit_payload(
        {
            "status": "pass",
            "profile_id": "jane",
            "event": "applied",
            "duplicate": False,
            "total_events": 2,
            "weights": {"skills": 1.0},
        },
        "table",
    )
    feedback_out = capsys.readouterr().out
    assert "FEEDBACK profile" in feedback_out

    output.emit_payload(
        {
            "status": "pass",
            "profile_id": "jane",
            "profile_counts": {"jane": {"applied": 1}},
            "total_events": 1,
            "counts": {"applied": 1},
            "weights": None,
        },
        "table",
    )
    summary_out = capsys.readouterr().out
    assert "FEEDBACK SUMMARY" in summary_out
