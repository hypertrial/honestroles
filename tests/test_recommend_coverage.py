from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from honestroles.cli import lineage, output
from honestroles.errors import ConfigValidationError
from honestroles.recommend import CandidateProfile, SalaryTargets, VisaWorkAuth
from honestroles.recommend import evaluation as eval_mod
from honestroles.recommend import feedback as feedback_mod
from honestroles.recommend import index as index_mod
from honestroles.recommend import matching as matching_mod
from honestroles.recommend import models as models_mod
from honestroles.recommend import parser as parser_mod
from honestroles.recommend import policy as policy_mod
from honestroles.recommend import scoring as scoring_mod


def test_recommend_policy_error_branches(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(ConfigValidationError, match="cannot read recommendation policy"):
        policy_mod.load_recommendation_policy(tmp_path / "missing.toml")

    invalid_toml = tmp_path / "invalid.toml"
    invalid_toml.write_text("[weights", encoding="utf-8")
    with pytest.raises(ConfigValidationError, match="invalid recommendation policy"):
        policy_mod.load_recommendation_policy(invalid_toml)

    root_override = tmp_path / "root_override.toml"
    root_override.write_text("x=1", encoding="utf-8")
    monkeypatch.setattr(policy_mod.tomllib, "loads", lambda _text: [])
    with pytest.raises(ConfigValidationError, match="root must be a TOML table"):
        policy_mod.load_recommendation_policy(root_override)

    monkeypatch.undo()

    non_table = tmp_path / "non_table.toml"
    non_table.write_text("weights = 1", encoding="utf-8")
    with pytest.raises(ConfigValidationError, match="weights must be a TOML table"):
        policy_mod.load_recommendation_policy(non_table)

    empty_key = tmp_path / "empty_key.toml"
    empty_key.write_text("[weights]\n\"\" = 1", encoding="utf-8")
    with pytest.raises(ConfigValidationError, match="keys must be non-empty"):
        policy_mod.load_recommendation_policy(empty_key)

    bool_weight = tmp_path / "bool_weight.toml"
    bool_weight.write_text("[weights]\nskills = true", encoding="utf-8")
    with pytest.raises(ConfigValidationError, match="must be numeric"):
        policy_mod.load_recommendation_policy(bool_weight)

    reason_bad = tmp_path / "reason_bad.toml"
    reason_bad.write_text("reason_limit = \"x\"", encoding="utf-8")
    with pytest.raises(ConfigValidationError, match="must be an integer"):
        policy_mod.load_recommendation_policy(reason_bad)

    weights_empty = tmp_path / "weights_empty.toml"
    weights_empty.write_text("weights = \"\"", encoding="utf-8")
    loaded_policy, _, _ = policy_mod.load_recommendation_policy(weights_empty)
    assert loaded_policy.weights

    with pytest.raises(ConfigValidationError, match="cannot read eval thresholds"):
        policy_mod.load_eval_thresholds(tmp_path / "missing_eval.toml")

    invalid_eval = tmp_path / "invalid_eval.toml"
    invalid_eval.write_text("ks = [", encoding="utf-8")
    with pytest.raises(ConfigValidationError, match="invalid eval thresholds"):
        policy_mod.load_eval_thresholds(invalid_eval)

    eval_root = tmp_path / "eval_root.toml"
    eval_root.write_text("x=1", encoding="utf-8")
    monkeypatch.setattr(policy_mod.tomllib, "loads", lambda _text: [])
    with pytest.raises(ConfigValidationError, match="root must be a TOML table"):
        policy_mod.load_eval_thresholds(eval_root)

    monkeypatch.undo()

    ks_non_list = tmp_path / "ks_non_list.toml"
    ks_non_list.write_text("ks = 1", encoding="utf-8")
    with pytest.raises(ConfigValidationError, match="array of integers"):
        policy_mod.load_eval_thresholds(ks_non_list)

    ks_bool = tmp_path / "ks_bool.toml"
    ks_bool.write_text("ks = [true]", encoding="utf-8")
    with pytest.raises(ConfigValidationError, match="entries must be integers"):
        policy_mod.load_eval_thresholds(ks_bool)

    ks_small = tmp_path / "ks_small.toml"
    ks_small.write_text("ks = [0]", encoding="utf-8")
    with pytest.raises(ConfigValidationError, match=">= 1"):
        policy_mod.load_eval_thresholds(ks_small)

    ks_dup = tmp_path / "ks_dup.toml"
    ks_dup.write_text("ks = [10, 10, 25]", encoding="utf-8")
    loaded_thresholds, _, _ = policy_mod.load_eval_thresholds(ks_dup)
    assert loaded_thresholds.ks == (10, 25)


def test_recommend_parser_error_branches(tmp_path: Path) -> None:
    with pytest.raises(ConfigValidationError, match="cannot read candidate JSON"):
        parser_mod.parse_candidate_json_file(tmp_path / "missing.json")

    bad_json = tmp_path / "bad.json"
    bad_json.write_text("{", encoding="utf-8")
    with pytest.raises(ConfigValidationError, match="invalid candidate JSON"):
        parser_mod.parse_candidate_json_file(bad_json)

    with pytest.raises(ConfigValidationError, match="resume text must be a string"):
        parser_mod.parse_resume_text(123)  # type: ignore[arg-type]

    parsed = parser_mod.parse_resume_text(
        "Hybrid on-site staff engineer with $120 and 8 years",
        profile_id="abc",
    )
    assert "hybrid" in parsed.work_mode_preferences
    assert parsed.salary_targets.minimum == 120000.0

    parsed2 = parser_mod.parse_resume_text(
        "Senior engineer with $120000 annual no sponsorship",
        profile_id="abc",
    )
    assert parsed2.salary_targets.minimum == 120000.0

    with pytest.raises(ConfigValidationError, match="candidate.profile_id must be non-empty"):
        parser_mod.parse_resume_text("python", profile_id="   ")

    with pytest.raises(ConfigValidationError, match="non-empty string"):
        parser_mod.parse_candidate_profile_payload({})

    with pytest.raises(ConfigValidationError, match="array of strings"):
        parser_mod._string_list("x", field="skills")

    with pytest.raises(ConfigValidationError, match="entries must be strings"):
        parser_mod._string_list([1], field="skills")

    assert parser_mod._string_list([" ", "Python"], field="skills") == ["python"]

    with pytest.raises(ConfigValidationError, match="must be numeric"):
        parser_mod._optional_float(True, field="years")

    with pytest.raises(ConfigValidationError, match="must be numeric"):
        parser_mod._optional_float("x", field="years")

    with pytest.raises(ConfigValidationError, match="must be numeric"):
        parser_mod._optional_float(object(), field="years")

    with pytest.raises(ConfigValidationError, match="currency must be a string"):
        parser_mod._parse_salary_targets({"currency": 1})

    with pytest.raises(ConfigValidationError, match="interval must be a string"):
        parser_mod._parse_salary_targets({"interval": 1})

    with pytest.raises(ConfigValidationError, match="must be boolean"):
        parser_mod._parse_visa_work_auth({"requires_sponsorship": "yes"})

    with pytest.raises(ConfigValidationError, match="array of strings"):
        parser_mod._parse_visa_work_auth({"authorized_locations": "us"})

    assert parser_mod._collect_unknown_resume_tokens("$120 sponsorship maybe") == [
        "visa_signal_ambiguous",
        "salary_interval_unspecified",
    ]
    assert parser_mod._parser_confidence(
        has_skills=False,
        has_titles=False,
        has_locations=False,
        has_years=False,
    ) == 0.25


def test_recommend_scoring_helper_branches() -> None:
    assert scoring_mod._skill_overlap_score(set(), set()) == 0.5
    assert scoring_mod._title_similarity_score(set(), set()) == 0.5
    assert scoring_mod._title_similarity_score({"+++"}, set()) == 0.0
    assert scoring_mod._title_similarity_score({"python"}, {"java"}) == 0.0

    candidate = CandidateProfile(profile_id="x", locations=("remote",), work_mode_preferences=("remote",))
    assert scoring_mod._location_work_mode_score(candidate, {"location": "remote", "work_mode": "remote"}) == 1.0
    partial_candidate = CandidateProfile(
        profile_id="x",
        locations=("remote",),
        work_mode_preferences=("onsite",),
    )
    assert scoring_mod._location_work_mode_score(partial_candidate, {"location": "berlin", "work_mode": "remote"}) == 0.5
    assert scoring_mod._location_work_mode_score(candidate, {"location": "berlin", "work_mode": "onsite"}) == 0.0

    assert scoring_mod._seniority_score(CandidateProfile(profile_id="x"), {"seniority": "senior"}) == 0.5
    assert scoring_mod._recency_score({"posted_at": None}) == 0.0

    no_target = CandidateProfile(profile_id="x")
    assert scoring_mod._compensation_score(no_target, {"salary_min": 1}) == 0.5
    with_target = CandidateProfile(profile_id="x", salary_targets=SalaryTargets(minimum=100000))
    assert scoring_mod._compensation_score(with_target, {"salary_min": None, "salary_max": None}) == 0.0
    assert scoring_mod._compensation_score(with_target, {"salary_min": 50000, "salary_max": None}) == 0.5
    zero_target = CandidateProfile(profile_id="x", salary_targets=SalaryTargets(minimum=0))
    assert scoring_mod._compensation_score(zero_target, {"salary_min": -1}) == 0.0

    only_auth = CandidateProfile(profile_id="x", visa_work_auth=VisaWorkAuth(authorized_locations=("us",)))
    assert scoring_mod._location_eligible(only_auth, {"location": "US Remote", "work_mode": "remote"})
    assert not scoring_mod._location_eligible(only_auth, {"location": "Berlin", "work_mode": "onsite"})

    assert scoring_mod._work_mode_eligible(CandidateProfile(profile_id="x"), {"work_mode": "onsite"})
    assert scoring_mod._quality_flags({}) == (
        "MISSING_COMPANY",
        "MISSING_POSTED_AT",
        "MISSING_DESCRIPTION",
    )

    assert scoring_mod._normalize_skills(None) == []
    assert scoring_mod._normalize_skills("python,python,sql") == ["python", "sql"]
    assert scoring_mod._normalize_skills(5) == []

    assert scoring_mod._normalize_timestamp(" ") is None
    assert scoring_mod._normalize_timestamp("not-a-time") == "not-a-time"
    assert scoring_mod._parse_datetime(None) is None
    assert scoring_mod._parse_datetime(" ") is None
    assert scoring_mod._parse_datetime("2026-03-01T00:00:00Z") is not None

    assert scoring_mod._infer_work_mode({"work_mode": "hybrid"}) == "hybrid"
    assert scoring_mod._infer_work_mode({"remote": False, "location": "On-site"}) == "onsite"
    assert scoring_mod._infer_work_mode({"location": "Hybrid"}) == "hybrid"
    assert scoring_mod._infer_work_mode({}) == "unknown"

    assert scoring_mod._infer_seniority("principal engineer") == "staff"
    assert scoring_mod._infer_seniority("senior engineer") == "senior"
    assert scoring_mod._infer_seniority("intern") == "junior"
    assert scoring_mod._infer_seniority(None) == "mid"

    assert scoring_mod._to_float(True) is None
    assert scoring_mod._to_float("  ") is None
    assert scoring_mod._coerce_bool(None) is None

    normalized = scoring_mod.normalize_job_record(
        {
            "title": "Data Engineer",
            "company": "Acme",
            "location": "Remote",
            "remote": "yes",
            "description_text": "python",
            "skills": "python,sql",
            "employment_type": "full-time",
            "posted_at": "2026-03-01",
        }
    )
    assert normalized["job_id"]


def test_recommend_index_and_matching_helpers(tmp_path: Path) -> None:
    root = tmp_path / "idx"
    root.mkdir()
    with pytest.raises(ConfigValidationError, match="manifest not found"):
        index_mod.load_index(root)

    (root / "manifest.json").write_text("{}", encoding="utf-8")
    with pytest.raises(ConfigValidationError, match="jobs file not found"):
        index_mod.load_index(root)

    (root / "jobs_latest.jsonl").write_text("\n[]\n{\"job_id\":\"1\"}\n", encoding="utf-8")
    manifest, jobs = index_mod.load_index(root)
    assert manifest == {}
    assert jobs == [{"job_id": "1"}]

    from collections import Counter

    facet_counter = Counter()
    index_mod._increment_facet(facet_counter, None)
    index_mod._increment_facet(facet_counter, " ")
    index_mod._increment_facet(facet_counter, "X")
    assert facet_counter["x"] == 1
    quality = index_mod._quality_summary([{"company": " ", "posted_at": "2026-01-01", "description_text": "x"}])
    assert quality["company_non_null_pct"] == 0.0

    assert matching_mod._sortable_date(None) == ""
    assert matching_mod._sortable_date("2026-01-01T00:00:00Z") == "2026-01-01T00:00:00+00:00"
    assert matching_mod._posted_sort_value(None) == float("-inf")
    assert matching_mod._posted_sort_value("not-a-date") == float("-inf")
    assert matching_mod._posted_sort_value("2026-01-01T00:00:00") > 0
    assert matching_mod._text_or_none(" ") is None

    with pytest.raises(ConfigValidationError, match="top-k"):
        matching_mod.match_jobs_with_profile(index_dir=root, candidate=CandidateProfile(profile_id="x"), top_k=0)


def test_recommend_feedback_branches(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    summary_empty = feedback_mod.summarize_feedback()
    assert summary_empty.total_events == 0

    # load_profile_weights payload non-dict + non-numeric entries
    path = feedback_mod.weights_file("abc")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("[]", encoding="utf-8")
    assert feedback_mod.load_profile_weights("abc")["skills"] == 1.0
    path.write_text(json.dumps({"skills": "x", "title": 2.0}), encoding="utf-8")
    weights = feedback_mod.load_profile_weights("abc")
    assert weights["skills"] == 1.0
    assert weights["title"] == 1.5

    events = feedback_mod.events_file()
    events.parent.mkdir(parents=True, exist_ok=True)
    events.write_text("\n[]\n{}\n{\"profile_id\":\"\",\"event\":\"\"}\n", encoding="utf-8")
    summary = feedback_mod.summarize_feedback()
    assert summary.total_events == 0

    bad_meta = tmp_path / "bad_meta.json"
    bad_meta.write_text("{", encoding="utf-8")
    with pytest.raises(ConfigValidationError, match="invalid feedback meta JSON"):
        feedback_mod._load_meta(bad_meta)

    root_meta = tmp_path / "root_meta.json"
    root_meta.write_text("[]", encoding="utf-8")
    with pytest.raises(ConfigValidationError, match="root must be an object"):
        feedback_mod._load_meta(root_meta)

    assert feedback_mod._event_hash_exists(tmp_path / "missing.jsonl", "x") is False
    events.write_text("\n[]\n{}\n", encoding="utf-8")
    assert feedback_mod._event_hash_exists(events, "x") is False

    assert feedback_mod._count_events(tmp_path / "none.jsonl") == 0
    blank_events = tmp_path / "blank_events.jsonl"
    blank_events.write_text("\n{}\n\n", encoding="utf-8")
    assert feedback_mod._count_events(blank_events) == 1
    with pytest.raises(ConfigValidationError, match="non-empty string"):
        feedback_mod._required_text(" ", field="profile_id")
    assert feedback_mod._clamp_weight(99.0) == 1.5
    assert feedback_mod._clamp_weight(0.1) == 0.5


def test_recommend_evaluation_error_and_branch_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with pytest.raises(ConfigValidationError, match="cannot read golden set"):
        eval_mod.evaluate_relevance(index_dir=tmp_path, golden_set=tmp_path / "missing.json")

    invalid = tmp_path / "invalid.json"
    invalid.write_text("{", encoding="utf-8")
    with pytest.raises(ConfigValidationError, match="invalid golden set JSON"):
        eval_mod.evaluate_relevance(index_dir=tmp_path, golden_set=invalid)

    no_cases = tmp_path / "no_cases.json"
    no_cases.write_text(json.dumps({"cases": {}}), encoding="utf-8")
    with pytest.raises(ConfigValidationError, match="must include cases"):
        eval_mod.evaluate_relevance(index_dir=tmp_path, golden_set=no_cases)

    valid = tmp_path / "valid.json"
    valid.write_text(
        json.dumps(
            {
                "cases": [
                    "skip-me",
                    {
                        "candidate": {"profile_id": "p", "skills": ["python"]},
                        "relevant_job_ids": ["job-a", "job-b"],
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        eval_mod,
        "load_eval_thresholds",
        lambda _path: (
            models_mod.EvalThresholds(ks=(25,), precision_at_10_min=0.0, recall_at_25_min=1.0),
            "builtin",
            "x" * 64,
        ),
    )
    monkeypatch.setattr(
        eval_mod,
        "match_jobs_with_profile",
        lambda **_kwargs: SimpleNamespace(results=[SimpleNamespace(job_id="job-a")]),
    )
    result = eval_mod.evaluate_relevance(index_dir=tmp_path, golden_set=valid)
    assert result.status == "fail"
    assert "EVAL_RECALL_AT_25_BELOW_THRESHOLD" in result.check_codes

    monkeypatch.setattr(
        eval_mod,
        "load_eval_thresholds",
        lambda _path: (
            models_mod.EvalThresholds(ks=(), precision_at_10_min=0.0, recall_at_25_min=0.0),
            "builtin",
            "x" * 64,
        ),
    )
    with pytest.raises(ConfigValidationError, match="ks must be non-empty"):
        eval_mod.evaluate_relevance(index_dir=tmp_path, golden_set=valid)


def test_recommend_lineage_output_and_model_helpers(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert models_mod.utc_now_iso()

    assert lineage.build_artifact_paths(
        {"command": "recommend", "recommend_command": "build-index", "output_dir": ""},
        None,
    ) == {}

    build_paths = lineage.build_artifact_paths(
        {"command": "recommend", "recommend_command": "build-index", "output_dir": ""},
        {"index_dir": str(tmp_path / "idx")},
    )
    assert build_paths["index_dir"].endswith("idx")
    assert lineage.build_artifact_paths(
        {"command": "recommend", "recommend_command": "match"},
        None,
    ) == {}
    assert lineage.build_artifact_paths(
        {
            "command": "recommend",
            "recommend_command": "feedback",
            "recommend_feedback_command": "add",
            "profile_id": "",
        },
        None,
    )["events_file"].endswith("events.jsonl")
    assert lineage.build_artifact_paths(
        {
            "command": "recommend",
            "recommend_command": "feedback",
            "recommend_feedback_command": "add",
            "profile_id": "jane",
        },
        None,
    )["weights_file"].endswith("jane.json")
    assert lineage.build_artifact_paths(
        {
            "command": "recommend",
            "recommend_command": "feedback",
            "recommend_feedback_command": "summarize",
        },
        None,
    )["events_file"].endswith("events.jsonl")

    output.emit_payload(
        {
            "status": "pass",
            "index_id": "abc",
            "jobs_count": 1,
            "token_count": 1,
            "shard_count": 1,
        },
        "table",
    )
    assert "INDEX" in capsys.readouterr().out

    output.emit_payload(
        {
            "status": "pass",
            "eligible_count": 1,
            "excluded_count": 0,
            "total_jobs": 1,
            "top_k": 1,
            "results": "not-list",
        },
        "table",
    )
    assert "MATCH" not in capsys.readouterr().out

    output._print_recommend_match_table({"results": "not-list"})
    assert "MATCH profile" in capsys.readouterr().out

    output.emit_payload(
        {
            "status": "pass",
            "cases_evaluated": 0,
            "metrics": "not-map",
            "failing_checks": [],
        },
        "table",
    )
    assert "EVAL" not in capsys.readouterr().out

    output._print_recommend_eval_table(
        {
            "cases_evaluated": 0,
            "status": "pass",
            "metrics": {},
            "failing_checks": "not-list",
        }
    )
    eval_table = capsys.readouterr().out
    assert "EVAL cases=0 status=pass" in eval_table

    output._print_recommend_eval_table(
        {
            "cases_evaluated": 1,
            "status": "pass",
            "metrics": "not-map",
            "failing_checks": [],
        }
    )
    assert "EVAL cases=1 status=pass" in capsys.readouterr().out
