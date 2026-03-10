from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
from pathlib import Path

import polars as pl
import pytest

from honestroles.errors import ConfigValidationError
from honestroles.recommend import (
    CandidateProfile,
    SalaryTargets,
    VisaWorkAuth,
    build_retrieval_index,
    evaluate_relevance,
    load_eval_thresholds,
    load_recommendation_policy,
    match_jobs,
    match_jobs_with_profile,
    parse_candidate_json_file,
    parse_candidate_profile_payload,
    parse_resume_text,
    parse_resume_text_file,
    record_feedback_event,
    summarize_feedback,
)
from honestroles.recommend import evaluation as eval_mod
from honestroles.recommend import feedback as feedback_mod
from honestroles.recommend import index as index_mod
from honestroles.recommend import parser as parser_mod
from honestroles.recommend import policy as policy_mod
from honestroles.recommend import scoring as scoring_mod
from honestroles.recommend.models import EvalThresholds, RecommendationPolicy


def _recommend_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "id": ["1", "2", "3"],
            "title": ["Senior Data Engineer", "Frontend Engineer", "Intern Analyst"],
            "company": ["Acme", "Beta", None],
            "location": ["Remote, US", "Lisbon", "New York"],
            "remote": [True, False, False],
            "description_text": [
                "Python SQL AWS ETL pipelines. Sponsorship available.",
                "React TypeScript frontend product role.",
                None,
            ],
            "description_html": ["<p>Python SQL AWS</p>", "<p>React</p>", None],
            "skills": [["python", "sql", "aws"], ["react", "typescript"], None],
            "salary_min": [150000, 90000, None],
            "salary_max": [190000, 120000, None],
            "apply_url": ["https://jobs.example/1", "https://jobs.example/2", "https://jobs.example/3"],
            "posted_at": ["2026-03-09T00:00:00+00:00", "2025-03-01", None],
            "source": ["greenhouse", "lever", "ashby"],
            "source_ref": ["stripe", "plaid", "notion"],
            "source_job_id": ["g-1", "l-2", "a-3"],
            "job_url": ["https://jobs.example/1", "https://jobs.example/2", "https://jobs.example/3"],
            "source_updated_at": ["2026-03-09", "2025-03-01", None],
            "work_mode": ["remote", "onsite", "unknown"],
            "salary_currency": ["USD", "EUR", None],
            "salary_interval": ["year", "year", None],
            "employment_type": ["full_time", "contract", None],
            "seniority": ["senior", "mid", "junior"],
        }
    )


def _write_index(tmp_path: Path) -> tuple[Path, Path]:
    parquet = tmp_path / "jobs.parquet"
    _recommend_df().write_parquet(parquet)
    result = build_retrieval_index(input_parquet=parquet)
    return parquet, Path(result.index_dir)


def test_parse_candidate_profile_json_and_payload(tmp_path: Path) -> None:
    payload = {
        "profile_id": " Jane_Doe ",
        "skills": ["Python", "SQL", "python"],
        "titles": ["Data Engineer", "DATA ENGINEER"],
        "years_experience": "6",
        "locations": ["Remote", "United States"],
        "work_mode_preferences": ["remote"],
        "seniority_targets": ["senior"],
        "salary_targets": {"minimum": 130000, "currency": "usd", "interval": "YEAR"},
        "visa_work_auth": {"requires_sponsorship": False, "authorized_locations": ["us"]},
        "employment_type_preferences": ["full_time"],
    }
    path = tmp_path / "candidate.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    profile = parse_candidate_json_file(path)
    assert profile.profile_id == "jane_doe"
    assert profile.skills == ("python", "sql")
    assert profile.titles == ("data engineer",)
    assert profile.years_experience == 6.0
    assert profile.salary_targets.currency == "USD"
    assert profile.salary_targets.interval == "year"

    parsed = parse_candidate_profile_payload(payload)
    assert parsed.employment_type_preferences == ("full_time",)

    with pytest.raises(ConfigValidationError, match="candidate.work_mode_preferences"):
        parse_candidate_profile_payload({"profile_id": "x", "work_mode_preferences": ["space"]})

    with pytest.raises(ConfigValidationError, match="candidate.salary_targets"):
        parse_candidate_profile_payload({"profile_id": "x", "salary_targets": [1]})

    with pytest.raises(ConfigValidationError, match="candidate.visa_work_auth"):
        parse_candidate_profile_payload({"profile_id": "x", "visa_work_auth": [1]})


@pytest.mark.parametrize(
    ("text", "expected_skill", "expected_sponsorship"),
    [
        ("Senior Data Engineer, 7 years, Python SQL, remote, requires sponsorship", "python", True),
        ("Frontend engineer with React in Lisbon, 4 years, no sponsorship", "react", False),
    ],
)
def test_parse_resume_text_variants(text: str, expected_skill: str, expected_sponsorship: bool) -> None:
    profile = parse_resume_text(text, profile_id="CandidateX")
    assert profile.profile_id == "candidatex"
    assert expected_skill in profile.skills
    assert profile.visa_work_auth.requires_sponsorship is expected_sponsorship
    assert profile.parser_confidence is not None


def test_parse_resume_text_file_and_errors(tmp_path: Path) -> None:
    resume = tmp_path / "resume.txt"
    resume.write_text("Data engineer 5 years Python SQL remote", encoding="utf-8")
    profile = parse_resume_text_file(resume)
    assert profile.profile_id == "resume"

    with pytest.raises(ConfigValidationError, match="resume text is empty"):
        parse_resume_text("   ")
    with pytest.raises(ConfigValidationError, match="cannot read resume text"):
        parse_resume_text_file(tmp_path / "missing.txt")
    with pytest.raises(ConfigValidationError, match="candidate JSON root"):
        bad = tmp_path / "bad.json"
        bad.write_text("[]", encoding="utf-8")
        parse_candidate_json_file(bad)


def test_load_recommendation_policy_and_thresholds(tmp_path: Path) -> None:
    default_policy, source, digest = load_recommendation_policy(None)
    assert source == "builtin"
    assert len(digest) == 64
    assert round(sum(default_policy.normalized_weights().values()), 6) == 1.0

    policy_file = tmp_path / "recommendation.toml"
    policy_file.write_text(
        """
reason_limit = 2

[weights]
skills = 0.7
recency = 0.3
""".strip(),
        encoding="utf-8",
    )
    loaded_policy, _, _ = load_recommendation_policy(policy_file)
    assert loaded_policy.reason_limit == 2
    assert loaded_policy.normalized_weights()["skills"] > 0.6

    with pytest.raises(ConfigValidationError, match="must be >= 0"):
        bad = tmp_path / "bad.toml"
        bad.write_text("[weights]\nskills = -1", encoding="utf-8")
        load_recommendation_policy(bad)

    with pytest.raises(ConfigValidationError, match="reason_limit"):
        bad2 = tmp_path / "bad2.toml"
        bad2.write_text("reason_limit = 0", encoding="utf-8")
        load_recommendation_policy(bad2)

    thresholds, t_source, t_hash = load_eval_thresholds(None)
    assert isinstance(thresholds, EvalThresholds)
    assert t_source == "builtin"
    assert len(t_hash) == 64

    thresholds_file = tmp_path / "thresholds.toml"
    thresholds_file.write_text("ks=[5,10]\nprecision_at_10_min=0.5\nrecall_at_25_min=0.6", encoding="utf-8")
    loaded, _, _ = load_eval_thresholds(thresholds_file)
    assert loaded.ks == (5, 10)

    with pytest.raises(ConfigValidationError, match="between 0 and 1"):
        bad_t = tmp_path / "bad_t.toml"
        bad_t.write_text("precision_at_10_min = 2", encoding="utf-8")
        load_eval_thresholds(bad_t)


def test_build_retrieval_index_and_load(tmp_path: Path) -> None:
    parquet = tmp_path / "jobs.parquet"
    _recommend_df().write_parquet(parquet)

    result = build_retrieval_index(input_parquet=parquet)
    payload = result.to_payload()
    assert payload["jobs_count"] == 3
    assert Path(result.manifest_file).exists()
    assert Path(result.jobs_file).exists()
    assert Path(result.facets_file).exists()
    assert Path(result.quality_summary_file or "").exists()

    manifest, jobs = index_mod.load_index(result.index_dir)
    assert manifest["index_id"] == result.index_id
    assert len(jobs) == 3

    custom = tmp_path / "custom_index"
    custom_result = build_retrieval_index(input_parquet=parquet, output_dir=custom)
    assert Path(custom_result.index_dir) == custom.resolve()

    with pytest.raises(ConfigValidationError, match="input parquet does not exist"):
        build_retrieval_index(input_parquet=tmp_path / "missing.parquet")

    assert index_mod._quality_summary([])["row_count"] == 0
    shards_dir = tmp_path / "shards"
    shards_dir.mkdir(parents=True, exist_ok=True)
    assert len(index_mod._write_shards({}, shards_dir)) == 16


def test_scoring_and_filter_paths() -> None:
    candidate = CandidateProfile(
        profile_id="jane",
        skills=("python", "sql"),
        titles=("data engineer",),
        locations=("remote", "united states"),
        work_mode_preferences=("remote",),
        seniority_targets=("senior",),
        salary_targets=SalaryTargets(minimum=120000),
        visa_work_auth=VisaWorkAuth(requires_sponsorship=True),
        employment_type_preferences=("full_time",),
    )

    job = scoring_mod.normalize_job_record(_recommend_df().to_dicts()[0])
    reasons = scoring_mod.filter_job(candidate, job)
    assert reasons == ()

    score, match_reasons, missing, quality_flags, signal_values = scoring_mod.score_job(
        candidate=candidate,
        job=job,
        policy=RecommendationPolicy(),
        multipliers={key: 1.0 for key in scoring_mod.SIGNAL_KEYS},
    )
    assert 0.0 <= score <= 1.0
    assert match_reasons
    assert signal_values["skills"] >= 0.5
    assert "MISSING_COMPANY" not in quality_flags
    assert "python" not in missing

    blocked_job = dict(job)
    blocked_job["description_text"] = "No sponsorship offered."
    assert "FILTER_VISA" in scoring_mod.filter_job(candidate, blocked_job)

    mismatch_job = dict(job)
    mismatch_job["employment_type"] = "contract"
    mismatch_job["salary_min"] = 90000
    mismatch_job["salary_max"] = 100000
    mismatch_job["work_mode"] = "onsite"
    mismatch_job["location"] = "Berlin"
    assert set(scoring_mod.filter_job(candidate, mismatch_job)) == {
        "FILTER_LOCATION",
        "FILTER_WORK_MODE",
        "FILTER_EMPLOYMENT_TYPE",
        "FILTER_SALARY",
    }

    assert scoring_mod._parse_datetime("bad") is None
    assert scoring_mod._normalize_employment_type("full-time") == "full_time"
    assert scoring_mod._normalize_employment_type("weird") == "unknown"
    assert scoring_mod._coerce_bool("yes") is True
    assert scoring_mod._coerce_bool("no") is False
    assert scoring_mod._coerce_bool("maybe") is None
    assert scoring_mod._to_float("x") is None
    assert scoring_mod.tokenize_text("AB BC") == {"ab", "bc"}


def test_match_jobs_and_match_jobs_with_profile(tmp_path: Path) -> None:
    _parquet, index_dir = _write_index(tmp_path)

    candidate_json = tmp_path / "candidate.json"
    candidate_json.write_text(
        json.dumps(
            {
                "profile_id": "jane",
                "skills": ["python", "sql"],
                "titles": ["data engineer"],
                "locations": ["remote", "us"],
                "work_mode_preferences": ["remote"],
                "seniority_targets": ["senior"],
                "salary_targets": {"minimum": 120000},
                "visa_work_auth": {"requires_sponsorship": True},
                "employment_type_preferences": ["full_time"],
            }
        ),
        encoding="utf-8",
    )

    result = match_jobs(
        index_dir=index_dir,
        candidate_json=candidate_json,
        include_excluded=True,
        top_k=5,
    )
    payload = result.to_payload()
    assert payload["eligible_count"] >= 1
    assert payload["excluded_count"] >= 1
    assert payload["results"][0]["job_id"]

    resume = tmp_path / "resume.txt"
    resume.write_text("Data engineer 6 years python sql remote no sponsorship", encoding="utf-8")
    result2 = match_jobs(
        index_dir=index_dir,
        resume_text=resume,
        profile_id="override_profile",
        top_k=2,
    )
    assert result2.profile.profile_id == "override_profile"

    profile = parse_candidate_json_file(candidate_json)
    direct = match_jobs_with_profile(index_dir=index_dir, candidate=profile, top_k=1)
    assert len(direct.results) == 1

    with pytest.raises(ConfigValidationError, match="exactly one"):
        match_jobs(index_dir=index_dir, candidate_json=candidate_json, resume_text=resume)
    with pytest.raises(ConfigValidationError, match="top-k"):
        match_jobs(index_dir=index_dir, candidate_json=candidate_json, top_k=0)
    with pytest.raises(ConfigValidationError, match="profile-id must be non-empty"):
        match_jobs(
            index_dir=index_dir,
            candidate_json=candidate_json,
            profile_id="   ",
        )


def test_match_jobs_tie_break_prefers_newer_posted_at(tmp_path: Path) -> None:
    parquet = tmp_path / "jobs_tie.parquet"
    now = datetime.now(UTC).replace(microsecond=0)
    older = (now - timedelta(days=30)).isoformat()
    newer = now.isoformat()
    pl.DataFrame(
        {
            "id": ["1", "2"],
            "source_job_id": ["job-1", "job-2"],
            "title": ["Engineer", "Engineer"],
            "company": ["Acme", "Acme"],
            "location": [None, None],
            "remote": [None, None],
            "description_text": ["", ""],
            "skills": [[], []],
            "salary_min": [None, None],
            "salary_max": [None, None],
            "apply_url": ["https://jobs/1", "https://jobs/2"],
            "posted_at": [older, newer],
            "source": ["greenhouse", "greenhouse"],
        }
    ).write_parquet(parquet)

    index = build_retrieval_index(input_parquet=parquet)
    result = match_jobs_with_profile(
        index_dir=index.index_dir,
        candidate=CandidateProfile(profile_id="p"),
        top_k=2,
    )
    assert [item.job_id for item in result.results] == ["job-2", "job-1"]


def test_evaluate_relevance_pass_and_fail(tmp_path: Path) -> None:
    _parquet, index_dir = _write_index(tmp_path)

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

    thresholds_pass = tmp_path / "pass.toml"
    thresholds_pass.write_text("precision_at_10_min=0.0\nrecall_at_25_min=0.0\nks=[10,25]", encoding="utf-8")
    ok = evaluate_relevance(index_dir=index_dir, golden_set=golden, thresholds_file=thresholds_pass)
    assert ok.status == "pass"
    assert ok.metrics["precision_at_10"] >= 0.0

    thresholds_fail = tmp_path / "fail.toml"
    thresholds_fail.write_text("precision_at_10_min=1.0\nrecall_at_25_min=1.0\nks=[10,25]", encoding="utf-8")
    bad = evaluate_relevance(index_dir=index_dir, golden_set=golden, thresholds_file=thresholds_fail)
    assert bad.status == "fail"
    assert "EVAL_PRECISION_AT_10_BELOW_THRESHOLD" in bad.check_codes

    with pytest.raises(ConfigValidationError, match="golden set root"):
        broken = tmp_path / "broken.json"
        broken.write_text("[]", encoding="utf-8")
        evaluate_relevance(index_dir=index_dir, golden_set=broken)

    with pytest.raises(ConfigValidationError, match="no valid cases"):
        empty = tmp_path / "empty.json"
        empty.write_text(json.dumps({"cases": [{"candidate": {"profile_id": "x"}, "relevant_job_ids": []}]}), encoding="utf-8")
        evaluate_relevance(index_dir=index_dir, golden_set=empty)


def test_feedback_record_duplicate_and_summary(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)

    meta = tmp_path / "meta.json"
    meta.write_text(json.dumps({"source": "agent"}), encoding="utf-8")

    first = record_feedback_event(
        profile_id="Jane",
        job_id="g-1",
        event="interviewed",
        meta_json_file=meta,
    )
    assert first.duplicate is False
    assert first.weights["skills"] > 1.0

    duplicate = record_feedback_event(
        profile_id="Jane",
        job_id="g-1",
        event="interviewed",
        meta_json_file=meta,
    )
    assert duplicate.duplicate is True

    second = record_feedback_event(
        profile_id="Jane",
        job_id="l-2",
        event="not_relevant",
    )
    assert second.weights["location_work_mode"] <= first.weights["location_work_mode"]

    summary_all = summarize_feedback()
    assert summary_all.total_events == 2
    assert summary_all.counts["interviewed"] == 1

    summary_profile = summarize_feedback(profile_id="jane")
    assert summary_profile.weights is not None
    assert summary_profile.profile_counts["jane"]["not_relevant"] == 1

    assert feedback_mod.load_profile_weights("missing") == {
        "skills": 1.0,
        "title": 1.0,
        "location_work_mode": 1.0,
        "seniority": 1.0,
        "recency": 1.0,
        "compensation": 1.0,
    }

    with pytest.raises(ConfigValidationError, match="cannot read feedback meta JSON"):
        record_feedback_event(profile_id="jane", job_id="x", event="applied", meta_json_file=tmp_path / "missing.json")

    with pytest.raises(ConfigValidationError, match="feedback event must be one of"):
        feedback_mod.record_feedback_event(profile_id="jane", job_id="x", event="bad")  # type: ignore[arg-type]


def test_feedback_duplicate_ignores_timestamp_and_bad_weights_json(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)

    class _FakeDatetime:
        _tick = 0

        @classmethod
        def now(cls, _tz: object) -> datetime:
            cls._tick += 1
            return datetime(2026, 1, 1, 0, 0, cls._tick, tzinfo=UTC)

    monkeypatch.setattr(feedback_mod, "datetime", _FakeDatetime)

    first = record_feedback_event(profile_id="jane", job_id="g-1", event="applied")
    second = record_feedback_event(profile_id="jane", job_id="g-1", event="applied")
    assert first.duplicate is False
    assert second.duplicate is True

    weights_path = Path(second.weights_file)
    weights_path.write_text("{", encoding="utf-8")
    assert feedback_mod.load_profile_weights("jane") == {
        "skills": 1.0,
        "title": 1.0,
        "location_work_mode": 1.0,
        "seniority": 1.0,
        "recency": 1.0,
        "compensation": 1.0,
    }


def test_internal_helpers_and_branches(tmp_path: Path) -> None:
    # policy helper parse ratio branch
    with pytest.raises(ConfigValidationError, match="must be numeric"):
        policy_mod._parse_ratio("x", field="precision_at_10_min")

    # parser helper unknown token branch
    assert "visa_signal_ambiguous" in parser_mod._collect_unknown_resume_tokens("sponsorship maybe")

    # index helper shard and resolve branch
    assert index_mod._token_shard("python")
    assert index_mod._resolve_output_dir(output_dir=None, index_id="abc").name == "abc"
    assert index_mod._resolve_output_dir(output_dir=tmp_path / "x", index_id="abc") == (tmp_path / "x").resolve()

    # scoring helper branches
    zero_policy = RecommendationPolicy(weights={key: 0.0 for key in scoring_mod.SIGNAL_KEYS})
    normalized = zero_policy.normalized_weights()
    assert round(sum(normalized.values()), 6) == 1.0
    assert scoring_mod._stable_job_id({"title": "a"})

    assert eval_mod._parse_case_candidate({"candidate": {"profile_id": "abc"}}).profile_id == "abc"
    with pytest.raises(ConfigValidationError, match="candidate object"):
        eval_mod._parse_case_candidate({"candidate": []})
