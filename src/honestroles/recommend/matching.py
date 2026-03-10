from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from honestroles.errors import ConfigValidationError

from .feedback import load_profile_weights
from .index import load_index
from .models import CandidateProfile, ExcludedJob, MatchResult, MatchedJob, SCHEMA_VERSION
from .parser import parse_candidate_json_file, parse_resume_text_file
from .policy import load_recommendation_policy
from .scoring import filter_job, score_job


def match_jobs(
    *,
    index_dir: str | Path,
    candidate_json: str | Path | None = None,
    resume_text: str | Path | None = None,
    profile_id: str | None = None,
    top_k: int = 25,
    policy_file: str | Path | None = None,
    include_excluded: bool = False,
) -> MatchResult:
    if top_k < 1:
        raise ConfigValidationError("top-k must be >= 1")
    candidate = _resolve_candidate(candidate_json=candidate_json, resume_text=resume_text, profile_id=profile_id)
    return match_jobs_with_profile(
        index_dir=index_dir,
        candidate=candidate,
        top_k=top_k,
        policy_file=policy_file,
        include_excluded=include_excluded,
    )


def match_jobs_with_profile(
    *,
    index_dir: str | Path,
    candidate: CandidateProfile,
    top_k: int = 25,
    policy_file: str | Path | None = None,
    include_excluded: bool = False,
) -> MatchResult:
    if top_k < 1:
        raise ConfigValidationError("top-k must be >= 1")

    policy, policy_source, policy_hash = load_recommendation_policy(policy_file)
    manifest, jobs = load_index(index_dir)
    _ = manifest

    multipliers = load_profile_weights(candidate.profile_id)

    matched: list[MatchedJob] = []
    excluded: list[ExcludedJob] = []

    for job in jobs:
        reasons = filter_job(candidate, job)
        if reasons:
            if include_excluded:
                excluded.append(
                    ExcludedJob(
                        job_id=str(job.get("job_id", "")),
                        exclude_reasons=reasons,
                        apply_url=_text_or_none(job.get("apply_url")),
                        posted_at=_text_or_none(job.get("posted_at")),
                        source=_text_or_none(job.get("source")),
                    )
                )
            continue

        score, match_reasons, missing_skills, quality_flags, signal_values = score_job(
            candidate=candidate,
            job=job,
            policy=policy,
            multipliers=multipliers,
        )
        _ = signal_values
        matched.append(
            MatchedJob(
                job_id=str(job.get("job_id", "")),
                score=score,
                match_reasons=match_reasons,
                required_missing_skills=missing_skills,
                apply_url=_text_or_none(job.get("apply_url")),
                posted_at=_text_or_none(job.get("posted_at")),
                source=_text_or_none(job.get("source")),
                quality_flags=quality_flags,
            )
        )

    matched_sorted = tuple(
        sorted(
            matched,
            key=lambda item: (
                -float(item.score),
                -_posted_sort_value(item.posted_at),
                item.job_id,
            ),
        )[:top_k]
    )

    return MatchResult(
        schema_version=SCHEMA_VERSION,
        status="pass",
        profile=candidate,
        index_dir=str(Path(index_dir).expanduser().resolve()),
        policy_source=policy_source,
        policy_hash=policy_hash,
        eligible_count=len(matched),
        excluded_count=len(jobs) - len(matched),
        total_jobs=len(jobs),
        top_k=top_k,
        results=matched_sorted,
        excluded_jobs=tuple(excluded) if include_excluded else (),
        check_codes=(),
    )


def _resolve_candidate(
    *,
    candidate_json: str | Path | None,
    resume_text: str | Path | None,
    profile_id: str | None,
) -> CandidateProfile:
    has_json = candidate_json not in (None, "")
    has_text = resume_text not in (None, "")
    if has_json == has_text:
        raise ConfigValidationError(
            "provide exactly one of --candidate-json or --resume-text"
        )

    if has_json:
        profile = parse_candidate_json_file(candidate_json)
    else:
        profile = parse_resume_text_file(resume_text, profile_id=profile_id)

    if profile_id not in (None, ""):
        normalized_profile_id = str(profile_id).strip().lower()
        if not normalized_profile_id:
            raise ConfigValidationError("--profile-id must be non-empty when provided")
        profile = CandidateProfile(
            profile_id=normalized_profile_id,
            skills=profile.skills,
            titles=profile.titles,
            years_experience=profile.years_experience,
            locations=profile.locations,
            work_mode_preferences=profile.work_mode_preferences,
            seniority_targets=profile.seniority_targets,
            salary_targets=profile.salary_targets,
            visa_work_auth=profile.visa_work_auth,
            employment_type_preferences=profile.employment_type_preferences,
            parser_confidence=profile.parser_confidence,
            parser_unknown_tokens=profile.parser_unknown_tokens,
        )
    return profile


def _text_or_none(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _sortable_date(value: str | None) -> str:
    if value is None:
        return ""
    text = value.strip()
    if text.endswith("Z"):
        return text[:-1] + "+00:00"
    return text


def _posted_sort_value(value: str | None) -> float:
    text = _sortable_date(value)
    if not text:
        return float("-inf")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return float("-inf")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.timestamp()
