from __future__ import annotations

from datetime import UTC, datetime
import hashlib
import math
import re
from typing import Any

from .models import CandidateProfile, MatchReason, RecommendationPolicy, SIGNAL_KEYS


def tokenize_text(text: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-z0-9]+", text.lower())
        if len(token) >= 2
    }


def normalize_job_record(raw: dict[str, Any]) -> dict[str, Any]:
    source_job_id = _text_or_none(raw.get("source_job_id"))
    canonical_id = _text_or_none(raw.get("id"))
    apply_url = _text_or_none(raw.get("apply_url"))
    job_id = source_job_id or canonical_id or _stable_job_id(raw)
    title = _text_or_none(raw.get("title"))
    company = _text_or_none(raw.get("company"))
    description_text = _text_or_none(raw.get("description_text"))
    location = _text_or_none(raw.get("location"))
    work_mode = _infer_work_mode(raw)
    seniority = _text_or_none(raw.get("seniority")) or _infer_seniority(title)
    employment_type = _normalize_employment_type(raw.get("employment_type"))
    remote = _coerce_bool(raw.get("remote"))

    skills = _normalize_skills(raw.get("skills"))
    posted_at = _normalize_timestamp(raw.get("posted_at"))
    source_updated_at = _normalize_timestamp(raw.get("source_updated_at"))

    salary_min = _to_float(raw.get("salary_min"))
    salary_max = _to_float(raw.get("salary_max"))

    payload = {
        "job_id": job_id,
        "id": canonical_id,
        "title": title,
        "company": company,
        "location": location,
        "work_mode": work_mode,
        "seniority": seniority,
        "employment_type": employment_type,
        "remote": remote,
        "description_text": description_text,
        "description_html": _text_or_none(raw.get("description_html")),
        "skills": skills,
        "salary_min": salary_min,
        "salary_max": salary_max,
        "salary_currency": _text_or_none(raw.get("salary_currency")),
        "salary_interval": _text_or_none(raw.get("salary_interval")),
        "apply_url": apply_url,
        "posted_at": posted_at,
        "source_updated_at": source_updated_at,
        "source": _text_or_none(raw.get("source")),
        "source_ref": _text_or_none(raw.get("source_ref")),
        "job_url": _text_or_none(raw.get("job_url")) or apply_url,
    }
    payload["tokens"] = sorted(_job_tokens(payload))
    return payload


def score_job(
    *,
    candidate: CandidateProfile,
    job: dict[str, Any],
    policy: RecommendationPolicy,
    multipliers: dict[str, float],
) -> tuple[float, tuple[MatchReason, ...], tuple[str, ...], tuple[str, ...], dict[str, float]]:
    job_tokens = set(str(item) for item in job.get("tokens", []))
    skill_score = _skill_overlap_score(set(candidate.skills), job_tokens)
    title_score = _title_similarity_score(set(candidate.titles), tokenize_text(str(job.get("title") or "")))
    location_score = _location_work_mode_score(candidate, job)
    seniority_score = _seniority_score(candidate, job)
    recency_score = _recency_score(job)
    compensation_score = _compensation_score(candidate, job)

    signal_values = {
        "skills": skill_score,
        "title": title_score,
        "location_work_mode": location_score,
        "seniority": seniority_score,
        "recency": recency_score,
        "compensation": compensation_score,
    }

    weights = policy.normalized_weights()
    weighted_score = 0.0
    reasons: list[MatchReason] = []
    for key in SIGNAL_KEYS:
        multiplier = float(multipliers.get(key, 1.0))
        weight = float(weights.get(key, 0.0)) * multiplier
        contribution = signal_values[key] * weight
        weighted_score += contribution
        reasons.append(
            MatchReason(
                code=f"SIGNAL_{key.upper()}",
                value=round(signal_values[key], 6),
                weight=round(weight, 6),
                contribution=round(contribution, 6),
            )
        )

    capped_score = max(0.0, min(1.0, weighted_score))
    reasons_sorted = tuple(sorted(reasons, key=lambda item: item.contribution, reverse=True)[: policy.reason_limit])
    missing_skills = tuple(sorted(set(candidate.skills) - _job_skill_tokens(job)))
    quality_flags = _quality_flags(job)
    return round(capped_score, 6), reasons_sorted, missing_skills, quality_flags, signal_values


def filter_job(candidate: CandidateProfile, job: dict[str, Any]) -> tuple[str, ...]:
    reasons: list[str] = []
    if not _location_eligible(candidate, job):
        reasons.append("FILTER_LOCATION")
    if not _work_mode_eligible(candidate, job):
        reasons.append("FILTER_WORK_MODE")
    if not _employment_type_eligible(candidate, job):
        reasons.append("FILTER_EMPLOYMENT_TYPE")
    if not _salary_eligible(candidate, job):
        reasons.append("FILTER_SALARY")
    if not _visa_eligible(candidate, job):
        reasons.append("FILTER_VISA")
    return tuple(reasons)


def _skill_overlap_score(candidate_skills: set[str], job_tokens: set[str]) -> float:
    if not candidate_skills:
        return 0.5
    overlap = len(candidate_skills & job_tokens)
    return overlap / float(len(candidate_skills))


def _title_similarity_score(candidate_titles: set[str], job_title_tokens: set[str]) -> float:
    if not candidate_titles:
        return 0.5
    best = 0.0
    for title in candidate_titles:
        title_tokens = tokenize_text(title)
        if not title_tokens:
            continue
        denom = len(title_tokens | job_title_tokens)
        score = len(title_tokens & job_title_tokens) / float(denom)
        if score > best:
            best = score
    return best


def _location_work_mode_score(candidate: CandidateProfile, job: dict[str, Any]) -> float:
    location_ok = _location_eligible(candidate, job)
    work_mode_ok = _work_mode_eligible(candidate, job)
    if location_ok and work_mode_ok:
        return 1.0
    if location_ok or work_mode_ok:
        return 0.5
    return 0.0


def _seniority_score(candidate: CandidateProfile, job: dict[str, Any]) -> float:
    if not candidate.seniority_targets:
        return 0.5
    job_seniority = str(job.get("seniority") or "").strip().lower()
    return 1.0 if job_seniority in set(candidate.seniority_targets) else 0.0


def _recency_score(job: dict[str, Any]) -> float:
    posted = _parse_datetime(job.get("posted_at"))
    if posted is None:
        return 0.0
    age_days = max(0.0, (datetime.now(UTC) - posted).total_seconds() / 86400.0)
    # 90-day half life; deterministic and bounded.
    return max(0.0, min(1.0, math.exp(-math.log(2.0) * (age_days / 90.0))))


def _compensation_score(candidate: CandidateProfile, job: dict[str, Any]) -> float:
    target = candidate.salary_targets.minimum
    if target is None:
        return 0.5
    job_min = _to_float(job.get("salary_min"))
    job_max = _to_float(job.get("salary_max"))
    if job_min is None and job_max is None:
        return 0.0
    offered = max(value for value in (job_min, job_max) if value is not None)
    if offered >= target:
        return 1.0
    if target <= 0:
        return 0.0
    return max(0.0, min(1.0, offered / target))


def _location_eligible(candidate: CandidateProfile, job: dict[str, Any]) -> bool:
    if not candidate.locations and not candidate.visa_work_auth.authorized_locations:
        return True
    job_location = str(job.get("location") or "").lower()
    allowed_locations = set(candidate.locations) | set(candidate.visa_work_auth.authorized_locations)
    if any(loc in job_location for loc in allowed_locations):
        return True
    return bool(job.get("work_mode") == "remote" and "remote" in allowed_locations)


def _work_mode_eligible(candidate: CandidateProfile, job: dict[str, Any]) -> bool:
    if not candidate.work_mode_preferences:
        return True
    job_work_mode = str(job.get("work_mode") or "unknown").lower()
    return job_work_mode in set(candidate.work_mode_preferences)


def _employment_type_eligible(candidate: CandidateProfile, job: dict[str, Any]) -> bool:
    if not candidate.employment_type_preferences:
        return True
    employment_type = str(job.get("employment_type") or "unknown").lower()
    return employment_type in set(candidate.employment_type_preferences)


def _salary_eligible(candidate: CandidateProfile, job: dict[str, Any]) -> bool:
    floor = candidate.salary_targets.minimum
    if floor is None:
        return True
    job_min = _to_float(job.get("salary_min"))
    job_max = _to_float(job.get("salary_max"))
    if job_min is None and job_max is None:
        return False
    best = max(value for value in (job_min, job_max) if value is not None)
    return best >= floor


def _visa_eligible(candidate: CandidateProfile, job: dict[str, Any]) -> bool:
    requires = candidate.visa_work_auth.requires_sponsorship
    if requires is None:
        return True
    text = " ".join(
        str(value or "")
        for value in (
            job.get("title"),
            job.get("description_text"),
            job.get("description_html"),
        )
    ).lower()
    if requires:
        return "no sponsorship" not in text and "not sponsor" not in text
    return True


def _quality_flags(job: dict[str, Any]) -> tuple[str, ...]:
    flags: list[str] = []
    if not _text_or_none(job.get("company")):
        flags.append("MISSING_COMPANY")
    if not _text_or_none(job.get("posted_at")):
        flags.append("MISSING_POSTED_AT")
    if not _text_or_none(job.get("description_text")):
        flags.append("MISSING_DESCRIPTION")
    return tuple(flags)


def _job_skill_tokens(job: dict[str, Any]) -> set[str]:
    skills = {str(item).strip().lower() for item in job.get("skills", []) if str(item).strip()}
    return skills | tokenize_text(str(job.get("title") or "")) | tokenize_text(str(job.get("description_text") or ""))


def _job_tokens(job: dict[str, Any]) -> set[str]:
    tokens = set()
    tokens |= tokenize_text(str(job.get("title") or ""))
    tokens |= tokenize_text(str(job.get("company") or ""))
    tokens |= tokenize_text(str(job.get("description_text") or ""))
    tokens |= {str(item).strip().lower() for item in job.get("skills", []) if str(item).strip()}
    tokens |= tokenize_text(str(job.get("location") or ""))
    return tokens


def _normalize_skills(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        source = [item.strip().lower() for item in value.split(",")]
    elif isinstance(value, list):
        source = [str(item).strip().lower() for item in value]
    else:
        source = []
    seen: set[str] = set()
    cleaned: list[str] = []
    for item in source:
        if item and item not in seen:
            seen.add(item)
            cleaned.append(item)
    return cleaned


def _stable_job_id(raw: dict[str, Any]) -> str:
    seed = "|".join(
        str(raw.get(key) or "")
        for key in ("title", "company", "location", "posted_at", "apply_url")
    )
    digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()
    return digest[:16]


def _normalize_timestamp(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    parsed = _parse_datetime(text)
    if parsed is None:
        return text
    return parsed.isoformat()


def _parse_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _normalize_employment_type(value: object) -> str:
    text = _text_or_none(value)
    if text is None:
        return "unknown"
    normalized = text.replace("-", "_").replace(" ", "_")
    if normalized in {"full_time", "part_time", "contract", "internship", "temporary"}:
        return normalized
    return "unknown"


def _infer_work_mode(raw: dict[str, Any]) -> str:
    explicit = _text_or_none(raw.get("work_mode"))
    if explicit in {"remote", "hybrid", "onsite", "unknown"}:
        return explicit
    remote = _coerce_bool(raw.get("remote"))
    location = _text_or_none(raw.get("location")) or ""
    lowered = location.lower()
    if remote is True or "remote" in lowered:
        return "remote"
    if "hybrid" in lowered:
        return "hybrid"
    if remote is False or "onsite" in lowered or "on-site" in lowered:
        return "onsite"
    return "unknown"


def _infer_seniority(title: str | None) -> str:
    lowered = (title or "").lower()
    if any(token in lowered for token in ("principal", "staff")):
        return "staff"
    if "senior" in lowered:
        return "senior"
    if any(token in lowered for token in ("junior", "intern", "entry")):
        return "junior"
    return "mid"


def _text_or_none(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _to_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str) and value.strip():
        try:
            return float(value.strip())
        except ValueError:
            return None
    return None


def _coerce_bool(value: object) -> bool | None:
    if isinstance(value, bool):
        return value
    text = _text_or_none(value)
    if text is None:
        return None
    lowered = text.lower()
    if lowered in {"true", "1", "yes", "y"}:
        return True
    if lowered in {"false", "0", "no", "n"}:
        return False
    return None
