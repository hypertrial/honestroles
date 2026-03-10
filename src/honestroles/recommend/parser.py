from __future__ import annotations

from dataclasses import replace
import json
from pathlib import Path
import re
from typing import Any

from honestroles.errors import ConfigValidationError

from .models import CandidateProfile, SalaryTargets, Seniority, VisaWorkAuth, WorkMode

_WORK_MODE_SET: set[str] = {"remote", "hybrid", "onsite", "unknown"}
_SENIORITY_SET: set[str] = {"junior", "mid", "senior", "staff", "principal"}
_EMPLOYMENT_SET: set[str] = {
    "full_time",
    "part_time",
    "contract",
    "internship",
    "temporary",
    "unknown",
}

_SKILL_VOCAB: tuple[str, ...] = (
    "python",
    "sql",
    "java",
    "javascript",
    "typescript",
    "go",
    "rust",
    "aws",
    "gcp",
    "docker",
    "kubernetes",
    "react",
    "node",
    "machine learning",
)

_TITLE_HINTS: tuple[str, ...] = (
    "software engineer",
    "backend engineer",
    "frontend engineer",
    "full stack engineer",
    "data engineer",
    "data scientist",
    "product manager",
    "analyst",
    "designer",
)

_LOCATION_HINTS: tuple[str, ...] = (
    "remote",
    "new york",
    "san francisco",
    "london",
    "berlin",
    "lisbon",
    "europe",
    "united states",
    "usa",
)


def parse_candidate_json_file(path: str | Path) -> CandidateProfile:
    candidate_path = Path(path).expanduser().resolve()
    try:
        payload = json.loads(candidate_path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ConfigValidationError(f"cannot read candidate JSON '{candidate_path}': {exc}") from exc
    except json.JSONDecodeError as exc:
        raise ConfigValidationError(f"invalid candidate JSON '{candidate_path}': {exc}") from exc
    if not isinstance(payload, dict):
        raise ConfigValidationError("candidate JSON root must be an object")
    return parse_candidate_profile_payload(payload)


def parse_resume_text_file(path: str | Path, *, profile_id: str | None = None) -> CandidateProfile:
    resume_path = Path(path).expanduser().resolve()
    try:
        text = resume_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ConfigValidationError(f"cannot read resume text '{resume_path}': {exc}") from exc
    return parse_resume_text(text, profile_id=profile_id or resume_path.stem)


def parse_candidate_profile_payload(payload: dict[str, Any]) -> CandidateProfile:
    profile_id = _required_text(payload.get("profile_id"), field="profile_id")
    skills = _string_list(payload.get("skills"), field="skills")
    titles = _string_list(payload.get("titles"), field="titles")
    years_experience = _optional_float(payload.get("years_experience"), field="years_experience")
    locations = _string_list(payload.get("locations"), field="locations")
    work_modes = _enum_list(
        payload.get("work_mode_preferences"),
        field="work_mode_preferences",
        allowed=_WORK_MODE_SET,
    )
    seniority = _enum_list(
        payload.get("seniority_targets"),
        field="seniority_targets",
        allowed=_SENIORITY_SET,
    )
    salary_targets = _parse_salary_targets(payload.get("salary_targets"))
    visa_work_auth = _parse_visa_work_auth(payload.get("visa_work_auth"))
    employment_type = _enum_list(
        payload.get("employment_type_preferences"),
        field="employment_type_preferences",
        allowed=_EMPLOYMENT_SET,
    )

    return CandidateProfile(
        profile_id=profile_id,
        skills=tuple(skills),
        titles=tuple(titles),
        years_experience=years_experience,
        locations=tuple(locations),
        work_mode_preferences=tuple(work_modes),
        seniority_targets=tuple(seniority),
        salary_targets=salary_targets,
        visa_work_auth=visa_work_auth,
        employment_type_preferences=tuple(employment_type),
    )


def parse_resume_text(text: str, *, profile_id: str = "resume") -> CandidateProfile:
    if not isinstance(text, str):
        raise ConfigValidationError("resume text must be a string")
    raw = text.strip()
    if not raw:
        raise ConfigValidationError("resume text is empty")
    lowered = raw.lower()

    skills = [skill for skill in _SKILL_VOCAB if skill in lowered]
    titles = [title for title in _TITLE_HINTS if title in lowered]
    locations = [loc for loc in _LOCATION_HINTS if loc in lowered and loc != "remote"]

    work_modes: list[WorkMode] = []
    if "remote" in lowered:
        work_modes.append("remote")
    if "hybrid" in lowered:
        work_modes.append("hybrid")
    if "onsite" in lowered or "on-site" in lowered:
        work_modes.append("onsite")

    seniority: list[Seniority] = []
    for token in ("junior", "mid", "senior", "staff", "principal"):
        if token in lowered:
            seniority.append(token)  # type: ignore[arg-type]

    years = None
    years_match = re.search(r"(\d{1,2})\+?\s+years", lowered)
    if years_match is not None:
        years = float(int(years_match.group(1)))

    salary_min = None
    salary_match = re.search(r"\$\s*([0-9]{2,3})(?:[, ]?([0-9]{3}))?", lowered)
    if salary_match is not None:
        major = salary_match.group(1)
        minor = salary_match.group(2)
        if minor is not None:
            salary_min = float(f"{major}{minor}")
        else:
            salary_min = float(int(major) * 1000)

    requires_sponsorship = None
    if "requires sponsorship" in lowered or "need sponsorship" in lowered:
        requires_sponsorship = True
    if "no sponsorship" in lowered or "does not require sponsorship" in lowered:
        requires_sponsorship = False

    unknown_tokens = _collect_unknown_resume_tokens(lowered)
    confidence = _parser_confidence(
        has_skills=bool(skills),
        has_titles=bool(titles),
        has_locations=bool(locations),
        has_years=years is not None,
    )

    profile = CandidateProfile(
        profile_id=_normalize_text(profile_id, field="profile_id"),
        skills=tuple(_dedupe(skills)),
        titles=tuple(_dedupe(titles)),
        years_experience=years,
        locations=tuple(_dedupe(locations)),
        work_mode_preferences=tuple(_dedupe(work_modes)),
        seniority_targets=tuple(_dedupe(seniority)),
        salary_targets=SalaryTargets(minimum=salary_min),
        visa_work_auth=VisaWorkAuth(requires_sponsorship=requires_sponsorship),
    )
    return replace(
        profile,
        parser_confidence=confidence,
        parser_unknown_tokens=tuple(unknown_tokens),
    )


def _required_text(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ConfigValidationError(f"candidate.{field} must be a non-empty string")
    return _normalize_text(value, field=field)


def _normalize_text(value: str, *, field: str) -> str:
    text = value.strip().lower()
    if not text:
        raise ConfigValidationError(f"candidate.{field} must be non-empty")
    return text


def _string_list(value: Any, *, field: str) -> list[str]:
    if value in (None, ""):
        return []
    if not isinstance(value, list):
        raise ConfigValidationError(f"candidate.{field} must be an array of strings")
    cleaned: list[str] = []
    for item in value:
        if not isinstance(item, str):
            raise ConfigValidationError(f"candidate.{field} entries must be strings")
        token = item.strip().lower()
        if token:
            cleaned.append(token)
    return _dedupe(cleaned)


def _enum_list(value: Any, *, field: str, allowed: set[str]) -> list[str]:
    values = _string_list(value, field=field)
    bad = [item for item in values if item not in allowed]
    if bad:
        raise ConfigValidationError(
            f"candidate.{field} contains unsupported values: {', '.join(sorted(bad))}"
        )
    return values


def _optional_float(value: Any, *, field: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ConfigValidationError(f"candidate.{field} must be numeric")
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str) and value.strip():
        try:
            return float(value.strip())
        except ValueError as exc:
            raise ConfigValidationError(f"candidate.{field} must be numeric") from exc
    raise ConfigValidationError(f"candidate.{field} must be numeric")


def _parse_salary_targets(value: Any) -> SalaryTargets:
    if value in (None, ""):
        return SalaryTargets()
    if not isinstance(value, dict):
        raise ConfigValidationError("candidate.salary_targets must be an object")
    minimum = _optional_float(value.get("minimum"), field="salary_targets.minimum")
    maximum = _optional_float(value.get("maximum"), field="salary_targets.maximum")
    currency = None
    if value.get("currency") not in (None, ""):
        if not isinstance(value.get("currency"), str):
            raise ConfigValidationError("candidate.salary_targets.currency must be a string")
        currency = value["currency"].strip().upper()
    interval = None
    if value.get("interval") not in (None, ""):
        if not isinstance(value.get("interval"), str):
            raise ConfigValidationError("candidate.salary_targets.interval must be a string")
        interval = value["interval"].strip().lower()
    return SalaryTargets(minimum=minimum, maximum=maximum, currency=currency, interval=interval)


def _parse_visa_work_auth(value: Any) -> VisaWorkAuth:
    if value in (None, ""):
        return VisaWorkAuth()
    if not isinstance(value, dict):
        raise ConfigValidationError("candidate.visa_work_auth must be an object")

    sponsorship = value.get("requires_sponsorship")
    if sponsorship is not None and not isinstance(sponsorship, bool):
        raise ConfigValidationError("candidate.visa_work_auth.requires_sponsorship must be boolean")

    authorized_locations = _string_list(
        value.get("authorized_locations"),
        field="visa_work_auth.authorized_locations",
    )
    return VisaWorkAuth(
        requires_sponsorship=sponsorship,
        authorized_locations=tuple(authorized_locations),
    )


def _collect_unknown_resume_tokens(text: str) -> list[str]:
    unknown: list[str] = []
    if "sponsorship" in text and "require" not in text and "no sponsorship" not in text:
        unknown.append("visa_signal_ambiguous")
    if "$" in text and "year" not in text and "annual" not in text:
        unknown.append("salary_interval_unspecified")
    return unknown


def _parser_confidence(
    *,
    has_skills: bool,
    has_titles: bool,
    has_locations: bool,
    has_years: bool,
) -> float:
    score = 0.25
    if has_skills:
        score += 0.30
    if has_titles:
        score += 0.20
    if has_locations:
        score += 0.15
    if has_years:
        score += 0.10
    return round(min(1.0, score), 4)


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        if value not in seen:
            seen.add(value)
            out.append(value)
    return out
