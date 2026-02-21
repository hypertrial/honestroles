from __future__ import annotations

import json
import logging
import math
import re

import pandas as pd

from honestroles.llm.client import OllamaClient
from honestroles.llm.prompts import build_job_signal_prompt
from honestroles.match.models import DEFAULT_RESULT_COLUMNS
from honestroles.schema import (
    APPLY_URL,
    DESCRIPTION_TEXT,
    REMOTE_FLAG,
    REMOTE_TYPE,
    SKILLS,
    TITLE,
)

LOGGER = logging.getLogger(__name__)

_EXPERIENCE_RANGE_RE = re.compile(
    r"\b(\d{1,2})\s*(?:-|to)\s*(\d{1,2})\+?\s+years?",
    re.IGNORECASE,
)
_EXPERIENCE_MIN_RE = re.compile(r"\b(\d{1,2})\+?\s+years?", re.IGNORECASE)

_ENTRY_LEVEL_POSITIVE = (
    "entry level",
    "new grad",
    "new graduate",
    "recent graduate",
    "early career",
    "graduate program",
    "intern",
    "junior",
)
_ENTRY_LEVEL_NEGATIVE = (
    "senior",
    "staff",
    "principal",
    "lead",
    "director",
    "vp",
    "head of",
)

_VISA_POSITIVE = (
    "visa sponsorship available",
    "we sponsor",
    "sponsor visa",
    "h-1b sponsorship",
    "h1b sponsorship",
    "opt accepted",
    "cpt accepted",
)
_VISA_NEGATIVE = (
    "no sponsorship",
    "unable to sponsor",
    "will not sponsor",
    "cannot sponsor",
    "must be authorized to work",
    "without sponsorship",
)

_FRICTION_TERMS = (
    "cover letter required",
    "take-home",
    "take home",
    "portfolio required",
    "assessment",
    "case study",
    "references required",
)

_CLARITY_POSITIVE = (
    "responsibilities",
    "requirements",
    "qualifications",
    "about the role",
    "what you'll do",
)

_SKILL_TERMS = {
    "python",
    "sql",
    "pandas",
    "numpy",
    "scikit-learn",
    "spark",
    "airflow",
    "dbt",
    "tableau",
    "power bi",
    "statistics",
    "machine learning",
    "deep learning",
    "nlp",
    "pytorch",
    "tensorflow",
    "snowflake",
    "bigquery",
    "aws",
    "gcp",
    "azure",
}

_REQUIRED_SKILL_PREFIX = (
    "required",
    "must have",
    "must-haves",
    "you have",
    "minimum qualifications",
)
_PREFERRED_SKILL_PREFIX = (
    "preferred",
    "nice to have",
    "plus",
    "bonus",
)


def _compile_skill_pattern(term: str) -> re.Pattern[str]:
    escaped = re.escape(term).replace(r"\ ", r"\s+")
    return re.compile(rf"(?<![a-z0-9]){escaped}(?![a-z0-9])", re.IGNORECASE)


_SKILL_PATTERNS: dict[str, re.Pattern[str]] = {
    term: _compile_skill_pattern(term) for term in sorted(_SKILL_TERMS)
}


def _as_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    return str(value)


def _list_from_value(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, float) and pd.isna(value):
        return []
    if isinstance(value, str):
        stripped = value.strip()
        return [stripped] if stripped else []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    return [str(value).strip()]


def _extract_years(text: str) -> tuple[int | None, int | None]:
    min_years: int | None = None
    max_years: int | None = None

    for match in _EXPERIENCE_RANGE_RE.finditer(text):
        low = int(match.group(1))
        high = int(match.group(2))
        low, high = min(low, high), max(low, high)
        if low > 20 or high > 20:
            continue
        min_years = low if min_years is None else min(min_years, low)
        max_years = high if max_years is None else max(max_years, high)

    for match in _EXPERIENCE_MIN_RE.finditer(text):
        years = int(match.group(1))
        if years > 20:
            continue
        min_years = years if min_years is None else min(min_years, years)
        max_years = years if max_years is None else max(max_years, years)

    return min_years, max_years


def _extract_skills(text: str, seed_skills: list[str]) -> tuple[list[str], list[str]]:
    lowered = text.lower()
    chunks = [chunk.strip().lower() for chunk in re.split(r"[\n.;]", text) if chunk.strip()]
    required: set[str] = {skill.lower() for skill in seed_skills}
    preferred: set[str] = set()

    for skill, pattern in _SKILL_PATTERNS.items():
        if not pattern.search(text):
            continue
        required_hint = any(
            pattern.search(chunk) and any(prefix in chunk for prefix in _REQUIRED_SKILL_PREFIX)
            for chunk in chunks
        ) or any(f"{prefix} {skill}" in lowered for prefix in _REQUIRED_SKILL_PREFIX)
        preferred_hint = any(
            pattern.search(chunk) and any(prefix in chunk for prefix in _PREFERRED_SKILL_PREFIX)
            for chunk in chunks
        ) or any(f"{prefix} {skill}" in lowered for prefix in _PREFERRED_SKILL_PREFIX)
        if required_hint:
            required.add(skill)
        elif preferred_hint:
            preferred.add(skill)
        else:
            required.add(skill)

    preferred -= required
    return sorted(required), sorted(preferred)


def _entry_level_signal(text: str, years_min: int | None) -> bool | None:
    lowered = text.lower()
    if any(term in lowered for term in _ENTRY_LEVEL_POSITIVE):
        return True
    if any(term in lowered for term in _ENTRY_LEVEL_NEGATIVE):
        return False
    if years_min is None:
        return None
    if years_min <= 2:
        return True
    return False


def _visa_signal(text: str) -> bool | None:
    lowered = text.lower()
    if any(term in lowered for term in _VISA_NEGATIVE):
        return False
    if any(term in lowered for term in _VISA_POSITIVE):
        return True
    return None


def _application_friction_score(text: str, apply_url: str) -> float:
    lowered = text.lower()
    score = 0.8
    if any(term in lowered for term in _FRICTION_TERMS):
        score -= 0.3
    if "workday" in apply_url.lower():
        score -= 0.1
    if "greenhouse" in apply_url.lower() or "lever" in apply_url.lower():
        score += 0.05
    return max(0.0, min(1.0, score))


def _role_clarity_score(text: str) -> float:
    if not text.strip():
        return 0.0
    length_score = min(1.0, len(text) / 1800)
    structure_boost = 0.15 if any(term in text.lower() for term in _CLARITY_POSITIVE) else 0.0
    return max(0.0, min(1.0, 0.75 * length_score + structure_boost))


def _heuristic_confidence(
    *,
    years_min: int | None,
    entry_level_likely: bool | None,
    visa_signal: bool | None,
    required_skills: list[str],
) -> float:
    confidence = 0.35
    if years_min is not None:
        confidence += 0.2
    if entry_level_likely is not None:
        confidence += 0.2
    if visa_signal is not None:
        confidence += 0.15
    if required_skills:
        confidence += 0.2
    return max(0.0, min(0.95, confidence))


def _parse_optional_bool(value: object) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "yes", "1"}:
            return True
        if lowered in {"false", "no", "0"}:
            return False
    return None


def _parse_optional_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        if not math.isfinite(float(value)):
            return None
        parsed = int(value)
        return parsed if 0 <= parsed <= 20 else None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if not text.lstrip("-").isdigit():
            return None
        parsed = int(text)
        return parsed if 0 <= parsed <= 20 else None
    return None


def _parse_score(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        parsed = float(value)
    elif isinstance(value, str):
        try:
            parsed = float(value)
        except ValueError:
            return None
    else:
        return None
    if not math.isfinite(parsed):
        return None
    return max(0.0, min(1.0, parsed))


def _parse_skill_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    parsed: list[str] = []
    for item in value:
        text = str(item).strip().lower()
        if text:
            parsed.append(text)
    return sorted(set(parsed))


def _merge_skills(primary: list[str], secondary: list[str]) -> list[str]:
    return sorted(set(primary).union(secondary))


def extract_job_signals(
    df: pd.DataFrame,
    *,
    title_column: str = TITLE,
    description_column: str = DESCRIPTION_TEXT,
    skills_column: str = SKILLS,
    apply_url_column: str = APPLY_URL,
    remote_flag_column: str = REMOTE_FLAG,
    remote_type_column: str = REMOTE_TYPE,
    use_llm: bool = False,
    model: str = "llama3",
    ollama_url: str = "http://localhost:11434",
    llm_min_confidence: float = 0.7,
) -> pd.DataFrame:
    """Extract matching-relevant job signals with heuristic-first, Ollama-fallback flow."""
    result = df.copy()
    client: OllamaClient | None = None
    if use_llm:
        candidate = OllamaClient(base_url=ollama_url)
        if candidate.is_available():
            client = candidate
        else:
            LOGGER.warning("Ollama is not available at %s.", ollama_url)

    required_skills_all: list[list[str]] = []
    preferred_skills_all: list[list[str]] = []
    exp_min_all: list[int | None] = []
    exp_max_all: list[int | None] = []
    entry_level_all: list[bool | None] = []
    visa_all: list[bool | None] = []
    friction_all: list[float] = []
    clarity_all: list[float] = []
    confidence_all: list[float] = []
    source_all: list[str] = []
    reason_all: list[str] = []

    for _, row in result.iterrows():
        title = _as_text(row.get(title_column, ""))
        description = _as_text(row.get(description_column, ""))
        apply_url = _as_text(row.get(apply_url_column, ""))
        text = f"{title}\n{description}".strip()
        seed_skills = [skill.lower() for skill in _list_from_value(row.get(skills_column))]

        years_min, years_max = _extract_years(text)
        required_skills, preferred_skills = _extract_skills(text, seed_skills)
        entry_level_likely = _entry_level_signal(text, years_min)
        visa_signal = _visa_signal(text)
        friction_score = _application_friction_score(text, apply_url)
        clarity_score = _role_clarity_score(text)
        confidence = _heuristic_confidence(
            years_min=years_min,
            entry_level_likely=entry_level_likely,
            visa_signal=visa_signal,
            required_skills=required_skills,
        )
        source = "heuristic"
        reason = ""

        if client is not None and confidence < llm_min_confidence and text:
            prompt = build_job_signal_prompt(title=title, description=description)
            response = client.generate(prompt, model=model)
            try:
                payload = json.loads(response)
                llm_required = _parse_skill_list(payload.get("required_skills"))
                llm_preferred = _parse_skill_list(payload.get("preferred_skills"))
                llm_min = _parse_optional_int(payload.get("experience_years_min"))
                llm_max = _parse_optional_int(payload.get("experience_years_max"))
                llm_entry = _parse_optional_bool(payload.get("entry_level_likely"))
                llm_visa = _parse_optional_bool(payload.get("visa_sponsorship_signal"))
                llm_friction = _parse_score(payload.get("application_friction_score"))
                llm_clarity = _parse_score(payload.get("role_clarity_score"))
                llm_confidence = _parse_score(payload.get("confidence"))

                required_skills = _merge_skills(required_skills, llm_required)
                preferred_skills = _merge_skills(preferred_skills, llm_preferred)
                if llm_min is not None:
                    years_min = llm_min
                if llm_max is not None:
                    years_max = llm_max
                if llm_entry is not None:
                    entry_level_likely = llm_entry
                if llm_visa is not None:
                    visa_signal = llm_visa
                if llm_friction is not None:
                    friction_score = llm_friction
                if llm_clarity is not None:
                    clarity_score = llm_clarity
                if llm_confidence is not None:
                    confidence = max(confidence, llm_confidence)
                source = "heuristic+ollama"
                reason = str(payload.get("reason", "")).strip()
            except (json.JSONDecodeError, TypeError, ValueError):
                LOGGER.warning("Failed to parse Ollama signal response: %s", response)

        if years_min is None and years_max is not None:
            years_min = years_max
        if years_max is None and years_min is not None:
            years_max = years_min

        remote_flag_value = row.get(remote_flag_column)
        is_remote_flag = False
        if remote_flag_value is not None and not (
            isinstance(remote_flag_value, float) and pd.isna(remote_flag_value)
        ):
            lowered_flag = str(remote_flag_value).strip().lower()
            is_remote_flag = lowered_flag in {"true", "1", "yes"}
        remote_type_value = _as_text(row.get(remote_type_column, "")).lower()
        if remote_type_value == "remote" or is_remote_flag:
            friction_score = min(1.0, friction_score + 0.05)

        required_skills_all.append(required_skills)
        preferred_skills_all.append(preferred_skills)
        exp_min_all.append(years_min)
        exp_max_all.append(years_max)
        entry_level_all.append(entry_level_likely)
        visa_all.append(visa_signal)
        friction_all.append(friction_score)
        clarity_all.append(clarity_score)
        confidence_all.append(confidence)
        source_all.append(source)
        reason_all.append(reason)

    columns = DEFAULT_RESULT_COLUMNS
    result[columns.required_skills_extracted] = required_skills_all
    result[columns.preferred_skills_extracted] = preferred_skills_all
    result[columns.experience_years_min] = exp_min_all
    result[columns.experience_years_max] = exp_max_all
    result[columns.entry_level_likely] = pd.Series(entry_level_all, dtype="object")
    result[columns.visa_sponsorship_signal] = pd.Series(visa_all, dtype="object")
    result[columns.application_friction_score] = friction_all
    result[columns.role_clarity_score] = clarity_all
    result[columns.signal_confidence] = confidence_all
    result[columns.signal_source] = source_all
    result[columns.signal_reason] = reason_all
    return result
