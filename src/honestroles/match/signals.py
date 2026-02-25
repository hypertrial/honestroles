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
    EXPERIENCE_YEARS_MAX,
    EXPERIENCE_YEARS_MIN,
    REMOTE_FLAG,
    REMOTE_TYPE,
    SKILLS,
    TITLE,
    VISA_SPONSORSHIP,
)

LOGGER = logging.getLogger(__name__)

_SIGNAL_PARSE_TEXT_MAX_CHARS = 800

_EXPERIENCE_RANGE_RE = re.compile(
    r"\b(\d{1,2})\s*(?:-|to)\s*(\d{1,2})\+?\s*(?:years?|yrs?)\b(?:\s+of\s+experience)?",
    re.IGNORECASE,
)
_EXPERIENCE_MIN_RE = re.compile(
    r"\b(?:at\s+least|min(?:imum)?\s+)?(\d{1,2})\+?\s*(?:years?|yrs?)\b(?:\s+of\s+experience)?",
    re.IGNORECASE,
)

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

_SKILL_ALIASES = {
    "python": ("python",),
    "sql": ("sql",),
    "pandas": ("pandas",),
    "numpy": ("numpy",),
    "scikit-learn": ("scikit-learn", "sklearn"),
    "spark": ("spark", "apache spark", "pyspark"),
    "airflow": ("airflow", "apache airflow"),
    "dbt": ("dbt",),
    "tableau": ("tableau",),
    "power bi": ("power bi", "powerbi"),
    "statistics": ("statistics",),
    "machine learning": ("machine learning",),
    "deep learning": ("deep learning",),
    "nlp": ("nlp", "natural language processing"),
    "pytorch": ("pytorch",),
    "tensorflow": ("tensorflow",),
    "snowflake": ("snowflake",),
    "bigquery": ("bigquery",),
    "aws": ("aws", "amazon web services"),
    "gcp": ("gcp", "google cloud", "google cloud platform"),
    "azure": ("azure", "microsoft azure"),
    "javascript": ("javascript",),
    "typescript": ("typescript",),
    "react": ("react", "reactjs"),
    "node": ("node", "nodejs", "node.js"),
    "docker": ("docker",),
    "kubernetes": ("kubernetes", "k8s"),
    "postgres": ("postgres", "postgresql"),
    "mysql": ("mysql",),
    "java": ("java",),
    "c++": ("c++", "cpp"),
    "c#": ("c#", "csharp"),
    "go": ("go", "golang"),
    "rust": ("rust",),
    "linux": ("linux",),
    "git": ("git",),
    "bash": ("bash",),
    "terraform": ("terraform",),
    "excel": ("excel", "microsoft excel"),
    "communication": ("communication",),
    "leadership": ("leadership",),
    "project management": ("project management",),
    "stakeholder management": ("stakeholder management",),
    "customer service": ("customer service",),
    "customer success": ("customer success",),
    "salesforce": ("salesforce",),
    "agile": ("agile",),
    "scrum": ("scrum",),
    "analytics": ("analytics",),
    "compliance": ("compliance",),
    "risk management": ("risk management",),
    "product management": ("product management", "product manager", "product owner"),
    "account management": ("account management", "account manager"),
    "business development": ("business development",),
    "operations": ("operations",),
    "finance": ("finance",),
    "accounting": ("accounting",),
    "marketing": ("marketing",),
    "design": ("design",),
    "ux": ("ux", "user experience"),
    "ui": ("ui", "user interface"),
    "recruiting": ("recruiting", "recruiter", "talent acquisition"),
    "sales": ("sales",),
    "support": ("support",),
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

_SENIORITY_EXPERIENCE_RULES: tuple[tuple[re.Pattern[str], tuple[int, int]], ...] = (
    (
        re.compile(
            r"\b(?:intern|entry[- ]level|new grad|new graduate|recent graduate|junior|jr\.?)\b",
            re.IGNORECASE,
        ),
        (0, 2),
    ),
    (re.compile(r"\bmid(?:-level)?\b", re.IGNORECASE), (2, 5)),
    (re.compile(r"\b(?:senior|sr\.?)\b", re.IGNORECASE), (4, 8)),
    (re.compile(r"\b(?:staff|principal)\b", re.IGNORECASE), (7, 12)),
    (
        re.compile(
            r"\b(?:lead|manager|director|head|vp|vice president|chief)\b",
            re.IGNORECASE,
        ),
        (6, 12),
    ),
)


def _compile_skill_pattern(term: str) -> re.Pattern[str]:
    escaped = re.escape(term).replace(r"\ ", r"\s+")
    return re.compile(rf"(?<![a-z0-9]){escaped}(?![a-z0-9])", re.IGNORECASE)


def _skill_fragment(term: str) -> str:
    return re.escape(term).replace(r"\ ", r"\s+")


_SKILL_ALIAS_TO_CANONICAL = {
    alias: canonical
    for canonical, aliases in _SKILL_ALIASES.items()
    for alias in aliases
}
_SKILL_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    (canonical, _compile_skill_pattern(alias))
    for alias, canonical in sorted(_SKILL_ALIAS_TO_CANONICAL.items())
]
_SKILL_EXTRACT_RE = re.compile(
    r"(?<![a-z0-9])("
    + "|".join(_skill_fragment(alias) for alias in sorted(_SKILL_ALIAS_TO_CANONICAL, key=len, reverse=True))
    + r")(?![a-z0-9])",
    re.IGNORECASE,
)


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


def _normalize_seed_skills(values: list[str]) -> list[str]:
    normalized: set[str] = set()
    for value in values:
        token = value.strip().lower().replace("-", " ")
        canonical = _SKILL_ALIAS_TO_CANONICAL.get(token)
        if canonical is not None:
            normalized.add(canonical)
    return sorted(normalized)


def _canonical_skill_from_match(value: str) -> str | None:
    token = value.strip().lower().replace("-", " ")
    return _SKILL_ALIAS_TO_CANONICAL.get(token)


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


def _infer_years_from_seniority(text: str) -> tuple[int | None, int | None]:
    for pattern, (min_years, max_years) in _SENIORITY_EXPERIENCE_RULES:
        if pattern.search(text):
            return min_years, max_years
    return None, None


def _extract_skills(text: str, seed_skills: list[str]) -> tuple[list[str], list[str]]:
    lowered = text.lower()
    chunks = [chunk.strip().lower() for chunk in re.split(r"[\n.;]", text) if chunk.strip()]
    required: set[str] = {skill.lower() for skill in seed_skills}
    preferred: set[str] = set()

    for skill, pattern in _SKILL_PATTERNS:
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


def _extract_required_skills_series(text: pd.Series) -> pd.Series:
    lowered = text.astype("string").fillna("").str.lower()
    extracted = pd.Series([[] for _ in range(len(lowered))], index=lowered.index, dtype="object")
    if lowered.empty:
        return extracted
    matches = lowered.str.extractall(_SKILL_EXTRACT_RE)
    if matches.empty:
        return extracted
    canonical = matches[0].map(_canonical_skill_from_match).dropna()
    if canonical.empty:
        return extracted
    grouped = canonical.groupby(level=0).agg(lambda values: sorted(set(values)))
    extracted.loc[grouped.index] = grouped.astype("object")
    return extracted


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


def _series_text(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series("", index=df.index, dtype="string")
    series = df[column]
    text = series.where(series.map(lambda value: isinstance(value, str)), "")
    return text.fillna("").astype("string")


def _series_lists(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series([[] for _ in range(len(df))], index=df.index, dtype="object")
    return df[column].map(_list_from_value).astype("object")


def _series_optional_int(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series([None] * len(df), index=df.index, dtype="object")
    return df[column].map(_parse_optional_int).astype("object")


def _series_optional_bool(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series([None] * len(df), index=df.index, dtype="object")
    return df[column].map(_parse_optional_bool).astype("object")


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
    title_text = _series_text(result, title_column)
    description_text = _series_text(result, description_column).str.slice(0, _SIGNAL_PARSE_TEXT_MAX_CHARS)
    apply_urls = _series_text(result, apply_url_column)
    text = title_text.str.cat(description_text, sep="\n").str.strip()
    seed_skills = _series_lists(result, skills_column).map(
        lambda values: _normalize_seed_skills(
            [str(skill).strip().lower() for skill in values if str(skill).strip()]
        )
    )

    years = text.map(_extract_years)
    exp_min = years.map(lambda value: value[0]).astype("object")
    exp_max = years.map(lambda value: value[1]).astype("object")
    inferred_years = text.map(_infer_years_from_seniority)
    inferred_min = inferred_years.map(lambda value: value[0]).astype("object")
    inferred_max = inferred_years.map(lambda value: value[1]).astype("object")
    exp_min = exp_min.where(exp_min.notna(), inferred_min)
    exp_max = exp_max.where(exp_max.notna(), inferred_max)

    extracted_required = _extract_required_skills_series(text)
    required_skills = pd.Series(
        [
            _merge_skills(seed, extracted)
            for seed, extracted in zip(seed_skills.tolist(), extracted_required.tolist())
        ],
        index=result.index,
        dtype="object",
    )
    preferred_skills = pd.Series([[] for _ in range(len(result))], index=result.index, dtype="object")

    entry_level = pd.Series(
        [
            _entry_level_signal(text_value, years_min)
            for text_value, years_min in zip(text.tolist(), exp_min.tolist())
        ],
        index=result.index,
        dtype="object",
    )
    visa_signal = text.map(_visa_signal).astype("object")
    friction = pd.Series(
        [
            _application_friction_score(text_value, apply_url)
            for text_value, apply_url in zip(text.tolist(), apply_urls.tolist())
        ],
        index=result.index,
        dtype="float64",
    )
    clarity = text.map(_role_clarity_score).astype("float64")
    confidence = pd.Series(
        [
            _heuristic_confidence(
                years_min=years_min,
                entry_level_likely=entry,
                visa_signal=visa,
                required_skills=req,
            )
            for years_min, entry, visa, req in zip(
                exp_min.tolist(),
                entry_level.tolist(),
                visa_signal.tolist(),
                required_skills.tolist(),
            )
        ],
        index=result.index,
        dtype="float64",
    )
    signal_source = pd.Series("heuristic", index=result.index, dtype="object")
    signal_reason = pd.Series("", index=result.index, dtype="object")

    source_exp_min = _series_optional_int(result, EXPERIENCE_YEARS_MIN)
    source_exp_max = _series_optional_int(result, EXPERIENCE_YEARS_MAX)
    exp_min = source_exp_min.where(source_exp_min.notna(), exp_min)
    exp_max = source_exp_max.where(source_exp_max.notna(), exp_max)

    source_visa = _series_optional_bool(result, VISA_SPONSORSHIP)
    visa_signal = source_visa.where(source_visa.notna(), visa_signal)

    exp_min = exp_min.where(exp_min.notna(), exp_max)
    exp_max = exp_max.where(exp_max.notna(), exp_min)

    remote_flag = _series_optional_bool(result, remote_flag_column).fillna(False)
    remote_type = _series_text(result, remote_type_column).str.strip().str.lower().eq("remote")
    friction = (friction + (remote_flag.astype("bool") | remote_type).astype("float64") * 0.05).clip(
        upper=1.0
    )

    if client is not None:
        llm_candidates = (confidence < llm_min_confidence) & text.str.strip().ne("")
        for index in llm_candidates.index[llm_candidates]:
            prompt = build_job_signal_prompt(
                title=title_text.loc[index],
                description=description_text.loc[index],
            )
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

                required_skills.at[index] = _merge_skills(required_skills.at[index], llm_required)
                preferred_skills.at[index] = _merge_skills(preferred_skills.at[index], llm_preferred)
                if llm_min is not None:
                    exp_min.at[index] = llm_min
                if llm_max is not None:
                    exp_max.at[index] = llm_max
                if llm_entry is not None:
                    entry_level.at[index] = llm_entry
                if llm_visa is not None:
                    visa_signal.at[index] = llm_visa
                if llm_friction is not None:
                    friction.at[index] = llm_friction
                if llm_clarity is not None:
                    clarity.at[index] = llm_clarity
                if llm_confidence is not None:
                    confidence.at[index] = max(float(confidence.at[index]), llm_confidence)
                signal_source.at[index] = "heuristic+ollama"
                signal_reason.at[index] = str(payload.get("reason", "")).strip()
            except (json.JSONDecodeError, TypeError, ValueError):
                LOGGER.warning("Failed to parse Ollama signal response: %s", response)

    columns = DEFAULT_RESULT_COLUMNS
    result[columns.required_skills_extracted] = required_skills
    result[columns.preferred_skills_extracted] = preferred_skills
    result[columns.experience_years_min] = exp_min.astype("object")
    result[columns.experience_years_max] = exp_max.astype("object")
    result[columns.entry_level_likely] = entry_level.astype("object")
    result[columns.visa_sponsorship_signal] = visa_signal.astype("object")
    result[columns.application_friction_score] = friction.astype("float64")
    result[columns.role_clarity_score] = clarity.astype("float64")
    result[columns.signal_confidence] = confidence.astype("float64")
    result[columns.signal_source] = signal_source.astype("object")
    result[columns.signal_reason] = signal_reason.astype("object")
    return result
