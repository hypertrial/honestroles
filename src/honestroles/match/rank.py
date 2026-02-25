from __future__ import annotations

from collections.abc import Callable
from datetime import date
import re
from typing import Any, Literal

import pandas as pd

from honestroles.match.models import (
    DEFAULT_RESULT_COLUMNS,
    CandidateProfile,
    MatchWeights,
)
from honestroles.match.signals import extract_job_signals
from honestroles.schema import (
    ACTIVE_LIKELIHOOD,
    DESCRIPTION_TEXT,
    LOCATION_RAW,
    QUALITY_SCORE,
    RATING,
    REMOTE_FLAG,
    REMOTE_TYPE,
    SALARY_CONFIDENCE,
    SALARY_CURRENCY,
    SALARY_MAX,
    SKILLS,
    TECH_STACK,
    TITLE,
    CITY,
    COUNTRY,
    REGION,
)

ComponentScorer = Callable[[pd.Series, CandidateProfile], float]


def _as_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_skill_set(value: object) -> set[str]:
    if value is None:
        return set()
    if isinstance(value, float) and pd.isna(value):
        return set()
    if isinstance(value, str):
        text = value.strip().lower()
        return {text} if text else set()
    if isinstance(value, list):
        return {str(item).strip().lower() for item in value if str(item).strip()}
    return {str(value).strip().lower()}


def _collect_job_skills(row: pd.Series, columns: Any) -> set[str]:
    skills = set()
    skills |= _to_skill_set(row.get(SKILLS))
    skills |= _to_skill_set(row.get(TECH_STACK))
    skills |= _to_skill_set(row.get(columns.required_skills_extracted))
    skills |= _to_skill_set(row.get(columns.preferred_skills_extracted))
    return skills


def _skill_score(row: pd.Series, profile: CandidateProfile, columns: Any) -> tuple[float, list[str]]:
    required = {skill.lower() for skill in profile.required_skills}
    preferred = {skill.lower() for skill in profile.preferred_skills}
    available = _collect_job_skills(row, columns)

    missing = sorted(required - available)
    if not required:
        required_score = 1.0
    else:
        required_score = (len(required) - len(missing)) / len(required)

    preferred_overlap = len(preferred & available)
    preferred_score = 0.0 if not preferred else preferred_overlap / len(preferred)
    score = min(1.0, 0.85 * required_score + 0.15 * preferred_score)
    return score, missing


def _entry_level_score(row: pd.Series, columns: Any) -> float:
    value = row.get(columns.entry_level_likely)
    if value is True:
        return 1.0
    if value is False:
        return 0.15
    return 0.55


def _experience_score(row: pd.Series, profile: CandidateProfile, columns: Any) -> float:
    if profile.max_years_experience < 0:
        return 0.0
    minimum = row.get(columns.experience_years_min)
    if minimum is None or (isinstance(minimum, float) and pd.isna(minimum)):
        return 0.6
    try:
        minimum_value = int(minimum)
    except (TypeError, ValueError):
        return 0.5
    if minimum_value <= profile.max_years_experience:
        return 1.0
    gap = minimum_value - profile.max_years_experience
    return max(0.0, 1.0 - (gap * 0.25))


def _visa_score(
    row: pd.Series,
    profile: CandidateProfile,
    columns: Any,
    *,
    ranking_profile: Literal["job_seeker_v2", "legacy"] = "legacy",
) -> float:
    if profile.needs_visa_sponsorship is not True:
        return 1.0
    if ranking_profile == "job_seeker_v2" and row.get(columns.citizenship_required) is True:
        return 0.0
    value = row.get(columns.visa_sponsorship_signal)
    if value is True:
        return 1.0
    if value is False:
        return 0.0
    if ranking_profile == "job_seeker_v2" and row.get(columns.work_authorization_required) is True:
        return 0.3
    return 0.5


def _salary_score(row: pd.Series, profile: CandidateProfile) -> float:
    if profile.min_salary is None:
        return 0.7
    salary_max = _as_float(row.get(SALARY_MAX))
    if salary_max is None:
        return 0.45
    currency = row.get(SALARY_CURRENCY)
    if (
        isinstance(currency, str)
        and isinstance(profile.salary_currency, str)
        and currency.strip().upper() != profile.salary_currency.strip().upper()
    ):
        return 0.4
    if salary_max >= profile.min_salary:
        return 1.0
    return max(0.0, salary_max / profile.min_salary)


def _location_score(row: pd.Series, profile: CandidateProfile) -> float:
    remote_flag_raw = row.get(REMOTE_FLAG)
    remote_flag = False
    if remote_flag_raw is not None and not (
        isinstance(remote_flag_raw, float) and pd.isna(remote_flag_raw)
    ):
        remote_flag = str(remote_flag_raw).strip().lower() in {"true", "1", "yes"}
    remote_type = str(row.get(REMOTE_TYPE, "")).strip().lower()
    if profile.remote_ok and (remote_flag or remote_type == "remote"):
        return 1.0

    city = str(row.get(CITY, "")).strip().lower()
    region = str(row.get(REGION, "")).strip().lower()
    country = str(row.get(COUNTRY, "")).strip().upper()
    location_raw = str(row.get(LOCATION_RAW, "")).strip().lower()

    preferred_cities = {item.lower() for item in profile.preferred_cities}
    preferred_regions = {item.lower() for item in profile.preferred_regions}
    preferred_countries = {item.upper() for item in profile.preferred_countries}

    if preferred_cities and city in preferred_cities:
        return 1.0
    if preferred_regions and region in preferred_regions:
        return 0.9
    if preferred_countries and country in preferred_countries:
        return 0.85
    if preferred_cities and any(city_pref in location_raw for city_pref in preferred_cities):
        return 0.75
    if preferred_regions and any(region_pref in location_raw for region_pref in preferred_regions):
        return 0.7
    if preferred_cities or preferred_regions or preferred_countries:
        return 0.25
    return 0.7


def _quality_score(row: pd.Series, columns: Any) -> float:
    rating_value = _as_float(row.get(RATING))
    if rating_value is not None:
        return max(0.0, min(1.0, rating_value))
    quality_value = _as_float(row.get(QUALITY_SCORE))
    if quality_value is not None:
        return max(0.0, min(1.0, quality_value))
    clarity = _as_float(row.get(columns.role_clarity_score))
    return 0.5 if clarity is None else clarity


def _active_score(row: pd.Series, columns: Any) -> float:
    value = _as_float(row.get(columns.active_likelihood))
    if value is None:
        value = _as_float(row.get(ACTIVE_LIKELIHOOD))
    if value is None:
        return 0.5
    return max(0.0, min(1.0, value))


def _friction_score(row: pd.Series, columns: Any) -> float:
    value = _as_float(row.get(columns.application_friction_score))
    if value is None:
        return 0.6
    return max(0.0, min(1.0, 1.0 - value))


def _confidence_score(row: pd.Series, columns: Any) -> float:
    signal_confidence = _as_float(row.get(columns.signal_confidence))
    if signal_confidence is not None:
        return max(0.0, min(1.0, signal_confidence))
    salary_confidence = _as_float(row.get(SALARY_CONFIDENCE))
    if salary_confidence is not None:
        return max(0.0, min(1.0, salary_confidence))
    return 0.5


def _role_alignment_score(row: pd.Series, profile: CandidateProfile) -> float:
    target_roles = [role.lower().strip() for role in profile.target_roles if role.strip()]
    if not target_roles:
        return 0.6

    title = str(row.get(TITLE, "")).lower()
    description = str(row.get(DESCRIPTION_TEXT, "")).lower()
    text = f"{title} {description}".strip()
    if not text:
        return 0.3

    if any(role in title for role in target_roles):
        return 1.0
    if any(role in text for role in target_roles):
        return 0.85

    tokens = set(re.findall(r"[a-z0-9]+", text))
    role_tokens = {
        token
        for role in target_roles
        for token in re.findall(r"[a-z0-9]+", role)
        if len(token) >= 3
    }
    if role_tokens and (role_tokens & tokens):
        overlap = len(role_tokens & tokens) / len(role_tokens)
        return max(0.35, min(0.75, overlap))
    return 0.2


def _graduation_alignment_score(row: pd.Series, profile: CandidateProfile, columns: Any) -> float:
    if profile.graduation_year is None:
        return 0.6
    current_year = date.today().year
    years_since_grad = current_year - profile.graduation_year
    if years_since_grad < 0:
        years_since_grad = 0

    entry_level = row.get(columns.entry_level_likely)
    exp_min = row.get(columns.experience_years_min)
    exp_min_value: int | None = None
    if exp_min is not None and not (isinstance(exp_min, float) and pd.isna(exp_min)):
        try:
            exp_min_value = int(exp_min)
        except (TypeError, ValueError):
            exp_min_value = None

    if years_since_grad <= 2:
        if entry_level is True:
            return 1.0
        if exp_min_value is not None and exp_min_value > 2:
            return 0.15
        return 0.75
    if years_since_grad <= 5:
        if exp_min_value is not None and exp_min_value > 5:
            return 0.4
        return 0.65
    return 0.45


def _score_components(
    row: pd.Series,
    profile: CandidateProfile,
    columns: Any,
    *,
    ranking_profile: Literal["job_seeker_v2", "legacy"] = "job_seeker_v2",
    component_overrides: dict[str, ComponentScorer] | None = None,
) -> tuple[dict[str, float], list[str]]:
    skills, missing_skills = _skill_score(row, profile, columns)
    components = {
        "skills": skills,
        "entry_level": _entry_level_score(row, columns),
        "experience": _experience_score(row, profile, columns),
        "visa": _visa_score(row, profile, columns, ranking_profile=ranking_profile),
        "role_alignment": _role_alignment_score(row, profile),
        "graduation_alignment": _graduation_alignment_score(row, profile, columns),
        "salary": _salary_score(row, profile),
        "location": _location_score(row, profile),
        "quality": _quality_score(row, columns),
    }
    if ranking_profile == "job_seeker_v2":
        components["active"] = _active_score(row, columns)
        components["friction"] = _friction_score(row, columns)
        components["confidence"] = _confidence_score(row, columns)
    if component_overrides:
        for name, scorer in component_overrides.items():
            if name not in components:
                continue
            try:
                override_score = float(scorer(row, profile))
            except (TypeError, ValueError):
                continue
            components[name] = max(0.0, min(1.0, override_score))

    missing: list[str] = []
    if missing_skills:
        missing.append(f"missing_skills:{','.join(missing_skills)}")
    if profile.needs_visa_sponsorship is True and row.get(columns.visa_sponsorship_signal) is False:
        missing.append("visa_sponsorship_not_available")
    if (
        ranking_profile == "job_seeker_v2"
        and profile.needs_visa_sponsorship is True
        and row.get(columns.work_authorization_required) is True
        and row.get(columns.visa_sponsorship_signal) is not True
    ):
        missing.append("work_authorization_constraint")
    if (
        ranking_profile == "job_seeker_v2"
        and profile.needs_visa_sponsorship is True
        and row.get(columns.citizenship_required) is True
    ):
        missing.append("citizenship_constraint")
    if ranking_profile == "job_seeker_v2":
        active_value = _as_float(row.get(columns.active_likelihood))
        if active_value is not None and active_value < 0.35:
            missing.append("likely_stale_posting")

    min_exp = row.get(columns.experience_years_min)
    if min_exp is not None and not (isinstance(min_exp, float) and pd.isna(min_exp)):
        try:
            if int(min_exp) > profile.max_years_experience:
                missing.append("experience_requirement_above_profile")
        except (TypeError, ValueError):
            pass

    if profile.min_salary is not None:
        salary_max = _as_float(row.get(SALARY_MAX))
        if salary_max is not None and salary_max < profile.min_salary:
            missing.append("salary_below_minimum")
    return components, missing


def _weighted_score(components: dict[str, float], weights: dict[str, float]) -> float:
    available = {name: weights[name] for name in components if name in weights}
    total = sum(available.values())
    if total <= 0:
        return 0.0
    score = sum(components[name] * available[name] for name in available)
    return score / total


def _why_match(components: dict[str, float], missing: list[str], title: str) -> str:
    top = sorted(components.items(), key=lambda item: item[1], reverse=True)[:3]
    strengths = ", ".join(name for name, value in top if value >= 0.65)
    if strengths:
        summary = f"{title}: strong on {strengths}."
    else:
        summary = f"{title}: moderate fit with mixed signals."
    if missing:
        return f"{summary} Watchouts: {', '.join(missing)}."
    return summary


def rank_jobs(
    df: pd.DataFrame,
    *,
    profile: CandidateProfile | None = None,
    use_llm_signals: bool = False,
    model: str = "llama3",
    ollama_url: str = "http://localhost:11434",
    as_of: str | pd.Timestamp | None = None,
    weights: dict[str, float] | None = None,
    component_overrides: dict[str, ComponentScorer] | None = None,
    ranking_profile: Literal["job_seeker_v2", "legacy"] = "job_seeker_v2",
    sort_desc: bool = True,
    top_n: int | None = None,
) -> pd.DataFrame:
    """Rank jobs for a candidate profile with explainable component scores."""
    candidate = profile or CandidateProfile.mds_new_grad()
    columns = DEFAULT_RESULT_COLUMNS
    scored = extract_job_signals(
        df,
        use_llm=use_llm_signals,
        model=model,
        ollama_url=ollama_url,
        as_of=as_of,
    )

    weight_map = MatchWeights().as_dict()
    if weights:
        weight_map.update(weights)

    fit_scores: list[float] = []
    breakdowns: list[dict[str, float]] = []
    missing_all: list[list[str]] = []
    why_all: list[str] = []

    for _, row in scored.iterrows():
        components, missing = _score_components(
            row,
            candidate,
            columns,
            ranking_profile=ranking_profile,
            component_overrides=component_overrides,
        )
        fit_score = _weighted_score(components, weight_map)
        rounded = {name: round(value, 4) for name, value in components.items()}
        title = str(row.get(TITLE, "Untitled role"))

        fit_scores.append(round(fit_score, 6))
        breakdowns.append(rounded)
        missing_all.append(missing)
        why_all.append(_why_match(rounded, missing, title))

    scored[columns.fit_score] = fit_scores
    scored[columns.fit_breakdown] = breakdowns
    scored[columns.missing_requirements] = missing_all
    scored[columns.why_match] = why_all
    ranked = scored.sort_values(columns.fit_score, ascending=not sort_desc).reset_index(drop=True)
    if top_n is not None and top_n >= 0:
        return ranked.head(top_n).reset_index(drop=True)
    return ranked


def build_application_plan(
    ranked_df: pd.DataFrame,
    *,
    profile: CandidateProfile | None = None,
    top_n: int = 20,
    max_actions_per_job: int = 4,
    include_diagnostics: bool = True,
) -> pd.DataFrame:
    """Build concise next actions for top-ranked jobs."""
    candidate = profile or CandidateProfile.mds_new_grad()
    columns = DEFAULT_RESULT_COLUMNS
    result = ranked_df.copy()
    if columns.fit_score not in result.columns:
        result = rank_jobs(
            result,
            profile=candidate,
            use_llm_signals=False,
            ranking_profile="job_seeker_v2",
            top_n=None,
        )

    actions_all: list[list[str]] = []
    red_flags_all: list[list[str]] = []
    questions_all: list[list[str]] = []
    offer_risk_all: list[str] = []
    effort_all: list[int] = []
    for _, row in result.iterrows():
        actions: list[str] = []
        red_flags: list[str] = []
        ask_recruiter: list[str] = []
        missing = row.get(columns.missing_requirements, [])
        if isinstance(missing, list):
            missing_items = [str(item) for item in missing]
        else:
            missing_items = []

        skill_gaps = [
            item.split(":", 1)[1]
            for item in missing_items
            if item.startswith("missing_skills:")
        ]
        if skill_gaps:
            actions.append(f"Highlight projects/coursework covering: {skill_gaps[0]}.")
        actions.append("Tailor resume bullets to listed responsibilities and impact metrics.")

        role_category = str(row.get("role_category", "")).strip().lower()
        if role_category in {"engineering", "data"}:
            actions.append("Lead with quantified technical outcomes in the first 3 resume bullets.")
        elif role_category == "product":
            actions.append("Emphasize roadmap prioritization and cross-functional delivery examples.")

        if "experience_requirement_above_profile" in missing_items:
            actions.append("Use a summary section to frame equivalent internship/research depth.")
        if candidate.needs_visa_sponsorship is True:
            visa_signal = row.get(columns.visa_sponsorship_signal)
            if visa_signal is None:
                actions.append("Confirm sponsorship policy during recruiter screen.")
                ask_recruiter.append("Does this role support visa sponsorship for this location?")
            elif visa_signal is False:
                actions.append("Deprioritize unless a referral can validate sponsorship.")
                red_flags.append("visa_sponsorship_not_available")
        if row.get(columns.citizenship_required) is True:
            red_flags.append("citizenship_constraint")
            ask_recruiter.append("Is there any flexibility on citizenship or permanent residency requirements?")
        if row.get(columns.work_authorization_required) is True and row.get(columns.visa_sponsorship_signal) is not True:
            red_flags.append("work_authorization_constraint")
            ask_recruiter.append("Is employer-sponsored work authorization available for this role?")
        if "likely_stale_posting" in missing_items:
            red_flags.append("likely_stale_posting")
            ask_recruiter.append("Is this requisition still actively hiring and at what stage?")
        if "salary_below_minimum" in missing_items:
            actions.append("Prioritize only if brand/network upside outweighs compensation gap.")
            red_flags.append("salary_below_minimum")

        active_likelihood = _as_float(row.get(columns.active_likelihood))
        if active_likelihood is not None and active_likelihood < 0.55:
            ask_recruiter.append("What is the expected hiring timeline for this role?")
            if active_likelihood < 0.35:
                red_flags.append("likely_stale_posting")

        friction_value = _as_float(row.get(columns.application_friction_score))
        if friction_value is not None and friction_value < 0.45:
            actions.append("Prepare for higher-friction process steps (assessment/case/extra rounds).")

        if not missing_items:
            actions.append("Apply now; strong profile alignment for an early-career candidate.")
        deduped_actions = list(dict.fromkeys(actions))
        if max_actions_per_job >= 0:
            deduped_actions = deduped_actions[:max_actions_per_job]
        actions_all.append(deduped_actions)

        deduped_flags = list(dict.fromkeys(red_flags))
        deduped_questions = list(dict.fromkeys(ask_recruiter))
        red_flags_all.append(deduped_flags)
        questions_all.append(deduped_questions)

        confidence_value = _as_float(row.get(columns.signal_confidence))
        risk_points = len(deduped_flags) * 2
        if confidence_value is not None and confidence_value < 0.55:
            risk_points += 1
        if active_likelihood is not None and active_likelihood < 0.55:
            risk_points += 1
        if risk_points >= 5:
            risk_band = "high"
        elif risk_points >= 3:
            risk_band = "medium"
        else:
            risk_band = "low"
        offer_risk_all.append(risk_band)

        normalized_friction = 0.6 if friction_value is None else max(0.0, min(1.0, friction_value))
        effort = int(round(20 + (1.0 - normalized_friction) * 40 + min(len(missing_items), 4) * 6))
        effort_all.append(max(10, effort))

    result[columns.next_actions] = actions_all
    if include_diagnostics:
        result[columns.red_flags] = red_flags_all
        result[columns.must_ask_recruiter] = questions_all
        result[columns.offer_risk] = offer_risk_all
        result[columns.application_effort_minutes] = effort_all
    if top_n >= 0:
        return result.head(top_n).reset_index(drop=True)
    return result
