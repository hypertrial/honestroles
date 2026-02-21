from __future__ import annotations

from dataclasses import dataclass

import honestroles.schema as schema


@dataclass(frozen=True)
class CandidateProfile:
    """Candidate preferences used for agent-driven job matching."""

    target_roles: tuple[str, ...] = (
        "data scientist",
        "machine learning engineer",
        "data analyst",
    )
    required_skills: tuple[str, ...] = ("python", "sql")
    preferred_skills: tuple[str, ...] = (
        "pandas",
        "numpy",
        "scikit-learn",
        "statistics",
        "machine learning",
    )
    preferred_cities: tuple[str, ...] = ()
    preferred_regions: tuple[str, ...] = ()
    preferred_countries: tuple[str, ...] = ("US", "CA")
    remote_ok: bool = True
    min_salary: float | None = None
    salary_currency: str | None = "USD"
    max_years_experience: int = 2
    needs_visa_sponsorship: bool | None = None
    graduation_year: int | None = None

    @classmethod
    def mds_new_grad(cls) -> CandidateProfile:
        """Recommended defaults for Master's in Data Science new grads."""
        return cls()


@dataclass(frozen=True)
class MatchWeights:
    """Default weight profile tuned for early-career data candidates."""

    skills: float = 0.24
    entry_level: float = 0.22
    experience: float = 0.16
    visa: float = 0.16
    role_alignment: float = 0.12
    graduation_alignment: float = 0.10
    salary: float = 0.08
    location: float = 0.08
    quality: float = 0.04

    def as_dict(self) -> dict[str, float]:
        return {
            "skills": self.skills,
            "entry_level": self.entry_level,
            "experience": self.experience,
            "visa": self.visa,
            "role_alignment": self.role_alignment,
            "graduation_alignment": self.graduation_alignment,
            "salary": self.salary,
            "location": self.location,
            "quality": self.quality,
        }


@dataclass(frozen=True)
class MatchResultColumns:
    """Standard output columns for ranking and planning."""

    fit_score: str = schema.FIT_SCORE
    fit_breakdown: str = schema.FIT_BREAKDOWN
    missing_requirements: str = schema.MISSING_REQUIREMENTS
    why_match: str = schema.WHY_MATCH
    next_actions: str = schema.NEXT_ACTIONS
    signal_confidence: str = schema.SIGNAL_CONFIDENCE
    signal_source: str = schema.SIGNAL_SOURCE
    signal_reason: str = schema.SIGNAL_REASON
    required_skills_extracted: str = schema.REQUIRED_SKILLS_EXTRACTED
    preferred_skills_extracted: str = schema.PREFERRED_SKILLS_EXTRACTED
    experience_years_min: str = schema.EXPERIENCE_YEARS_MIN
    experience_years_max: str = schema.EXPERIENCE_YEARS_MAX
    entry_level_likely: str = schema.ENTRY_LEVEL_LIKELY
    visa_sponsorship_signal: str = schema.VISA_SPONSORSHIP_SIGNAL
    application_friction_score: str = schema.APPLICATION_FRICTION_SCORE
    role_clarity_score: str = schema.ROLE_CLARITY_SCORE


DEFAULT_RESULT_COLUMNS = MatchResultColumns()
