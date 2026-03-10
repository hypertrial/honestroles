from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal

SCHEMA_VERSION = "1.0"
RECOMMENDATION_POLICY_SCHEMA_VERSION = "1.0"
EVAL_THRESHOLDS_SCHEMA_VERSION = "1.0"

WorkMode = Literal["remote", "hybrid", "onsite", "unknown"]
Seniority = Literal["junior", "mid", "senior", "staff", "principal"]
EmploymentType = Literal[
    "full_time",
    "part_time",
    "contract",
    "internship",
    "temporary",
    "unknown",
]
FeedbackEventType = Literal["not_relevant", "applied", "interviewed"]

SIGNAL_KEYS: tuple[str, ...] = (
    "skills",
    "title",
    "location_work_mode",
    "seniority",
    "recency",
    "compensation",
)


@dataclass(frozen=True, slots=True)
class SalaryTargets:
    minimum: float | None = None
    maximum: float | None = None
    currency: str | None = None
    interval: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "minimum": self.minimum,
            "maximum": self.maximum,
            "currency": self.currency,
            "interval": self.interval,
        }


@dataclass(frozen=True, slots=True)
class VisaWorkAuth:
    requires_sponsorship: bool | None = None
    authorized_locations: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "requires_sponsorship": self.requires_sponsorship,
            "authorized_locations": list(self.authorized_locations),
        }


@dataclass(frozen=True, slots=True)
class CandidateProfile:
    profile_id: str
    skills: tuple[str, ...] = ()
    titles: tuple[str, ...] = ()
    years_experience: float | None = None
    locations: tuple[str, ...] = ()
    work_mode_preferences: tuple[WorkMode, ...] = ()
    seniority_targets: tuple[Seniority, ...] = ()
    salary_targets: SalaryTargets = field(default_factory=SalaryTargets)
    visa_work_auth: VisaWorkAuth = field(default_factory=VisaWorkAuth)
    employment_type_preferences: tuple[EmploymentType, ...] = ()
    parser_confidence: float | None = None
    parser_unknown_tokens: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "profile_id": self.profile_id,
            "skills": list(self.skills),
            "titles": list(self.titles),
            "years_experience": self.years_experience,
            "locations": list(self.locations),
            "work_mode_preferences": list(self.work_mode_preferences),
            "seniority_targets": list(self.seniority_targets),
            "salary_targets": self.salary_targets.to_dict(),
            "visa_work_auth": self.visa_work_auth.to_dict(),
            "employment_type_preferences": list(self.employment_type_preferences),
            "parser_confidence": self.parser_confidence,
            "parser_unknown_tokens": list(self.parser_unknown_tokens),
        }


@dataclass(frozen=True, slots=True)
class RecommendationPolicy:
    weights: dict[str, float] = field(
        default_factory=lambda: {
            "skills": 0.35,
            "title": 0.20,
            "location_work_mode": 0.15,
            "seniority": 0.10,
            "recency": 0.10,
            "compensation": 0.10,
        }
    )
    reason_limit: int = 3

    def normalized_weights(self) -> dict[str, float]:
        cleaned = {key: float(max(0.0, self.weights.get(key, 0.0))) for key in SIGNAL_KEYS}
        total = sum(cleaned.values())
        if total <= 0.0:
            return {key: 1.0 / float(len(SIGNAL_KEYS)) for key in SIGNAL_KEYS}
        return {key: cleaned[key] / total for key in SIGNAL_KEYS}

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": RECOMMENDATION_POLICY_SCHEMA_VERSION,
            "weights": {key: float(value) for key, value in sorted(self.weights.items())},
            "reason_limit": int(self.reason_limit),
        }


@dataclass(frozen=True, slots=True)
class EvalThresholds:
    ks: tuple[int, ...] = (10, 25, 50)
    precision_at_10_min: float = 0.60
    recall_at_25_min: float = 0.70

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": EVAL_THRESHOLDS_SCHEMA_VERSION,
            "ks": list(self.ks),
            "precision_at_10_min": float(self.precision_at_10_min),
            "recall_at_25_min": float(self.recall_at_25_min),
        }


@dataclass(frozen=True, slots=True)
class MatchReason:
    code: str
    value: float
    weight: float
    contribution: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "value": float(self.value),
            "weight": float(self.weight),
            "contribution": float(self.contribution),
        }


@dataclass(frozen=True, slots=True)
class MatchedJob:
    job_id: str
    score: float
    match_reasons: tuple[MatchReason, ...]
    required_missing_skills: tuple[str, ...]
    apply_url: str | None
    posted_at: str | None
    source: str | None
    quality_flags: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "score": float(self.score),
            "match_reasons": [item.to_dict() for item in self.match_reasons],
            "required_missing_skills": list(self.required_missing_skills),
            "apply_url": self.apply_url,
            "posted_at": self.posted_at,
            "source": self.source,
            "quality_flags": list(self.quality_flags),
        }


@dataclass(frozen=True, slots=True)
class ExcludedJob:
    job_id: str
    exclude_reasons: tuple[str, ...]
    apply_url: str | None
    posted_at: str | None
    source: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "exclude_reasons": list(self.exclude_reasons),
            "apply_url": self.apply_url,
            "posted_at": self.posted_at,
            "source": self.source,
        }


@dataclass(frozen=True, slots=True)
class RetrievalIndexResult:
    schema_version: str
    status: str
    index_id: str
    input_parquet: str
    index_dir: str
    manifest_file: str
    jobs_file: str
    facets_file: str
    quality_summary_file: str | None
    shard_dir: str
    policy_source: str
    policy_hash: str
    input_hash: str
    jobs_count: int
    token_count: int
    shard_count: int
    built_at_utc: str
    duration_ms: int

    def to_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "status": self.status,
            "index_id": self.index_id,
            "input_parquet": self.input_parquet,
            "index_dir": self.index_dir,
            "manifest_file": self.manifest_file,
            "jobs_file": self.jobs_file,
            "facets_file": self.facets_file,
            "quality_summary_file": self.quality_summary_file,
            "shard_dir": self.shard_dir,
            "policy_source": self.policy_source,
            "policy_hash": self.policy_hash,
            "input_hash": self.input_hash,
            "jobs_count": int(self.jobs_count),
            "token_count": int(self.token_count),
            "shard_count": int(self.shard_count),
            "built_at_utc": self.built_at_utc,
            "duration_ms": int(self.duration_ms),
            "check_codes": [],
        }


@dataclass(frozen=True, slots=True)
class MatchResult:
    schema_version: str
    status: str
    profile: CandidateProfile
    index_dir: str
    policy_source: str
    policy_hash: str
    eligible_count: int
    excluded_count: int
    total_jobs: int
    top_k: int
    results: tuple[MatchedJob, ...]
    excluded_jobs: tuple[ExcludedJob, ...] = ()
    check_codes: tuple[str, ...] = ()

    def to_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "status": self.status,
            "profile": self.profile.to_dict(),
            "index_dir": self.index_dir,
            "policy_source": self.policy_source,
            "policy_hash": self.policy_hash,
            "eligible_count": int(self.eligible_count),
            "excluded_count": int(self.excluded_count),
            "total_jobs": int(self.total_jobs),
            "top_k": int(self.top_k),
            "results": [item.to_dict() for item in self.results],
            "excluded_jobs": [item.to_dict() for item in self.excluded_jobs],
            "check_codes": list(self.check_codes),
        }


@dataclass(frozen=True, slots=True)
class RelevanceEvaluationResult:
    schema_version: str
    status: str
    index_dir: str
    thresholds_source: str
    thresholds_hash: str
    metrics: dict[str, float]
    thresholds: dict[str, float]
    cases_evaluated: int
    failing_checks: tuple[str, ...]
    check_codes: tuple[str, ...]

    def to_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "status": self.status,
            "index_dir": self.index_dir,
            "thresholds_source": self.thresholds_source,
            "thresholds_hash": self.thresholds_hash,
            "metrics": {key: float(value) for key, value in sorted(self.metrics.items())},
            "thresholds": {key: float(value) for key, value in sorted(self.thresholds.items())},
            "cases_evaluated": int(self.cases_evaluated),
            "failing_checks": list(self.failing_checks),
            "check_codes": list(self.check_codes),
        }


@dataclass(frozen=True, slots=True)
class FeedbackResult:
    schema_version: str
    status: str
    profile_id: str
    job_id: str
    event: FeedbackEventType
    duplicate: bool
    events_file: str
    weights_file: str
    total_events: int
    weights: dict[str, float]

    def to_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "status": self.status,
            "profile_id": self.profile_id,
            "job_id": self.job_id,
            "event": self.event,
            "duplicate": bool(self.duplicate),
            "events_file": self.events_file,
            "weights_file": self.weights_file,
            "total_events": int(self.total_events),
            "weights": {key: float(value) for key, value in sorted(self.weights.items())},
            "check_codes": [],
        }


@dataclass(frozen=True, slots=True)
class FeedbackSummary:
    schema_version: str
    status: str
    profile_id: str | None
    events_file: str
    total_events: int
    counts: dict[str, int]
    profile_counts: dict[str, dict[str, int]]
    weights: dict[str, float] | None

    def to_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "status": self.status,
            "profile_id": self.profile_id,
            "events_file": self.events_file,
            "total_events": int(self.total_events),
            "counts": {key: int(value) for key, value in sorted(self.counts.items())},
            "profile_counts": {
                profile: {key: int(value) for key, value in sorted(counter.items())}
                for profile, counter in sorted(self.profile_counts.items())
            },
            "weights": (
                {key: float(value) for key, value in sorted(self.weights.items())}
                if self.weights is not None
                else None
            ),
            "check_codes": [],
        }


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()
