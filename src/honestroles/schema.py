from __future__ import annotations

from typing import TypedDict

JOB_KEY = "job_key"
COMPANY = "company"
SOURCE = "source"
JOB_ID = "job_id"
TITLE = "title"
TEAM = "team"
LOCATION_RAW = "location_raw"
REMOTE_FLAG = "remote_flag"
EMPLOYMENT_TYPE = "employment_type"
POSTED_AT = "posted_at"
UPDATED_AT = "updated_at"
APPLY_URL = "apply_url"
DESCRIPTION_HTML = "description_html"
DESCRIPTION_TEXT = "description_text"
INGESTED_AT = "ingested_at"
CONTENT_HASH = "content_hash"
SALARY_MIN = "salary_min"
SALARY_MAX = "salary_max"
SALARY_CURRENCY = "salary_currency"
SALARY_INTERVAL = "salary_interval"
SALARY_ANNUAL_MIN = "salary_annual_min"
SALARY_ANNUAL_MAX = "salary_annual_max"
SALARY_CONFIDENCE = "salary_confidence"
SALARY_SOURCE = "salary_source"
CITY = "city"
COUNTRY = "country"
REGION = "region"
REMOTE_TYPE = "remote_type"
SKILLS = "skills"
LAST_SEEN = "last_seen"
SALARY_TEXT = "salary_text"
LANGUAGES = "languages"
BENEFITS = "benefits"
VISA_SPONSORSHIP = "visa_sponsorship"
TECH_STACK = "tech_stack"
QUALITY_SCORE = "quality_score"
RATING = "rating"

# Match/ranking derived columns (agent-facing outputs)
FIT_SCORE = "fit_score"
FIT_BREAKDOWN = "fit_breakdown"
MISSING_REQUIREMENTS = "missing_requirements"
WHY_MATCH = "why_match"
NEXT_ACTIONS = "next_actions"
REQUIRED_SKILLS_EXTRACTED = "required_skills_extracted"
PREFERRED_SKILLS_EXTRACTED = "preferred_skills_extracted"
EXPERIENCE_YEARS_MIN = "experience_years_min"
EXPERIENCE_YEARS_MAX = "experience_years_max"
ENTRY_LEVEL_LIKELY = "entry_level_likely"
VISA_SPONSORSHIP_SIGNAL = "visa_sponsorship_signal"
APPLICATION_FRICTION_SCORE = "application_friction_score"
ROLE_CLARITY_SCORE = "role_clarity_score"
SIGNAL_CONFIDENCE = "signal_confidence"
SIGNAL_SOURCE = "signal_source"
SIGNAL_REASON = "signal_reason"
WORK_AUTHORIZATION_REQUIRED = "work_authorization_required"
CITIZENSHIP_REQUIRED = "citizenship_required"
CLEARANCE_REQUIRED = "clearance_required"
ACTIVE_LIKELIHOOD = "active_likelihood"
ACTIVE_REASON = "active_reason"
RED_FLAGS = "red_flags"
MUST_ASK_RECRUITER = "must_ask_recruiter"
OFFER_RISK = "offer_risk"
APPLICATION_EFFORT_MINUTES = "application_effort_minutes"

REQUIRED_COLUMNS = {
    JOB_KEY,
    COMPANY,
    SOURCE,
    JOB_ID,
    TITLE,
    LOCATION_RAW,
    APPLY_URL,
    INGESTED_AT,
    CONTENT_HASH,
}

ALL_COLUMNS = [
    JOB_KEY,
    COMPANY,
    SOURCE,
    JOB_ID,
    TITLE,
    TEAM,
    LOCATION_RAW,
    REMOTE_FLAG,
    EMPLOYMENT_TYPE,
    POSTED_AT,
    UPDATED_AT,
    APPLY_URL,
    DESCRIPTION_HTML,
    DESCRIPTION_TEXT,
    INGESTED_AT,
    CONTENT_HASH,
    SALARY_MIN,
    SALARY_MAX,
    SALARY_CURRENCY,
    SALARY_INTERVAL,
    SALARY_ANNUAL_MIN,
    SALARY_ANNUAL_MAX,
    SALARY_CONFIDENCE,
    SALARY_SOURCE,
    CITY,
    COUNTRY,
    REGION,
    REMOTE_TYPE,
    SKILLS,
    LAST_SEEN,
    SALARY_TEXT,
    LANGUAGES,
    BENEFITS,
    VISA_SPONSORSHIP,
]


class JobsCurrentRow(TypedDict, total=False):
    job_key: str
    company: str
    source: str
    job_id: str
    title: str
    team: str | None
    location_raw: str
    remote_flag: bool | None
    employment_type: str | None
    posted_at: str | None
    updated_at: str | None
    apply_url: str
    description_html: str | None
    description_text: str | None
    ingested_at: str
    content_hash: str
    salary_min: float | None
    salary_max: float | None
    salary_currency: str | None
    salary_interval: str | None
    salary_annual_min: float | None
    salary_annual_max: float | None
    salary_confidence: float | None
    salary_source: str | None
    city: str | None
    country: str | None
    region: str | None
    remote_type: str | None
    skills: list[str] | None
    last_seen: str | None
    salary_text: str | None
    languages: list[str] | None
    benefits: list[str] | None
    visa_sponsorship: bool | None
