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
CITY = "city"
COUNTRY = "country"
REMOTE_TYPE = "remote_type"
SKILLS = "skills"
LAST_SEEN = "last_seen"
SALARY_TEXT = "salary_text"
LANGUAGES = "languages"
BENEFITS = "benefits"
VISA_SPONSORSHIP = "visa_sponsorship"

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
    CITY,
    COUNTRY,
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
    city: str | None
    country: str | None
    remote_type: str | None
    skills: list[str] | None
    last_seen: str | None
    salary_text: str | None
    languages: list[str] | None
    benefits: list[str] | None
    visa_sponsorship: bool | None
