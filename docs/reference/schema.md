## Schema

`honestroles.schema` centralizes column-name constants and a TypedDict for job rows.
Use these symbols to keep downstream pipelines consistent.

### Modules

- `schema.py`: Column constants, required/known column lists, and `JobsCurrentRow`.

### Public API reference

#### Column name constants

These constants are strings used as DataFrame column names:

- Core/source + cleaned fields:
  - `JOB_KEY`, `COMPANY`, `SOURCE`, `JOB_ID`
  - `TITLE`, `TEAM`
  - `LOCATION_RAW`, `CITY`, `REGION`, `COUNTRY`, `REMOTE_FLAG`, `REMOTE_TYPE`
  - `EMPLOYMENT_TYPE`, `POSTED_AT`, `UPDATED_AT`, `LAST_SEEN`
  - `APPLY_URL`
  - `DESCRIPTION_HTML`, `DESCRIPTION_TEXT`
  - `INGESTED_AT`, `CONTENT_HASH`
  - `SALARY_TEXT`, `SALARY_MIN`, `SALARY_MAX`, `SALARY_CURRENCY`, `SALARY_INTERVAL`
  - `SKILLS`, `LANGUAGES`, `BENEFITS`, `VISA_SPONSORSHIP`
- Label/rating fields:
  - `TECH_STACK`, `QUALITY_SCORE`, `RATING`
- Match/ranking signal/output fields:
  - `FIT_SCORE`, `FIT_BREAKDOWN`, `MISSING_REQUIREMENTS`, `WHY_MATCH`, `NEXT_ACTIONS`
  - `REQUIRED_SKILLS_EXTRACTED`, `PREFERRED_SKILLS_EXTRACTED`
  - `EXPERIENCE_YEARS_MIN`, `EXPERIENCE_YEARS_MAX`, `ENTRY_LEVEL_LIKELY`
  - `VISA_SPONSORSHIP_SIGNAL`, `APPLICATION_FRICTION_SCORE`, `ROLE_CLARITY_SCORE`
  - `SIGNAL_CONFIDENCE`, `SIGNAL_SOURCE`, `SIGNAL_REASON`

#### `REQUIRED_COLUMNS`

`REQUIRED_COLUMNS: set[str]` is the minimal column set required by I/O validation:

- `JOB_KEY`, `COMPANY`, `SOURCE`, `JOB_ID`, `TITLE`, `LOCATION_RAW`,
  `APPLY_URL`, `INGESTED_AT`, `CONTENT_HASH`

#### `ALL_COLUMNS`

`ALL_COLUMNS: list[str]` is the canonical base-schema column order used for
core/source + cleaned fields. Match/ranking outputs are defined as constants
but are not included in `ALL_COLUMNS`.

#### `JobsCurrentRow`

`JobsCurrentRow` is a `TypedDict` describing the `jobs_current` row shape.
All fields are optional (`total=False`), but values are typed to match the schema.

### Usage examples

```python
from honestroles import schema

required = schema.REQUIRED_COLUMNS
all_columns = schema.ALL_COLUMNS

print(schema.TITLE)
print(schema.DESCRIPTION_TEXT)
```

```python
from honestroles.schema import JobsCurrentRow

row: JobsCurrentRow = {
    "job_key": "acme::greenhouse::123",
    "company": "acme",
    "source": "greenhouse",
    "job_id": "123",
    "title": "Senior Software Engineer",
    "location_raw": "New York, NY",
    "apply_url": "https://example.com/apply",
    "ingested_at": "2025-01-01T00:00:00Z",
    "content_hash": "abc123",
}
```

### Design notes

- Use constants for column names to avoid typos and to simplify refactors.
- Validation in `honestroles.io.validate_dataframe` defaults to `REQUIRED_COLUMNS`.
