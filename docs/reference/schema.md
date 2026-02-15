## Schema

`honestroles.schema` centralizes column-name constants and a TypedDict for job rows.
Use these symbols to keep downstream pipelines consistent.

### Modules

- `schema.py`: Column constants, required/known column lists, and `JobsCurrentRow`.

### Public API reference

#### Column name constants

These constants are strings used as DataFrame column names:

- `JOB_KEY`, `COMPANY`, `SOURCE`, `JOB_ID`
- `TITLE`, `TEAM`
- `LOCATION_RAW`, `REMOTE_FLAG`, `REMOTE_TYPE`
- `EMPLOYMENT_TYPE`, `POSTED_AT`, `UPDATED_AT`
- `APPLY_URL`
- `DESCRIPTION_HTML`, `DESCRIPTION_TEXT`
- `INGESTED_AT`, `CONTENT_HASH`, `LAST_SEEN`
- `SALARY_TEXT`, `SALARY_MIN`, `SALARY_MAX`, `SALARY_CURRENCY`, `SALARY_INTERVAL`
- `CITY`, `REGION`, `COUNTRY`
- `SKILLS`, `LANGUAGES`, `BENEFITS`, `VISA_SPONSORSHIP`

#### `REQUIRED_COLUMNS`

`REQUIRED_COLUMNS: set[str]` is the minimal column set required by I/O validation:

- `JOB_KEY`, `COMPANY`, `SOURCE`, `JOB_ID`, `TITLE`, `LOCATION_RAW`,
  `APPLY_URL`, `INGESTED_AT`, `CONTENT_HASH`

#### `ALL_COLUMNS`

`ALL_COLUMNS: list[str]` is a stable list of all known columns, in canonical order.

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
