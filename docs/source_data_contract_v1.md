# Source Data -> honestroles Data Contract (v1)

**Status:** Draft  
**Version:** `1.0.0`  
**Date:** `2026-02-14`  
**Producer:** `source data pipeline`  
**Consumer:** `honestroles`

## Purpose

Define the minimum and expected payload shape that `honestroles` receives from source data.

## Canonical Handoff Artifact

The contract artifact is **`jobs_current` source data** from the primary datastore (or a Parquet/JSON export generated from that same row shape).

`honestroles` should not treat reporting CSV exports as canonical contract input.

## Core Required Fields (must exist, non-null)

These are the columns `honestroles` validates by default.

| Field | Type | Rules |
| :--- | :--- | :--- |
| `job_key` | string | Primary id, format `company::source::job_id` |
| `company` | string | Stable company identifier |
| `source` | string | ATS source (`greenhouse`, `lever`, `ashby`, `workable`, `smartrecruiters`, `recruitee`, `teamtailor`, `personio`) |
| `job_id` | string | ATS-native job id |
| `title` | string | Job title |
| `location_raw` | string | Raw location text from source |
| `apply_url` | string | Apply URL |
| `ingested_at` | timestamp/string | Ingestion timestamp |
| `content_hash` | string | Stable content fingerprint for change tracking |

## Standard Optional Fields (known by `honestroles`)

If present, these should use the listed types:

- `team`: string or null
- `remote_flag`: boolean or null
- `employment_type`: string or null
- `posted_at`: timestamp/string or null
- `updated_at`: timestamp/string or null
- `description_html`: string or null
- `description_text`: string or null
- `salary_min`: number or null
- `salary_max`: number or null
- `salary_currency`: string or null
- `salary_interval`: string or null
- `city`: string or null
- `region`: string or null
- `country`: string or null
- `remote_type`: string or null
- `skills`: array[string] or null
- `last_seen`: timestamp/string or null
- `salary_text`: string or null
- `languages`: array[string] or null
- `benefits`: array[string] or null
- `visa_sponsorship`: boolean or null

## Extended Fields (source data may send; library should tolerate)

`honestroles` should accept and pass through unknown columns without failing validation. Current known extras from source data include:

- `application_deadline`
- `apply_email`
- `apply_url_canonical`
- `bonus_text`
- `contract_duration`
- `department`
- `education_level`
- `employment_status`
- `equity_text`
- `experience_years_max`
- `experience_years_min`
- `is_internship`
- `job_code`
- `job_function`
- `keywords`
- `latitude`
- `longitude`
- `postal_code`
- `raw_data`
- `remote_allowed`
- `remote_scope`
- `requisition_id`
- `salary_type`
- `salary_unit`
- `seniority`
- `state`
- `timezone`
- `work_arrangement`

## Serialization Rules

- Timestamps:
  - DuckDB: `TIMESTAMP`
  - JSON/Parquet: ISO-8601-compatible string or native datetime type
- Arrays (`skills`, `languages`, `benefits`, `keywords`): preserve as arrays, not comma-delimited strings
- Nulls: use `NULL`/`None`/`null`, not empty string where possible
- Encoding: UTF-8

## Validation Behavior in `honestroles`

`validate_source_data_contract(...)` enforces:

- required columns exist
- required columns are non-null (default)
- known format/type checks (default), including:
  - parseable timestamps
  - valid `apply_url` (`http`/`https`)
  - array columns as array-of-string values
  - boolean columns (`remote_flag`, `visa_sponsorship`) as booleans
  - salary metadata shape (`salary_currency`, `salary_interval`, and `salary_min <= salary_max`)

Format checks can be disabled with `enforce_formats=False` for ingestion transitions.

## Compatibility Rules

- Producer (source data) may add columns without breaking v1.
- Consumer (`honestroles`) must:
  - fail only when required core fields are missing
  - ignore unknown extra columns
- Removing or renaming any core required field is a breaking change and requires `v2`.

## Recommended Producer Query

If exporting contract data from source data storage:

```sql
SELECT *
FROM jobs_current;
```

If a strict minimal contract payload is needed:

```sql
SELECT
  job_key,
  company,
  source,
  job_id,
  title,
  location_raw,
  apply_url,
  ingested_at,
  content_hash
FROM jobs_current;
```

## Non-Contract Output

Reporting CSV exports are **not** contract-safe for `honestroles` when they omit required core fields (`job_key`, `source`, `job_id`, `ingested_at`, `content_hash`).
