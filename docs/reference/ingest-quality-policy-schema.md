# Ingest Quality Policy Schema

Reference for `ingest_quality.toml` used by:

- `honestroles ingest sync --quality-policy ingest_quality.toml`
- `honestroles ingest validate --quality-policy ingest_quality.toml`
- `ingest.toml` defaults/source overrides via `quality_policy_file`

If `--quality-policy` (or manifest `quality_policy_file`) is omitted, HonestRoles
uses the built-in default quality policy.

## Schema

Top-level keys:

- `schema_version` (optional, default `"1.0"`)
- `min_rows` (optional integer, `>= 1`)
- `required_columns` (optional non-empty string array)
- `location_or_remote_signal_min` (optional number in `[0, 1]`, default `0.85`)
- `null_thresholds` (optional table, values in `[0, 1]`)
- `freshness` (optional table)

`freshness` keys:

- `posted_at_max_age_days` (optional integer, `>= 0`)
- `source_updated_at_max_age_days` (optional integer, `>= 0`)

## Full Example

```toml
schema_version = "1.0"
min_rows = 100
required_columns = [
  "id",
  "title",
  "apply_url",
  "posted_at",
  "source",
  "source_ref",
  "source_job_id",
  "source_payload_hash",
]
location_or_remote_signal_min = 0.85

[null_thresholds]
id = 0.00
title = 0.00
apply_url = 0.00
source_job_id = 0.00
company = 0.05
description_text = 0.10
posted_at = 0.10

[freshness]
posted_at_max_age_days = 365
source_updated_at_max_age_days = 730
```

## Check Codes

Quality checks emit stable codes:

- `INGEST_QUALITY_REQUIRED_COLUMNS`
- `INGEST_QUALITY_MIN_ROWS`
- `INGEST_QUALITY_NULL_RATE_ID`
- `INGEST_QUALITY_NULL_RATE_TITLE`
- `INGEST_QUALITY_NULL_RATE_APPLY_URL`
- `INGEST_QUALITY_NULL_RATE_SOURCE_JOB_ID`
- `INGEST_QUALITY_NULL_RATE_COMPANY`
- `INGEST_QUALITY_NULL_RATE_DESCRIPTION_TEXT`
- `INGEST_QUALITY_NULL_RATE_POSTED_AT`
- `INGEST_QUALITY_LOCATION_OR_REMOTE_SIGNAL`
- `INGEST_QUALITY_POSTED_AT_PARSEABLE`
- `INGEST_QUALITY_POSTED_AT_FRESHNESS`
- `INGEST_QUALITY_SOURCE_UPDATED_AT_PARSEABLE`
- `INGEST_QUALITY_SOURCE_UPDATED_AT_FRESHNESS`

Default behavior treats quality findings as warnings. `--strict-quality` upgrades
non-pass quality outcomes to command failure (`exit 1`) without changing emitted
check severities/codes.
