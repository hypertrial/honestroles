# Reliability Policy Schema

Reference for `reliability.toml` used by:

- `honestroles doctor --policy reliability.toml`
- `honestroles reliability check --policy reliability.toml`

If `--policy` is omitted, HonestRoles uses a built-in default policy.

## Schema (v1)

Top-level keys:

- `min_rows` (integer, `>= 1`)
- `required_columns` (array of non-empty strings)
- `max_null_pct` (table of `column -> number` in `[0, 100]`)
- `freshness` (table with `column` and `max_age_days`)

`freshness` fields:

- `column` (non-empty string)
- `max_age_days` (integer, `>= 0`)

## Example

```toml
min_rows = 500
required_columns = ["title", "description_text", "posted_at"]

[max_null_pct]
title = 5
description_text = 10
posted_at = 1

[freshness]
column = "posted_at"
max_age_days = 14
```

## Behavior Notes

- Policy parse/type errors raise `ConfigValidationError` (CLI exit code `2`).
- Policy threshold breaches are `warn` by default.
- `--strict` escalates aggregate `warn` status to exit code `1` without changing check severities.
- Reliability results include:
  - `policy_source`
  - `policy_hash`
  - `check_codes` (warn/fail codes in deterministic order)
