# Ingest Manifest Schema

Reference for `ingest.toml` used by:

- `honestroles ingest sync-all --manifest ingest.toml`
- `sync_sources_from_manifest(manifest_path="ingest.toml")`

## Schema

Top-level sections:

- `[defaults]` (optional)
- `[[sources]]` (required, at least one)

`[defaults]` keys:

- `state_file` (string path)
- `write_raw` (boolean)
- `max_pages` (integer, `>= 1`)
- `max_jobs` (integer, `>= 1`)
- `full_refresh` (boolean)
- `timeout_seconds` (number, `>= 0.1`)
- `max_retries` (integer, `>= 0`)
- `base_backoff_seconds` (number, `>= 0`)
- `user_agent` (non-empty string)

`[[sources]]` keys:

- `source` (required: `greenhouse|lever|ashby|workable`)
- `source_ref` (required, non-empty string)
- `enabled` (optional boolean, default `true`)
- `output_parquet` (optional string path)
- `report_file` (optional string path)
- `state_file` (optional string path)
- `write_raw` (optional boolean)
- `max_pages` (optional integer, `>= 1`)
- `max_jobs` (optional integer, `>= 1`)
- `full_refresh` (optional boolean)
- `timeout_seconds` (optional number, `>= 0.1`)
- `max_retries` (optional integer, `>= 0`)
- `base_backoff_seconds` (optional number, `>= 0`)
- `user_agent` (optional non-empty string)

Relative paths resolve against the manifest directory.

## Full Example

```toml
[defaults]
state_file = ".honestroles/ingest/state.json"
write_raw = false
max_pages = 25
max_jobs = 5000
full_refresh = false
timeout_seconds = 15.0
max_retries = 3
base_backoff_seconds = 0.25
user_agent = "honestroles-ingest/2.0"

[[sources]]
source = "greenhouse"
source_ref = "stripe"
enabled = true

[[sources]]
source = "lever"
source_ref = "netflix"
enabled = true
max_pages = 10
max_jobs = 1000
timeout_seconds = 20
max_retries = 5
base_backoff_seconds = 0.5
user_agent = "honestroles-batch/1.0"

[[sources]]
source = "ashby"
source_ref = "notion"
enabled = true
output_parquet = "dist/ingest/ashby/notion/jobs.parquet"
report_file = "dist/ingest/ashby/notion/sync_report.json"

[[sources]]
source = "workable"
source_ref = "workable"
enabled = false
```

## Batch Behavior Notes

- Sources execute in manifest order.
- Default mode runs all enabled sources and returns overall failure if any source fails.
- `--fail-fast` stops after the first failed source.
- Tombstones are only applied on coverage-complete runs (not truncated runs).
