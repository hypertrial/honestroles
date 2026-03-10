# Ingest from Public ATS APIs

Use `honestroles ingest sync` to fetch public postings from supported ATS APIs and write canonical parquet input for normal HonestRoles runs.

## When to use

Use this when you do not already have parquet input and want ToS-safe ingestion from public job-board endpoints.

## Prerequisites

- HonestRoles installed
- Internet access to supported public ATS endpoints
- Valid `--source-ref` for the chosen source

## Steps

1. Run a sync command:

```bash
$ honestroles ingest sync --source <source> --source-ref <ref>
```

Supported `--source` values:

- `greenhouse`
- `lever`
- `ashby`
- `workable`

2. Use one of these source-specific examples:

Greenhouse:

```bash
$ honestroles ingest sync --source greenhouse --source-ref stripe --format table
```

Lever:

```bash
$ honestroles ingest sync --source lever --source-ref netflix --format table
```

Ashby public postings:

```bash
$ honestroles ingest sync --source ashby --source-ref notion --format table
```

Workable public endpoints:

```bash
$ honestroles ingest sync --source workable --source-ref workable --format table
```

3. Optional flags for operational control:

Limit request scope:

```bash
$ honestroles ingest sync --source lever --source-ref netflix --max-pages 5 --max-jobs 800
```

Force full refresh (ignore watermark/recent IDs):

```bash
$ honestroles ingest sync --source greenhouse --source-ref stripe --full-refresh
```

Write raw payload for debugging:

```bash
$ honestroles ingest sync --source ashby --source-ref notion --write-raw
```

4. Point your pipeline input at the generated parquet:

```toml
[input]
kind = "parquet"
path = "dist/ingest/greenhouse/stripe/jobs.parquet"
```

5. Run the pipeline normally:

```bash
$ honestroles run --pipeline-config pipeline.toml
```

## Expected result

Each sync writes these artifacts by default:

- canonical parquet: `dist/ingest/<source>/<source_ref>/jobs.parquet`
- sync report: `dist/ingest/<source>/<source_ref>/sync_report.json`
- state file: `.honestroles/ingest/state.json`

Optional artifact:

- raw payload JSONL: `dist/ingest/<source>/<source_ref>/raw.jsonl` with `--write-raw`

Default sync behavior is incremental:

- previously seen `source_job_id` values are skipped
- records older than stored `high_watermark_posted_at` are skipped
- dedup uses deterministic key precedence

`sync_report.json` includes:

- `schema_version`, `status`, `source`, `source_ref`
- `started_at_utc`, `finished_at_utc`, `duration_ms`
- `request_count`, `fetched_count`, `normalized_count`, `dedup_dropped`
- `high_watermark_before`, `high_watermark_after`
- `output_paths` (`parquet`, `report`, optional `raw_jsonl`)
- optional `error` (`type`, `message`) on failures

Canonical parquet keeps core fields and adds ingestion metadata columns:

- `source`
- `source_ref`
- `source_job_id`
- `job_url`
- `ingested_at_utc`
- `source_payload_hash`

## Next steps

- Validate your source identifier: [Ingest Source-Ref Glossary](../reference/ingest-source-ref-glossary.md)
- Review all flags and output schema: [CLI Reference](../reference/cli.md)
- Troubleshoot empty results or rate limiting: [Common Errors](../troubleshooting/common-errors.md)
- Run the dedicated live smoke flow before release: [Release and PyPI](../for-maintainers/release-and-pypi.md)
