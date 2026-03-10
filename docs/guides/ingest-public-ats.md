# Ingest from Public ATS APIs

Use HonestRoles ingestion commands to fetch public postings from supported ATS APIs
and write canonical parquet input for pipeline runs.

## When to use

Use this when you do not already have parquet input and want deterministic,
ToS-safe ingestion from official public ATS endpoints.

## Prerequisites

- HonestRoles installed.
- Internet access to supported ATS APIs.
- Valid `--source-ref` values for the sources you ingest.

Supported `--source` values:

- `greenhouse` (board token)
- `lever` (site/company handle)
- `ashby` (job board name)
- `workable` (subdomain)

## Steps

1. Run one source directly:

```bash
$ honestroles ingest sync --source greenhouse --source-ref stripe --format table
```

2. Optional: tune operational controls (`timeout`, retries, and backoff):

```bash
$ honestroles ingest sync \
  --source lever \
  --source-ref netflix \
  --max-pages 10 \
  --max-jobs 1000 \
  --timeout-seconds 20 \
  --max-retries 5 \
  --base-backoff-seconds 0.5 \
  --user-agent "honestroles-batch/1.0" \
  --format table
```

3. Optional: use batch ingestion with `ingest.toml`:

```toml
[defaults]
state_file = ".honestroles/ingest/state.json"
max_pages = 25
max_jobs = 5000

[[sources]]
source = "greenhouse"
source_ref = "stripe"

[[sources]]
source = "lever"
source_ref = "netflix"
max_pages = 10
```

```bash
$ honestroles ingest sync-all --manifest ingest.toml --format table
```

4. Optional: force a refresh or write raw payload:

```bash
$ honestroles ingest sync --source ashby --source-ref notion --full-refresh
$ honestroles ingest sync --source workable --source-ref workable --write-raw
```

5. Point your runtime pipeline at the latest parquet output:

```toml
[input]
kind = "parquet"
path = "dist/ingest/greenhouse/stripe/jobs.parquet"
```

6. Run the runtime pipeline:

```bash
$ honestroles run --pipeline-config pipeline.toml
```

## Expected result

Per source, ingestion writes:

- latest parquet: `dist/ingest/<source>/<source_ref>/jobs.parquet`
- per-run snapshot parquet: `dist/ingest/<source>/<source_ref>/snapshots/<stamp>-<run>.parquet`
- catalog parquet: `dist/ingest/<source>/<source_ref>/catalog.parquet`
- sync report: `dist/ingest/<source>/<source_ref>/sync_report.json`
- optional raw payload: `dist/ingest/<source>/<source_ref>/raw.jsonl`
- state: `.honestroles/ingest/state.json`

Batch runs also write:

- `dist/ingest/sync_all_report.json` (or custom `--report-file`)

Incremental semantics:

- Filtering uses posted+updated watermarks and recent IDs.
- `--full-refresh` bypasses incremental filtering.
- Tombstones are applied only on coverage-complete runs.
- Truncated runs (hitting `max-pages` or `max-jobs`) do not tombstone missing records.

## Next steps

- Full flags and payload fields: [CLI Reference](../reference/cli.md)
- Batch schema details: [Ingest Manifest Schema](../reference/ingest-manifest-schema.md)
- Source identifier lookup: [Ingest Source-Ref Glossary](../reference/ingest-source-ref-glossary.md)
- Failure handling and retry tuning: [Common Errors](../troubleshooting/common-errors.md)
