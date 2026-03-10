# CLI Reference

Command reference for `honestroles`.

## Top-Level Commands

```bash
$ honestroles --help
```

Available commands:

- `run`
- `ingest sync`
- `ingest validate`
- `ingest sync-all`
- `plugins validate`
- `config validate`
- `report-quality`
- `init`
- `doctor`
- `reliability check`
- `adapter infer`
- `runs list`
- `runs show`
- `scaffold-plugin`
- `eda generate`
- `eda diff`
- `eda gate`
- `eda dashboard`

## Output Formats

Structured-output commands default to JSON and accept `--format {json,table}`.

- `json`: stable machine-readable payloads (default).
- `table`: concise human-readable summaries for terminals and CI logs.

`honestroles eda dashboard` launches Streamlit and does not use payload formatting.

## Command Matrix

| Command | Required flags | Description | Output |
| --- | --- | --- | --- |
| `honestroles run` | `--pipeline-config`, optional `--plugins` | Runs runtime pipeline | JSON/table diagnostics |
| `honestroles plugins validate` | `--manifest` | Validates and loads plugin manifest | JSON/table plugin listing |
| `honestroles config validate` | `--pipeline` | Validates pipeline config | JSON/table normalized config |
| `honestroles report-quality` | `--pipeline-config`, optional `--plugins` | Runs runtime and computes quality report | JSON/table quality summary |
| `honestroles ingest sync` | `--source`, `--source-ref`, optional `--output-parquet`, `--report-file`, `--state-file`, `--write-raw`, `--max-pages`, `--max-jobs`, `--full-refresh`, `--timeout-seconds`, `--max-retries`, `--base-backoff-seconds`, `--user-agent`, `--quality-policy`, `--strict-quality`, `--merge-policy`, `--retain-snapshots`, `--prune-inactive-days` | Fetches one public ATS source and writes latest parquet + snapshot/report artifacts | JSON/table sync summary |
| `honestroles ingest validate` | `--source`, `--source-ref`, optional `--report-file`, `--write-raw`, `--max-pages`, `--max-jobs`, `--timeout-seconds`, `--max-retries`, `--base-backoff-seconds`, `--user-agent`, `--quality-policy`, `--strict-quality` | Fetches + normalizes + evaluates ingestion quality without overwriting latest parquet | JSON/table validation summary |
| `honestroles ingest sync-all` | `--manifest`, optional `--report-file`, `--fail-fast` | Runs multi-source ingestion from `ingest.toml` in manifest order | JSON/table batch summary |
| `honestroles init` | `--input-parquet`, optional `--pipeline-config`, `--plugins-manifest`, `--output-parquet`, `--sample-rows`, `--force` | Scaffolds pipeline config + plugin manifest from sample data | JSON/table scaffold summary |
| `honestroles doctor` | `--pipeline-config`, optional `--plugins`, `--sample-rows`, `--policy`, `--strict` | Validates environment, config, schema readiness, output path, and reliability policy thresholds | JSON/table checks + summary |
| `honestroles reliability check` | `--pipeline-config`, optional `--plugins`, `--sample-rows`, `--policy`, `--output-file`, `--strict` | Runs policy-aware reliability checks and writes gate artifact | JSON/table checks + summary + artifact |
| `honestroles adapter infer` | `--input-parquet`, optional `--output-file`, `--sample-rows`, `--top-candidates`, `--min-confidence`, optional `--print` | Infers draft `[input.adapter]` mapping/coercion config from parquet input | JSON/table artifact summary |
| `honestroles runs list` | optional `--limit`, `--status`, `--command`, `--since`, `--contains-code` | Lists recorded run lineage entries with filters | JSON/table run rows |
| `honestroles runs show` | `--run-id` | Shows one recorded run lineage payload | JSON/table run record |
| `honestroles scaffold-plugin` | `--name`, optional `--output-dir` | Copies bundled plugin template | JSON/table scaffold path + package name |
| `honestroles eda generate` | `--input-parquet`, optional `--output-dir`, `--quality-profile`, repeated `--quality-weight`, `--top-k`, `--max-rows`, optional `--rules-file` | Builds deterministic profile artifacts (`summary.json`, tables, figures, report) | JSON/table artifact summary |
| `honestroles eda diff` | `--baseline-dir`, `--candidate-dir`, optional `--output-dir`, optional `--rules-file` | Compares two profile artifact dirs and writes diff artifacts (`diff.json`, drift tables) | JSON/table diff summary |
| `honestroles eda gate` | `--candidate-dir`, optional `--baseline-dir`, optional `--rules-file`, optional `--fail-on`, optional `--warn-on` | Evaluates gate policy and drift thresholds for CI | JSON/table gate summary + exit status |
| `honestroles eda dashboard` | `--artifacts-dir`, optional `--diff-dir`, optional `--host`, `--port` | Launches Streamlit artifact viewer | Process exit code |

## `ingest sync`, `ingest validate`, and `ingest sync-all`

`--source-ref` values:

- `greenhouse`: board token
- `lever`: site/company handle
- `ashby`: job board name
- `workable`: subdomain

Default per-source output locations:

- latest parquet: `dist/ingest/<source>/<source_ref>/jobs.parquet`
- source report: `dist/ingest/<source>/<source_ref>/sync_report.json`
- state file: `.honestroles/ingest/state.json`
- snapshots directory: `dist/ingest/<source>/<source_ref>/snapshots/`
- catalog parquet: `dist/ingest/<source>/<source_ref>/catalog.parquet`
- optional raw payload: `dist/ingest/<source>/<source_ref>/raw.jsonl` with `--write-raw`

Default batch report location:

- `dist/ingest/sync_all_report.json` when `--report-file` is omitted.

`ingest sync` report payload fields include:

- `schema_version`
- `status`
- `source`, `source_ref`
- `request_count`, `fetched_count`, `normalized_count`, `dedup_dropped`
- `new_count`, `updated_count`, `unchanged_count`
- `skipped_by_state`, `tombstoned_count`, `coverage_complete`
- `retry_count`, `http_status_counts`
- `quality_status`, `quality_summary`, `quality_check_codes`
- `stage_timings_ms`, `warnings`
- `merge_policy`, `retained_snapshot_count`, `pruned_snapshot_count`, `pruned_inactive_count`
- `quality_policy_source`, `quality_policy_hash`
- `high_watermark_before`, `high_watermark_after`
- `output_paths` (latest parquet, report, snapshot parquet, catalog parquet, state file, optional raw)
- optional `error` (`type`, `message`) on failures

`ingest validate` payload fields include:

- `schema_version`, `status`
- `source`, `source_ref`
- `request_count`, `fetched_count`, `normalized_count`, `dedup_dropped`
- `quality_status`, `quality_summary`, `quality_check_codes`
- `rows_evaluated`
- `stage_timings_ms`, `warnings`
- `output_paths` (validation report, optional raw JSONL)
- optional `error` (`type`, `message`) on failures

`ingest sync-all` batch payload fields include:

- `schema_version`, `status`
- `started_at_utc`, `finished_at_utc`, `duration_ms`
- `total_sources`, `pass_count`, `fail_count`
- `total_rows_written`, `total_fetched_count`, `total_request_count`
- `quality_summary`
- `stage_timings_ms`
- `sources` (one entry per attempted source)
- `report_file`
- `check_codes` (aggregate warn codes)

For full manifest schema details, see [Ingest Manifest Schema](./ingest-manifest-schema.md).
For quality policy schema details, see [Ingest Quality Policy Schema](./ingest-quality-policy-schema.md).

## Run Lineage

Tracked commands write a run record to:

```text
.honestroles/runs/<run_id>/run.json
```

Tracked commands:

- `run`
- `report-quality`
- `adapter infer`
- `eda generate`
- `eda diff`
- `eda gate`
- `reliability check`
- `ingest sync`
- `ingest validate`
- `ingest sync-all`

Run schema fields include:

- `schema_version`
- `run_id`
- `command`
- `status`
- `started_at_utc`, `finished_at_utc`, `duration_ms`
- `input_hash`, `input_hashes`
- `config_hash`
- `artifact_paths`
- `check_codes`
- `ingest_metrics` (for ingest commands)
- `error` (present on failures)

## Exit Codes

| Exit code | Meaning |
| --- | --- |
| `0` | Success. Includes `doctor`/`reliability check` statuses `pass` and `warn` when `--strict` is not set. |
| `1` | Generic `HonestRolesError`, failed `eda gate` policy, `doctor`/`reliability check` status `fail`, strict escalation of warn, or failed ingestion batch/source. |
| `2` | `ConfigValidationError` (invalid args, invalid/missing config, bad run lookup, manifest/state parse errors, etc.). |
| `3` | Plugin load/validation/execution failure. |
| `4` | `StageExecutionError`. |

## Examples

```bash
$ honestroles init --input-parquet data/jobs.parquet --pipeline-config pipeline.toml --plugins-manifest plugins.toml
$ honestroles ingest sync --source greenhouse --source-ref stripe --quality-policy ingest_quality.toml --strict-quality --merge-policy updated_hash --retain-snapshots 30 --prune-inactive-days 90 --timeout-seconds 20 --max-retries 4 --base-backoff-seconds 0.5 --user-agent "honestroles-batch/1.0" --format table
$ honestroles ingest validate --source greenhouse --source-ref stripe --quality-policy ingest_quality.toml --strict-quality --format table
$ honestroles ingest sync-all --manifest ingest.toml --format table
$ honestroles doctor --pipeline-config pipeline.toml --plugins plugins.toml --policy reliability.toml --format table
$ honestroles reliability check --pipeline-config pipeline.toml --plugins plugins.toml --strict --output-file dist/reliability/latest/gate_result.json --format table
$ honestroles run --pipeline-config pipeline.toml --plugins plugins.toml --format table
$ honestroles adapter infer --input-parquet data/jobs.parquet --output-file dist/adapters/adapter-draft.toml
$ honestroles runs list --limit 10 --command ingest.sync-all --contains-code INGEST_TRUNCATED --format table
$ honestroles runs show --run-id <run_id>
$ honestroles eda generate --input-parquet data/jobs.parquet --output-dir dist/eda/latest
$ honestroles eda diff --baseline-dir dist/eda/baseline --candidate-dir dist/eda/candidate --output-dir dist/eda/diff
$ honestroles eda gate --candidate-dir dist/eda/candidate --baseline-dir dist/eda/baseline --rules-file eda-rules.toml
$ honestroles eda dashboard --artifacts-dir dist/eda/candidate --diff-dir dist/eda/diff
```
