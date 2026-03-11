# Runtime API

Public runtime API contracts for Python usage.

## `HonestRolesRuntime`

Constructor entrypoint:

```python
from honestroles import HonestRolesRuntime

runtime = HonestRolesRuntime.from_configs(
    pipeline_config_path="pipeline.toml",
    plugin_manifest_path="plugins.toml",  # optional
)
```

- `pipeline_config_path`: `str | Path`, required
- `plugin_manifest_path`: `str | Path | None`, optional

Execution:

```python
run = runtime.run()
```

## `PipelineRun`

`run()` returns `PipelineRun` with fields:

- `dataset`: final `JobDataset`
- `diagnostics`: `RuntimeDiagnostics`
- `application_plan`: `tuple[ApplicationPlanEntry, ...]`

## `JobDataset`

`JobDataset` is the strict canonical runtime stage I/O object.

- `to_polars(copy: bool = True) -> pl.DataFrame`
- `row_count() -> int`
- `columns() -> tuple[str, ...]`
- `iter_records() -> Iterator[CanonicalJobRecord]`
- `materialize_records(limit: int | None = None) -> list[CanonicalJobRecord]`
- `validate() -> None`
- `with_frame(frame) -> JobDataset`
- `transform(fn) -> JobDataset`
- Runtime-produced and plugin-returned datasets must retain all canonical fields and canonical logical dtypes.

Notes:

- `to_polars(copy=True)` is the explicit engine boundary and returns a clone by default.
- `rows()` and `select()` are not part of the public `JobDataset` API.

## Diagnostics Contract

`RuntimeDiagnostics.to_dict()` always includes:

- `input_path`
- `stage_rows`
- `plugin_counts`
- `runtime`
- `input_adapter`
- `input_aliasing`
- `final_rows`

Diagnostics conditionally include:

- `output_path` (when `[output]` is configured)
- `non_fatal_errors` (when `fail_fast = false` and errors occur)

## Determinism

The runtime seeds Python randomness from `runtime.random_seed` at run start. Fixed inputs/spec/plugins produce stable outputs.

## Ingestion API

Use `sync_source(...)` to ingest one public ATS source into canonical parquet:

```python
from honestroles import sync_source

result = sync_source(
    source="greenhouse",
    source_ref="stripe",
)
```

`sync_source(...) -> IngestionResult` fields include:

- `report`: `IngestionReport`
- `output_parquet`: resolved latest parquet path
- `report_file`: resolved sync report path
- `raw_file`: optional raw JSONL path (when `write_raw=True`)
- `snapshot_file`: per-run snapshot parquet path
- `catalog_file`: catalog parquet path
- `state_file`: state file path written
- `rows_written`: active latest row count written

Additive request controls:

- `timeout_seconds`
- `max_retries`
- `base_backoff_seconds`
- `user_agent`
- `quality_policy_file`
- `strict_quality`
- `merge_policy` (`updated_hash|first_seen|last_seen`)
- `retain_snapshots`
- `prune_inactive_days`

Additive result/report fields include:

- `quality_status`, `quality_summary`, `quality_check_codes`
- `key_field_completeness` (`company_non_null_pct`, `posted_at_non_null_pct`, `description_text_non_null_pct`, `location_or_remote_signal_pct`)
- `stage_timings_ms`, `warnings`
- `merge_policy`, `retained_snapshot_count`, `pruned_snapshot_count`, `pruned_inactive_count`
- `quality_policy_source`, `quality_policy_hash`

Validation-only ingestion API:

```python
from honestroles import validate_ingestion_source

validation = validate_ingestion_source(
    source="greenhouse",
    source_ref="stripe",
    quality_policy_file="ingest_quality.toml",
    strict_quality=True,
)
print(validation.report.status, validation.rows_evaluated)
```

`validate_ingestion_source(...) -> IngestionValidationResult` writes a validation
report and optional raw payload, but does not overwrite latest parquet.

Batch ingestion from manifest:

```python
from honestroles import sync_sources_from_manifest

batch = sync_sources_from_manifest(manifest_path="ingest.toml", fail_fast=False)
print(batch.status, batch.total_sources, batch.fail_count)
```

`sync_sources_from_manifest(...) -> BatchIngestionResult` includes:

- aggregate status/timing fields
- per-source payloads under `sources`
- aggregate totals (`total_rows_written`, `total_fetched_count`, `total_request_count`)
- aggregate quality summary (`quality_summary`)
- aggregate key field completeness (`key_field_completeness`)
- `report_file`

Supported `source` values:

- `greenhouse`
- `lever`
- `ashby`
- `workable`

## Recommendation API

Build API-ready retrieval artifacts:

```python
from honestroles import build_retrieval_index

index = build_retrieval_index(
    input_parquet="dist/ingest/greenhouse/stripe/jobs.parquet",
    policy_file="recommendation.toml",
)
print(index.index_id, index.index_dir)
```

Match jobs from an index:

```python
from honestroles import match_jobs

matches = match_jobs(
    index_dir=index.index_dir,
    candidate_json="examples/candidate.json",
    top_k=25,
    include_excluded=True,
)
print(matches.eligible_count, len(matches.results))
```

Evaluate recommendation quality:

```python
from honestroles import evaluate_relevance

evaluation = evaluate_relevance(
    index_dir=index.index_dir,
    golden_set="examples/recommend_golden_set.json",
    thresholds_file="recommend_eval.toml",
)
print(evaluation.status, evaluation.metrics)
```

Feedback primitives:

```python
from honestroles import record_feedback_event, summarize_feedback

record_feedback_event(profile_id="jane_doe", job_id="12345", event="interviewed")
summary = summarize_feedback(profile_id="jane_doe")
print(summary.total_events, summary.weights)
```

## NeonDB Publish API

Apply migrations:

```python
from honestroles import migrate_neondb

result = migrate_neondb(
    database_url_env="NEON_DATABASE_URL",
    schema="honestroles_api",
)
print(result.status, result.migrations_applied)
```

Publish jobs + features:

```python
from honestroles import publish_neondb_sync

result = publish_neondb_sync(
    database_url_env="NEON_DATABASE_URL",
    schema="honestroles_api",
    jobs_parquet="dist/ingest/greenhouse/stripe/jobs.parquet",
    index_dir="dist/recommend/index/<index_id>",
    sync_report="dist/ingest/greenhouse/stripe/sync_report.json",
    require_quality_pass=True,
    full_refresh=False,
)
print(result.status, result.batch_id, result.inserted_count)
```

Verify DB contract:

```python
from honestroles import verify_neondb_contract

result = verify_neondb_contract(
    database_url_env="NEON_DATABASE_URL",
    schema="honestroles_api",
)
print(result.status, result.check_codes)
```
