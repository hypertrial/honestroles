# Run via Python API

Use the runtime directly when you want Python-level control around execution.

## When to use

Use this for services, orchestration tasks, and integration tests.

## Prerequisites

- HonestRoles installed
- Pipeline spec and optional plugin manifest files

## Steps

```python
from honestroles import (
    HonestRolesRuntime,
    sync_source,
    sync_sources_from_manifest,
    validate_ingestion_source,
)

ingest = sync_source(
    source="greenhouse",
    source_ref="stripe",
    timeout_seconds=20,
    max_retries=5,
    quality_policy_file="ingest_quality.toml",
    strict_quality=False,
    merge_policy="updated_hash",
    retain_snapshots=30,
    prune_inactive_days=90,
)
print(ingest.rows_written, ingest.output_parquet)

validation = validate_ingestion_source(
    source="greenhouse",
    source_ref="stripe",
    quality_policy_file="ingest_quality.toml",
    strict_quality=True,
)
print(validation.report.status, validation.rows_evaluated)

batch = sync_sources_from_manifest(manifest_path="ingest.toml")
print(batch.status, batch.total_sources, batch.fail_count)

runtime = HonestRolesRuntime.from_configs(
    pipeline_config_path="pipeline.toml",
    plugin_manifest_path="plugins.toml",
)
run = runtime.run()

print(run.dataset.row_count())
print(run.dataset.to_polars().shape)
print(run.dataset.materialize_records(limit=1)[0].to_dict())
print(run.diagnostics.to_dict())
print(run.application_plan[:3])
```

## Expected result

You receive a `PipelineRun` object with:

- `dataset`: final `JobDataset`
- `diagnostics`: typed `RuntimeDiagnostics`
- `application_plan`: typed ranked next-step entries from `match`

## Next steps

- Runtime contract details: [Runtime API](../reference/runtime-api.md)
- Output interpretation: [Understand Output](./understand-output.md)
