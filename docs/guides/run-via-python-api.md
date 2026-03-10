# Run via Python API

Use the runtime directly when you want Python-level control around execution.

## When to use

Use this for services, orchestration tasks, and integration tests.

## Prerequisites

- HonestRoles installed
- Pipeline spec and optional plugin manifest files

## Steps

```python
from honestroles import HonestRolesRuntime, sync_source, sync_sources_from_manifest

ingest = sync_source(
    source="greenhouse",
    source_ref="stripe",
    timeout_seconds=20,
    max_retries=5,
)
print(ingest.rows_written, ingest.output_parquet)

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
