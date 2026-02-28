# Run via Python API

Use the runtime directly when you want Python-level control around execution.

## When to use

Use this for services, orchestration tasks, and integration tests.

## Prerequisites

- HonestRoles installed
- Pipeline spec and optional plugin manifest files

## Steps

```python
from honestroles import HonestRolesRuntime

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
