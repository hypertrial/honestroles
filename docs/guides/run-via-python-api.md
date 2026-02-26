# Run via Python API

Use the runtime directly when you want Python-level control around execution.

## When to use

Use this for notebooks, services, orchestration tasks, and integration tests.

## Prerequisites

- HonestRoles installed
- Pipeline config and optional plugin manifest files

## Steps

```python
from honestroles import HonestRolesRuntime

runtime = HonestRolesRuntime.from_configs(
    pipeline_config_path="pipeline.toml",
    plugin_manifest_path="plugins.toml",
)
result = runtime.run()

print(result.dataframe.shape)
print(result.diagnostics)
print(result.application_plan[:3])
```

## Expected result

You receive a `RuntimeResult` object with:

- `dataframe`: final `polars.DataFrame`
- `diagnostics`: stage and runtime metadata
- `application_plan`: ranked next-step records from `match`

## Next steps

- Runtime return contract: [Runtime API](../reference/runtime-api.md)
- Stage outputs and invariants: [Stage Contracts](../reference/stage-contracts.md)
