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
result = runtime.run()
```

## `RuntimeResult`

`run()` returns `RuntimeResult` with fields:

- `dataframe`: final `polars.DataFrame`
- `diagnostics`: `dict[str, Any]`
- `application_plan`: `list[dict[str, Any]]`

## Diagnostics Contract

Diagnostics always include:

- `input_path`
- `stage_rows`
- `plugin_counts`
- `runtime`
- `final_rows`

Diagnostics conditionally include:

- `output_path` (when `[output]` is configured)
- `non_fatal_errors` (when `fail_fast = false` and errors occur)

## Determinism

The runtime seeds Python randomness from `runtime.random_seed` at run start. Fixed inputs/config/plugins produce stable outputs.
