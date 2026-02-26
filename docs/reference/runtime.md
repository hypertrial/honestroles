# Runtime API

```python
from honestroles import HonestRolesRuntime

runtime = HonestRolesRuntime.from_configs(
    pipeline_config_path="pipeline.toml",
    plugin_manifest_path="plugins.toml",
)
result = runtime.run()
```

`result` contains:

- `dataframe`: final `polars.DataFrame`
- `diagnostics`: stage row counts and execution metadata
- `application_plan`: ranked next-step records
