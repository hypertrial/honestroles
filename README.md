# HonestRoles

HonestRoles is a Polars-first, config-driven runtime for deterministic job-data processing.

## What Changed

This repository now uses a hard-replace architecture:

- No process-global plugin registry
- Explicit `PluginRegistry` loaded from TOML manifests
- Explicit `HonestRolesRuntime` lifecycle
- Polars-only stage execution model
- Config-driven CLI (`honestroles`)

Backward compatibility with the old API is intentionally removed.

## Install

```bash
pip install honestroles
```

For development:

```bash
pip install -e ".[dev]"
```

## Runtime API

```python
from honestroles import HonestRolesRuntime

runtime = HonestRolesRuntime.from_configs(
    pipeline_config_path="pipeline.toml",
    plugin_manifest_path="plugins.toml",
)
result = runtime.run()

print(result.diagnostics)
print(result.dataframe.head())
```

## CLI

```bash
honestroles run --pipeline-config pipeline.toml --plugins plugins.toml
honestroles plugins validate --manifest plugins.toml
honestroles config validate --pipeline pipeline.toml
honestroles report-quality --pipeline-config pipeline.toml
honestroles scaffold-plugin --name my-plugin --output-dir .
```

## Config Examples

### `pipeline.toml`

```toml
[input]
kind = "parquet"
path = "./jobs.parquet"

[output]
path = "./jobs_scored.parquet"

[stages.clean]
enabled = true

[stages.filter]
enabled = true
remote_only = true
required_keywords = ["python"]

[stages.label]
enabled = true

[stages.rate]
enabled = true
completeness_weight = 0.5
quality_weight = 0.5

[stages.match]
enabled = true
top_k = 100

[runtime]
fail_fast = true
random_seed = 0
```

### `plugins.toml`

```toml
[[plugins]]
name = "label_note"
kind = "label"
callable = "my_package.plugins:label_note"
enabled = true
order = 10

[plugins.settings]
note = "from-plugin"
```

## Plugin ABI

- Filter: `(pl.DataFrame, FilterPluginContext) -> pl.DataFrame`
- Label: `(pl.DataFrame, LabelPluginContext) -> pl.DataFrame`
- Rate: `(pl.DataFrame, RatePluginContext) -> pl.DataFrame`

Plugin errors are fail-fast and wrapped with plugin/stage context.

## Testing

Default deterministic suite:

```bash
pytest -q
```

All fuzz tests:

```bash
pytest -m "fuzz" -q
```

Profiled fuzz runs:

```bash
HYPOTHESIS_PROFILE=ci_smoke pytest -m "fuzz" -q
HYPOTHESIS_PROFILE=nightly_deep pytest -m "fuzz" -q
```
