# Plugin Author Guide

This guide covers how to build and ship external plugins for `honestroles`.

## 1. Start from Template

Copy the scaffold:

- `plugin_template/`

It includes:

- plugin package layout
- entrypoint config
- contract tests
- registration hooks

You can scaffold directly from this repository:

```bash
honestroles-scaffold-plugin --name honestroles-plugin-myorg --output-dir .
```

## 2. Implement Plugin Functions

Each function receives a DataFrame and returns a validated shape:

- filter plugin: `pd.Series` mask
- label plugin: `pd.DataFrame`
- rate plugin: `pd.DataFrame`

Keep plugins deterministic and side-effect free.

## 3. Register with Metadata

```python
from honestroles.plugins import PluginSpec, register_filter_plugin

register_filter_plugin(
    "only_usa",
    only_usa,
    spec=PluginSpec(
        api_version="1.0",
        plugin_version="0.1.0",
        capabilities=("filter", "geo"),
    ),
)
```

## 4. Choose Loading Strategy

### Option A: Explicit registration in user code

Call plugin `register_plugins()` at app startup.

### Option B: Python entrypoints

Declare entrypoints under one of:

- `honestroles.filter_plugins`
- `honestroles.label_plugins`
- `honestroles.rate_plugins`

Then call `load_plugins_from_entrypoints()`.

## 5. Testing Requirements

Recommended minimum test set:

1. plugin output type contract
2. deterministic behavior on fixed input
3. edge handling for nulls/empty rows
4. metadata compatibility (`api_version`)

## 6. Publishing Checklist

1. Include `PluginSpec` in registrations.
2. Add README with usage examples.
3. Add changelog and semantic version tags.
4. Verify compatibility against latest `honestroles`.
5. Submit to `plugins-index/plugins.toml` for discovery.
