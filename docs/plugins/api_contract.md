# Plugin API Contract

This document defines the stable extension contract for `honestroles` plugins.

## Supported API Version

- Current plugin API version: `1.0`
- Exposed constant: `honestroles.plugins.SUPPORTED_PLUGIN_API_VERSION`

Plugins may declare `api_version` in `PluginSpec`. Registration fails if the major version does not match or if a plugin targets a newer minor version than the core package supports.

## Plugin Kinds

- `filter`: function returns `pandas.Series` boolean mask.
- `label`: function returns `pandas.DataFrame`.
- `rate`: function returns `pandas.DataFrame`.

## Callable Signatures

All plugin callables are kwargs-extensible:

```python
def filter_plugin(df: pd.DataFrame, **kwargs: object) -> pd.Series: ...
def label_plugin(df: pd.DataFrame, **kwargs: object) -> pd.DataFrame: ...
def rate_plugin(df: pd.DataFrame, **kwargs: object) -> pd.DataFrame: ...
```

## Metadata (`PluginSpec`)

Use `PluginSpec` to declare metadata:

- `api_version`: plugin API target (default `1.0`)
- `plugin_version`: plugin package version string
- `capabilities`: tuple of short capability tags

Example:

```python
from honestroles.plugins import PluginSpec, register_filter_plugin

register_filter_plugin(
    "only_remote",
    only_remote,
    spec=PluginSpec(
        api_version="1.0",
        plugin_version="0.3.2",
        capabilities=("filter", "location"),
    ),
)
```

## Registration API

- `register_filter_plugin`
- `register_label_plugin`
- `register_rate_plugin`

Each registration function accepts:

- `name: str`
- plugin callable
- `overwrite: bool = False`
- `spec: PluginSpec | Mapping[str, object] | None = None`

## Runtime Guarantees

- Registry operations validate plugin names.
- Duplicate registrations fail unless `overwrite=True`.
- Runtime type checks validate plugin outputs.
- Unknown plugin names raise `KeyError`.
- Empty plugin lists are no-op.

## Discovery and Loading

### Entrypoints

`load_plugins_from_entrypoints()` discovers plugins from:

- `honestroles.filter_plugins`
- `honestroles.label_plugins`
- `honestroles.rate_plugins`

Entrypoint target may be:

- a callable plugin function
- `PluginExport` with explicit kind/name/spec

### Module Loader

`load_plugins_from_module(module_ref)` imports a module by dotted path or `.py` file path and invokes either:

- `register_plugins()`
- `register()`

The function returns newly registered plugin names by kind.
