# HonestRoles Plugin Template

Template for plugins compatible with the explicit HonestRoles runtime ABI.

## ABI

Plugins are plain Python callables referenced by `module:function` in `plugins.toml`.

- Filter: `(pl.DataFrame, FilterPluginContext) -> pl.DataFrame`
- Label: `(pl.DataFrame, LabelPluginContext) -> pl.DataFrame`
- Rate: `(pl.DataFrame, RatePluginContext) -> pl.DataFrame`

## Example manifest

```toml
[[plugins]]
name = "example_label"
kind = "label"
callable = "honestroles_plugin_example.plugins:example_label"
enabled = true
order = 10
```
