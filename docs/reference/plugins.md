# Plugin Manifest and ABI

Each plugin is configured in `plugins.toml` via `[[plugins]]` entries.

Required fields:

- `name`
- `kind` (`filter`, `label`, `rate`)
- `callable` (`module:function`)

Callable signatures:

- Filter: `(pl.DataFrame, FilterPluginContext) -> pl.DataFrame`
- Label: `(pl.DataFrame, LabelPluginContext) -> pl.DataFrame`
- Rate: `(pl.DataFrame, RatePluginContext) -> pl.DataFrame`

Plugin failures are wrapped and raised as `PluginExecutionError`.
