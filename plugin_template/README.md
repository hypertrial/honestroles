# HonestRoles Plugin Template

Template for plugins compatible with the explicit HonestRoles runtime ABI.

## ABI

Plugins are plain Python callables referenced by `module:function` in `plugins.toml`.

- Filter: `(JobDataset, FilterStageContext) -> JobDataset`
- Label: `(JobDataset, LabelStageContext) -> JobDataset`
- Rate: `(JobDataset, RateStageContext) -> JobDataset`

Returned datasets must preserve all canonical fields and canonical logical dtypes. Use
`dataset.transform(...)` for most plugin mutations and treat `dataset.to_polars(copy=True)` as an
explicit engine boundary.

## Example manifest

```toml
[[plugins]]
name = "example_label"
kind = "label"
callable = "honestroles_plugin_example.plugins:example_label"
enabled = true
order = 10
```
