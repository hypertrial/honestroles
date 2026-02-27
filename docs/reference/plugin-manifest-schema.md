# Plugin Manifest Schema

Field-level reference for `plugins.toml` and plugin ABI behavior.

## Root

The manifest contains repeated `[[plugins]]` entries.

## `[[plugins]]` fields

| Field | Required | Type | Default | Notes |
| --- | --- | --- | --- | --- |
| `name` | Yes | string | none | Must be non-empty after trim |
| `kind` | Yes | `filter|label|rate` | none | Determines required callable context type |
| `callable` | Yes | string | none | Format: `module:function` |
| `enabled` | No | bool | `true` | Disabled entries are skipped |
| `order` | No | int | `0` | Lower value runs first |
| `settings` | No | object | `{}` | Deep-frozen before plugin execution |
| `spec` | No | object | see below | Metadata for plugin identity/capabilities |

## `[plugins.spec]` fields

| Field | Type | Default |
| --- | --- | --- |
| `api_version` | string | `"1.0"` |
| `plugin_version` | string | `"0.1.0"` |
| `capabilities` | array of strings | `[]` |

## ABI Signatures

- Filter: `(JobDataset, FilterStageContext) -> JobDataset`
- Label: `(JobDataset, LabelStageContext) -> JobDataset`
- Rate: `(JobDataset, RateStageContext) -> JobDataset`

Plugin callables must use explicit type annotations and return `JobDataset`.

## Execution Ordering

Enabled plugins run in deterministic order by `(kind, order, name)`.

## Failure Semantics

- Import/reference issues: `PluginLoadError`
- Signature/annotation issues: `PluginValidationError`
- Runtime plugin exception or invalid return type: `PluginExecutionError`
