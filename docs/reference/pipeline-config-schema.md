# Pipeline Config Schema

Field-level reference for `pipeline.toml`.

## Root

| Section | Required | Type | Notes |
| --- | --- | --- | --- |
| `[input]` | Yes | object | Input data source |
| `[output]` | No | object | Output parquet path |
| `[stages]` | No | object | Stage options; defaults applied |
| `[runtime]` | No | object | Runtime behavior |

## `[input]`

| Field | Type | Default | Constraints |
| --- | --- | --- | --- |
| `kind` | `"parquet"` | `"parquet"` | Only `parquet` allowed |
| `path` | path-like string | none | Required |

## `[output]`

| Field | Type | Default | Constraints |
| --- | --- | --- | --- |
| `path` | path-like string | none | Optional section |

## `[stages.clean]`

| Field | Type | Default |
| --- | --- | --- |
| `enabled` | bool | `true` |
| `drop_null_titles` | bool | `true` |
| `strip_html` | bool | `true` |

## `[stages.filter]`

| Field | Type | Default | Constraints |
| --- | --- | --- | --- |
| `enabled` | bool | `true` | |
| `remote_only` | bool | `false` | |
| `min_salary` | float or null | `null` | |
| `required_keywords` | array of strings | `[]` | Coerced to immutable tuple |

## `[stages.label]`

| Field | Type | Default |
| --- | --- | --- |
| `enabled` | bool | `true` |

## `[stages.rate]`

| Field | Type | Default | Constraints |
| --- | --- | --- | --- |
| `enabled` | bool | `true` | |
| `completeness_weight` | float | `0.5` | Must be `>= 0` |
| `quality_weight` | float | `0.5` | Must be `>= 0` |

## `[stages.match]`

| Field | Type | Default | Constraints |
| --- | --- | --- | --- |
| `enabled` | bool | `true` | |
| `top_k` | int | `100` | Must be `>= 1` |

## `[runtime]`

| Field | Type | Default |
| --- | --- | --- |
| `fail_fast` | bool | `true` |
| `random_seed` | int | `0` |

## Validation Rules

- Strict models reject unknown keys.
- Relative paths resolve from pipeline file directory.
- Invalid config raises `ConfigValidationError`.

Invalid example (`top_k = 0`):

```toml
[stages.match]
enabled = true
top_k = 0
```
