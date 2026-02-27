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
| `aliases` | object | `{}` | Optional canonical field alias mapping |

## `[input.aliases]`

Canonical keys allowed:

- `id`
- `title`
- `company`
- `location`
- `remote`
- `description_text`
- `description_html`
- `skills`
- `salary_min`
- `salary_max`
- `apply_url`
- `posted_at`

Each alias value is an ordered array of source column names:

```toml
[input.aliases]
location = ["location_raw"]
remote = ["remote_flag"]
```

Alias behavior:

- Canonical column always wins if present.
- If canonical is missing, runtime uses the first existing alias in order.
- Alias conflicts are recorded in runtime diagnostics under `input_aliasing.conflicts`.

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
| `quality` | object | profile defaults |

## `[runtime.quality]`

| Field | Type | Default | Constraints |
| --- | --- | --- | --- |
| `profile` | `"core_fields_weighted" \\| "equal_weight_all" \\| "strict_recruiting"` | `"core_fields_weighted"` | |
| `field_weights` | mapping of field -> float | `{}` | Values must be `>= 0`; custom map must include at least one positive value |

```toml
[runtime.quality]
profile = "core_fields_weighted"

[runtime.quality.field_weights]
posted_at = 0.6
salary_min = 0.2
```

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
