# CLI Reference

Command reference for `honestroles`.

## Top-Level Commands

```bash
$ honestroles --help
```

Available commands:

- `run`
- `plugins validate`
- `config validate`
- `report-quality`
- `scaffold-plugin`

## Command Matrix

| Command | Required flags | Description | Output |
| --- | --- | --- | --- |
| `honestroles run` | `--pipeline-config`, optional `--plugins` | Runs runtime pipeline | JSON diagnostics |
| `honestroles plugins validate` | `--manifest` | Validates and loads plugin manifest | JSON plugin listing |
| `honestroles config validate` | `--pipeline` | Validates pipeline config | Normalized JSON config |
| `honestroles report-quality` | `--pipeline-config`, optional `--plugins` | Runs runtime and computes quality report | JSON quality summary |
| `honestroles scaffold-plugin` | `--name`, optional `--output-dir` | Copies bundled plugin template | JSON scaffold path + package name |

## Exit Codes

| Exit code | Meaning |
| --- | --- |
| `0` | Success |
| `1` | Generic `HonestRolesError` |
| `2` | `ConfigValidationError` |
| `3` | Plugin load/validation/execution failure |
| `4` | `StageExecutionError` |

## Examples

```bash
$ honestroles run --pipeline-config pipeline.toml --plugins plugins.toml
$ honestroles plugins validate --manifest plugins.toml
$ honestroles config validate --pipeline pipeline.toml
$ honestroles report-quality --pipeline-config pipeline.toml
$ honestroles scaffold-plugin --name my-plugin --output-dir .
```

## `report-quality` Output

`score_percent` uses profile-weighted null scoring.

```json
{
  "row_count": 12345,
  "score_percent": 78.4,
  "profile": "core_fields_weighted",
  "weighted_null_percent": 21.6,
  "effective_weights": {
    "apply_url": 2.5,
    "company": 2.5,
    "description_text": 3.0
  },
  "null_percentages": {
    "apply_url": 0.0,
    "salary_min": 100.0
  }
}
```
