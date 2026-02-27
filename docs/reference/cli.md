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
- `adapter infer`
- `scaffold-plugin`
- `eda generate`
- `eda diff`
- `eda gate`
- `eda dashboard`

## Command Matrix

| Command | Required flags | Description | Output |
| --- | --- | --- | --- |
| `honestroles run` | `--pipeline-config`, optional `--plugins` | Runs runtime pipeline | JSON diagnostics |
| `honestroles plugins validate` | `--manifest` | Validates and loads plugin manifest | JSON plugin listing |
| `honestroles config validate` | `--pipeline` | Validates pipeline config | Normalized JSON config |
| `honestroles report-quality` | `--pipeline-config`, optional `--plugins` | Runs runtime and computes quality report | JSON quality summary |
| `honestroles adapter infer` | `--input-parquet`, optional `--output-file`, `--sample-rows`, `--top-candidates`, `--min-confidence`, optional `--print` | Infers draft `[input.adapter]` mapping/coercion config from a parquet input | JSON artifact locations |
| `honestroles scaffold-plugin` | `--name`, optional `--output-dir` | Copies bundled plugin template | JSON scaffold path + package name |
| `honestroles eda generate` | `--input-parquet`, optional `--output-dir`, `--quality-profile`, repeated `--quality-weight`, `--top-k`, `--max-rows`, optional `--rules-file` | Builds deterministic profile artifacts (`summary.json`, tables, figures, report) | JSON artifact locations |
| `honestroles eda diff` | `--baseline-dir`, `--candidate-dir`, optional `--output-dir`, optional `--rules-file` | Compares two profile artifact dirs and writes diff artifacts (`diff.json`, drift tables) | JSON diff artifact locations |
| `honestroles eda gate` | `--candidate-dir`, optional `--baseline-dir`, optional `--rules-file`, optional `--fail-on`, optional `--warn-on` | Evaluates gate policy and drift thresholds for CI | JSON gate payload + exit status |
| `honestroles eda dashboard` | `--artifacts-dir`, optional `--diff-dir`, optional `--host`, `--port` | Launches Streamlit artifact viewer | Process exit code |

## Exit Codes

| Exit code | Meaning |
| --- | --- |
| `0` | Success |
| `1` | Generic `HonestRolesError` or failed `eda gate` policy |
| `2` | `ConfigValidationError` |
| `3` | Plugin load/validation/execution failure |
| `4` | `StageExecutionError` |

## Examples

```bash
$ honestroles adapter infer --input-parquet data/jobs_historical.parquet --output-file dist/adapters/adapter-draft.toml
$ honestroles eda generate --input-parquet data/jobs_historical.parquet --output-dir dist/eda/baseline
$ honestroles eda generate --input-parquet data/jobs_historical_candidate.parquet --output-dir dist/eda/candidate
$ honestroles eda diff --baseline-dir dist/eda/baseline --candidate-dir dist/eda/candidate --output-dir dist/eda/diff
$ honestroles eda gate --candidate-dir dist/eda/candidate --baseline-dir dist/eda/baseline --rules-file eda-rules.toml
$ honestroles eda dashboard --artifacts-dir dist/eda/candidate --diff-dir dist/eda/diff
```

## `adapter infer` Output

```json
{
  "input_parquet": "/abs/path/data/jobs.parquet",
  "adapter_draft": "/abs/path/dist/adapters/adapter-draft.toml",
  "inference_report": "/abs/path/dist/adapters/adapter-draft.report.json",
  "field_suggestions": 9
}
```

## `eda generate` Output

```json
{
  "artifacts_dir": "/abs/path/dist/eda/latest",
  "manifest": "/abs/path/dist/eda/latest/manifest.json",
  "summary": "/abs/path/dist/eda/latest/summary.json",
  "report": "/abs/path/dist/eda/latest/report.md"
}
```

## `eda diff` Output

```json
{
  "diff_dir": "/abs/path/dist/eda/diff",
  "manifest": "/abs/path/dist/eda/diff/manifest.json",
  "diff_json": "/abs/path/dist/eda/diff/diff.json"
}
```

## `eda gate` Output

```json
{
  "status": "pass",
  "severity_counts": {"P0": 0, "P1": 2, "P2": 1},
  "failures": [],
  "warnings": [
    {
      "type": "finding_count",
      "severity": "P1",
      "count": 2,
      "threshold": 999999,
      "detail": "P1 findings count is 2."
    }
  ],
  "evaluated_rules": {
    "gate": {
      "fail_on": ["P0"],
      "warn_on": ["P1"],
      "max_p0": 0,
      "max_p1": 999999
    },
    "drift": {
      "numeric_warn_psi": 0.1,
      "numeric_fail_psi": 0.25,
      "categorical_warn_jsd": 0.1,
      "categorical_fail_jsd": 0.2,
      "columns_numeric": ["salary_min", "salary_max"],
      "columns_categorical": ["source", "remote", "location", "company"]
    }
  }
}
```
