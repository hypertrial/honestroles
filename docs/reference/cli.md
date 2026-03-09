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
- `init`
- `doctor`
- `adapter infer`
- `runs list`
- `runs show`
- `scaffold-plugin`
- `eda generate`
- `eda diff`
- `eda gate`
- `eda dashboard`

## Output Formats

Structured-output commands default to JSON and accept `--format {json,table}`.

- `json`: stable machine-readable payloads (default).
- `table`: concise human-readable summaries for terminals and CI logs.

`honestroles eda dashboard` launches Streamlit and does not use payload formatting.

## Command Matrix

| Command | Required flags | Description | Output |
| --- | --- | --- | --- |
| `honestroles run` | `--pipeline-config`, optional `--plugins` | Runs runtime pipeline | JSON/table diagnostics |
| `honestroles plugins validate` | `--manifest` | Validates and loads plugin manifest | JSON/table plugin listing |
| `honestroles config validate` | `--pipeline` | Validates pipeline config | JSON/table normalized config |
| `honestroles report-quality` | `--pipeline-config`, optional `--plugins` | Runs runtime and computes quality report | JSON/table quality summary |
| `honestroles init` | `--input-parquet`, optional `--pipeline-config`, `--plugins-manifest`, `--output-parquet`, `--sample-rows`, `--force` | Scaffolds pipeline config + plugin manifest from sample data | JSON/table scaffold summary |
| `honestroles doctor` | `--pipeline-config`, optional `--plugins`, `--sample-rows` | Validates environment, config, schema readiness, and output path | JSON/table checks + summary |
| `honestroles adapter infer` | `--input-parquet`, optional `--output-file`, `--sample-rows`, `--top-candidates`, `--min-confidence`, optional `--print` | Infers draft `[input.adapter]` mapping/coercion config from parquet input | JSON/table artifact summary |
| `honestroles runs list` | optional `--limit`, `--status` | Lists recorded run lineage entries | JSON/table run rows |
| `honestroles runs show` | `--run-id` | Shows one recorded run lineage payload | JSON/table run record |
| `honestroles scaffold-plugin` | `--name`, optional `--output-dir` | Copies bundled plugin template | JSON/table scaffold path + package name |
| `honestroles eda generate` | `--input-parquet`, optional `--output-dir`, `--quality-profile`, repeated `--quality-weight`, `--top-k`, `--max-rows`, optional `--rules-file` | Builds deterministic profile artifacts (`summary.json`, tables, figures, report) | JSON/table artifact summary |
| `honestroles eda diff` | `--baseline-dir`, `--candidate-dir`, optional `--output-dir`, optional `--rules-file` | Compares two profile artifact dirs and writes diff artifacts (`diff.json`, drift tables) | JSON/table diff summary |
| `honestroles eda gate` | `--candidate-dir`, optional `--baseline-dir`, optional `--rules-file`, optional `--fail-on`, optional `--warn-on` | Evaluates gate policy and drift thresholds for CI | JSON/table gate summary + exit status |
| `honestroles eda dashboard` | `--artifacts-dir`, optional `--diff-dir`, optional `--host`, `--port` | Launches Streamlit artifact viewer | Process exit code |

## Run Lineage

Tracked commands write a run record to:

```text
.honestroles/runs/<run_id>/run.json
```

Tracked commands:

- `run`
- `report-quality`
- `adapter infer`
- `eda generate`
- `eda diff`
- `eda gate`

Run schema fields include:

- `schema_version`
- `run_id`
- `command`
- `status`
- `started_at_utc`, `finished_at_utc`, `duration_ms`
- `input_hash`, `input_hashes`
- `config_hash`
- `artifact_paths`
- `error` (present on failures)

## Exit Codes

| Exit code | Meaning |
| --- | --- |
| `0` | Success. Includes `doctor` statuses `pass` and `warn`. |
| `1` | Generic `HonestRolesError`, failed `eda gate` policy, or `doctor` status `fail`. |
| `2` | `ConfigValidationError` (invalid args, invalid/missing config, bad run lookup, etc.). |
| `3` | Plugin load/validation/execution failure. |
| `4` | `StageExecutionError`. |

## Examples

```bash
$ honestroles init --input-parquet data/jobs.parquet --pipeline-config pipeline.toml --plugins-manifest plugins.toml
$ honestroles doctor --pipeline-config pipeline.toml --plugins plugins.toml --format table
$ honestroles run --pipeline-config pipeline.toml --plugins plugins.toml --format table
$ honestroles adapter infer --input-parquet data/jobs.parquet --output-file dist/adapters/adapter-draft.toml
$ honestroles runs list --limit 10 --format table
$ honestroles runs show --run-id <run_id>
$ honestroles eda generate --input-parquet data/jobs.parquet --output-dir dist/eda/latest
$ honestroles eda diff --baseline-dir dist/eda/baseline --candidate-dir dist/eda/candidate --output-dir dist/eda/diff
$ honestroles eda gate --candidate-dir dist/eda/candidate --baseline-dir dist/eda/baseline --rules-file eda-rules.toml
$ honestroles eda dashboard --artifacts-dir dist/eda/candidate --diff-dir dist/eda/diff
```
