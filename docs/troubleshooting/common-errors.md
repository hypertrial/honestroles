# Common Errors

Fast diagnosis for common runtime-integrator and plugin-author failures.

## Config Validation Fails

Symptom:

```text
ConfigValidationError
```

Typical causes:

- Missing config file path
- Unknown keys in strict model
- Invalid values (for example `top_k = 0`)

Fix:

```bash
$ honestroles config validate --pipeline pipeline.toml
```

## `doctor` Reports `fail`

Symptom:

- `honestroles doctor` exits with code `1`.

Cause:

- One or more readiness checks failed (environment, config, input/schema, or output path checks).

Fix:

Run in table mode and apply the suggested `fix` values per check:

```bash
$ honestroles doctor --pipeline-config pipeline.toml --plugins plugins.toml --format table
```

Use a custom reliability policy file:

```bash
$ honestroles doctor --pipeline-config pipeline.toml --policy reliability.toml --format table
```

Use strict mode for CI:

```bash
$ honestroles doctor --pipeline-config pipeline.toml --strict --format table
```

In strict mode, aggregate `warn` becomes exit code `1`.

## `ingest sync` Fails with Invalid `source-ref`

Symptom:

```text
source-ref may only contain letters, numbers, '.', '_' and '-'
```

Cause:

- `--source-ref` contains unsupported characters (for example `/` or spaces).

Fix:

Use a valid connector identifier from the source glossary:

```bash
$ honestroles ingest sync --source greenhouse --source-ref stripe
```

See [Ingest Source-Ref Glossary](../reference/ingest-source-ref-glossary.md).

## `ingest sync` Hits HTTP 429 / Backoff

Symptom:

- Sync runs slowly and eventually fails with HTTP 429 or transient HTTP errors.

Cause:

- Public endpoint rate limiting or temporary server issues.

Fix:

- Reduce scope with `--max-pages` and/or `--max-jobs`.
- Re-run after cooldown; retries/backoff are built in for transient failures.
- Split large sources into separate scheduled runs.

## `ingest sync` Returns Empty Result Set

Symptom:

- `rows_written = 0` with successful status.

Cause:

- No public postings currently available.
- Incremental state dropped already-seen postings.

Fix:

Run a full refresh to bypass state filtering:

```bash
$ honestroles ingest sync --source lever --source-ref netflix --full-refresh --format table
```

## Reset Ingestion State

Symptom:

- You need to reprocess from scratch or state became stale/corrupt.

Fix:

- Use a clean state file path:

```bash
$ honestroles ingest sync --source ashby --source-ref notion --state-file .honestroles/ingest/state-reset.json
```

- Or run with `--full-refresh` for one execution.

## `reliability check` Fails in CI

Symptom:

- `honestroles reliability check --strict` exits with code `1`.

Cause:

- One or more checks are `fail`, or strict mode escalated a `warn` status.

Fix:

Run in table mode and remediate by check code:

```bash
$ honestroles reliability check --pipeline-config pipeline.toml --plugins plugins.toml --strict --format table
```

Default artifact path:

```text
dist/reliability/latest/gate_result.json
```

Override artifact path:

```bash
$ honestroles reliability check --pipeline-config pipeline.toml --output-file dist/reliability/custom_gate.json
```

## Reliability Check Code Map

Use this mapping for fast remediation:

| Code | Typical fix |
| --- | --- |
| `ENV_PYTHON_VERSION` | Use Python `>=3.11`. |
| `ENV_REQUIRED_IMPORTS` | Install package dependencies in the active environment. |
| `CONFIG_PIPELINE_PARSE` | Run `honestroles config validate --pipeline ...` and fix config schema errors. |
| `CONFIG_PLUGIN_MANIFEST_PARSE` | Run `honestroles plugins validate --manifest ...` and fix manifest schema/callables. |
| `INPUT_EXISTS` | Point `[input].path` to a readable parquet file. |
| `INPUT_SAMPLE_READ` | Verify parquet readability and file permissions. |
| `INPUT_CANONICAL_CONTRACT` | Fix alias/adapter mappings so canonical fields are populated. |
| `INPUT_CONTENT_READINESS` | Ensure sampled rows include required content (for example non-null titles). |
| `OUTPUT_PATH_WRITABLE` | Create/make writable the output parent directory. |
| `POLICY_MIN_ROWS` | Increase sample/input coverage or lower `min_rows` threshold. |
| `POLICY_REQUIRED_COLUMNS` | Add aliases/adapter mapping for required policy columns. |
| `POLICY_NULL_RATE` | Reduce null rates via mapping/cleaning of flagged fields. |
| `POLICY_FRESHNESS` | Use a valid date column and update source extraction cadence/threshold. |

## Plugin Callable Reference Fails

Symptom:

```text
PluginLoadError
```

Typical causes:

- Missing `:` in `module:function`
- Module import fails
- Function name does not exist

Fix:

```bash
$ honestroles plugins validate --manifest plugins.toml
```

## Plugin Signature Validation Fails

Symptom:

```text
PluginValidationError
```

Typical causes:

- Missing explicit annotations
- Wrong context type for plugin kind
- Wrong return annotation/type

Fix:

Match one of:

- `filter`: `(JobDataset, FilterStageContext) -> JobDataset`
- `label`: `(JobDataset, LabelStageContext) -> JobDataset`
- `rate`: `(JobDataset, RateStageContext) -> JobDataset`

## Plugin Execution Fails

Symptom:

```text
PluginExecutionError
```

Typical causes:

- Plugin raised an exception
- Plugin returned non-DataFrame result

Fix:

- Start with `fail_fast = true` for immediate failure context.
- Add defensive checks in plugin logic.

## Stage Execution Fails

Symptom:

```text
StageExecutionError
```

Typical causes:

- Invalid data shape for stage operation
- Unexpected stage transformation errors

Fix:

- Validate source data and schema assumptions.
- Inspect diagnostics to identify last successful stage.

## Recovery Mode Produces Partial Results

Symptom:

- Run returns success, but output quality is lower than expected.

Cause:

- `fail_fast = false` allowed continuation after one or more stage/plugin errors.

Fix:

Inspect:

```python
print(run.diagnostics.to_dict().get("non_fatal_errors", []))
```

## Adapter Coercion Warnings Are High

Symptom:

- `diagnostics["input_adapter"]["coercion_errors"]` contains high counts.

Cause:

- Source values do not match adapter cast expectations (for example non-boolean tokens in a `cast = "bool"` field, invalid dates for `cast = "date_string"`).

Fix:

1. Inspect adapter diagnostics and samples:

```python
print(run.diagnostics.to_dict()["input_adapter"]["coercion_errors"])
print(run.diagnostics.to_dict()["input_adapter"]["error_samples"][:5])
```

2. Expand adapter field fallbacks and parsing vocab:

```toml
[input.adapter.fields.remote]
from = ["remote_flag", "is_remote"]
cast = "bool"
true_values = ["true", "1", "yes", "y", "remote"]
false_values = ["false", "0", "no", "n", "onsite", "on-site"]
```

3. Regenerate a draft and compare:

```bash
$ honestroles adapter infer --input-parquet data/jobs.parquet --output-file dist/adapters/adapter-draft.toml
```

## `eda dashboard` Fails with Missing Streamlit

Symptom:

```text
streamlit is required for 'honestroles eda dashboard'
```

Cause:

- EDA optional dependencies are not installed.

Fix:

```bash
$ pip install "honestroles[eda]"
```

## `eda dashboard` Fails with Missing Manifest or Summary

Symptom:

```text
artifacts manifest missing
```

or

```text
artifacts summary missing
```

Cause:

- `--artifacts-dir` does not point to a completed `honestroles eda generate` output directory.

Fix:

```bash
$ honestroles eda generate --input-parquet jobs.parquet --output-dir dist/eda/latest
$ honestroles eda dashboard --artifacts-dir dist/eda/latest
```

## `eda diff` Fails with Profile Artifact Errors

Symptom:

```text
EDA diff requires profile artifacts as inputs
```

Cause:

- `--baseline-dir` or `--candidate-dir` points to non-profile artifacts (for example a diff artifact directory).

Fix:

```bash
$ honestroles eda generate --input-parquet baseline.parquet --output-dir dist/eda/baseline
$ honestroles eda generate --input-parquet candidate.parquet --output-dir dist/eda/candidate
$ honestroles eda diff --baseline-dir dist/eda/baseline --candidate-dir dist/eda/candidate
```

## `eda gate` Returns Exit Code 1

Symptom:

- Command exits with code `1`.

Cause:

- Gate policy failed (`P0` findings above threshold, or drift metrics exceeded fail thresholds).

Fix:

Inspect output payload:

```bash
$ honestroles eda gate --candidate-dir dist/eda/candidate --baseline-dir dist/eda/baseline --rules-file eda-rules.toml
```

Review:

- `failures`
- `warnings`
- `evaluated_rules`

Then adjust source extraction/normalization or update thresholds in `eda-rules.toml` as needed.

## Manual Publish Fails with Missing PyPI API Key

Symptom:

```text
Missing PyPI API key. Set PYPI_API_KEY (or PYPI_API_TOKEN) in env or .env.
```

Cause:

- `scripts/publish_pypi.sh` could not find `PYPI_API_KEY` or `PYPI_API_TOKEN`.

Fix:

1. Add `PYPI_API_KEY=<pypi-token>` to your shell env or `.env`.
2. Re-run:

```bash
$ bash scripts/publish_pypi.sh
```

## Manual Release Workflow Fails with Missing PyPI Secret

Symptom:

```text
Missing PyPI token secret. Set PYPI_API_KEY (or PYPI_API_TOKEN) in repository secrets.
```

Cause:

- GitHub Actions cannot read local `.env` values.
- Neither `PYPI_API_KEY` nor `PYPI_API_TOKEN` is set in repository secrets.

Fix:

1. Add one of these repository secrets:
   `PYPI_API_KEY` or `PYPI_API_TOKEN`.
2. Re-run the manual `Release` workflow (`workflow_dispatch`).

## `runs show` Cannot Find a Run

Symptom:

```text
run record not found
```

Cause:

- `--run-id` does not exist under `.honestroles/runs/` in the current project directory.

Fix:

1. List available run IDs:

```bash
$ honestroles runs list --limit 20 --format table
```

2. Use one of those IDs:

```bash
$ honestroles runs show --run-id <run_id>
```

## Coverage Gate Drops Below 100% in CI

Symptom:

```text
Coverage failure: total of 98 is less than fail-under=100
```

Common cause:

- Tests rely on optional plotting/runtime dependencies being installed in CI.

Fix:

- Run the canonical gate command:

```bash
$ PYTHON_BIN=.venv/bin/python bash scripts/run_coverage.sh
```

- Keep tests deterministic by stubbing optional dependencies instead of requiring them for coverage-critical paths.
