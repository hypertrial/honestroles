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
