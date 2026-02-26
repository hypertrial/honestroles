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

- `filter`: `(pl.DataFrame, FilterPluginContext) -> pl.DataFrame`
- `label`: `(pl.DataFrame, LabelPluginContext) -> pl.DataFrame`
- `rate`: `(pl.DataFrame, RatePluginContext) -> pl.DataFrame`

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
print(result.diagnostics.get("non_fatal_errors", []))
```
