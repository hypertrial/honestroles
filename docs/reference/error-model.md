# Error Model

Failure model and typed error boundaries.

## Error Types

- `ConfigValidationError`: invalid pipeline/manifest paths or schema values
- `RuntimeInitializationError`: unexpected runtime init failures wrapped by runtime constructor
- `StageExecutionError`: stage-level failure wrappers
- `PluginLoadError`: import/lookup errors for plugin callables
- `PluginValidationError`: signature/type annotation validation failures
- `PluginExecutionError`: plugin raised exception or returned wrong type

## Runtime Behavior

- `fail_fast = true`: raise immediately on stage/plugin error.
- `fail_fast = false`: collect non-fatal errors into diagnostics and continue.

Non-fatal diagnostics record shape:

```json
{
  "stage": "filter",
  "error_type": "PluginExecutionError",
  "detail": "..."
}
```

## CLI Exit Mapping

- `0`: success
- `2`: config errors
- `3`: plugin errors
- `4`: stage errors
- `1`: other `HonestRolesError`

## Debug Workflow

1. Run `honestroles config validate --pipeline ...`
2. Run `honestroles plugins validate --manifest ...`
3. Run `honestroles run ...`
4. If using recovery mode, inspect `non_fatal_errors` in diagnostics
