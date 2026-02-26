# Non-Fail-Fast and Recovery

Choose between strict fail-fast execution and best-effort continuation.

## When to use

Use this when deciding how runtime should behave after stage/plugin failures.

## Prerequisites

- A pipeline config with `[runtime]`

## Steps

Set strict mode:

```toml
[runtime]
fail_fast = true
random_seed = 0
```

Set recovery mode:

```toml
[runtime]
fail_fast = false
random_seed = 0
```

Run and inspect diagnostics in recovery mode:

```bash
$ honestroles run --pipeline-config pipeline.toml --plugins plugins.toml
```

## Expected result

- `fail_fast = true`: execution stops at first `HonestRolesError`.
- `fail_fast = false`: execution continues, and diagnostics include:

```json
{
  "non_fatal_errors": [
    {
      "stage": "filter",
      "error_type": "PluginExecutionError",
      "detail": "..."
    }
  ]
}
```

!!! warning
    Recovery mode can produce partial outputs. Always inspect `non_fatal_errors` before using results.

## Next steps

- Failure taxonomy and wrappers: [Error Model](../reference/error-model.md)
- Practical failure signatures: [Common Errors](../troubleshooting/common-errors.md)
