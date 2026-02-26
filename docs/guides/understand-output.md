# Understand Output

Interpret runtime outputs to verify correctness and drive downstream actions.

## When to use

Use this after every run in local development and CI.

## Prerequisites

- A completed runtime execution

## Steps

1. Inspect diagnostics keys:

```python
keys = sorted(result.diagnostics.keys())
print(keys)
```

2. Confirm row progression:

```python
print(result.diagnostics["stage_rows"])
print(result.diagnostics["final_rows"])
```

3. Confirm plugin loading by kind:

```python
print(result.diagnostics["plugin_counts"])
```

4. Inspect ranked results:

```python
print(result.dataframe.select("fit_rank", "fit_score", "title", "company").head())
print(result.application_plan[:5])
```

## Expected result

- `fit_score` is bounded to `[0.0, 1.0]`
- `fit_rank` starts at `1`
- `application_plan` contains top-ranked entries up to `top_k`
- `non_fatal_errors` appears only when `fail_fast = false` and errors occur

## Next steps

- Diagnostics and result structure: [Runtime API](../reference/runtime-api.md)
- Failure interpretation: [Error Model](../reference/error-model.md)
