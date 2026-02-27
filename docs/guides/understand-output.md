# Understand Output

Interpret runtime outputs to verify correctness and drive downstream actions.

## When to use

Use this after every run in local development and CI.

## Prerequisites

- A completed runtime execution

## Steps

1. Inspect diagnostics keys:

```python
diagnostics = run.diagnostics.to_dict()
print(sorted(diagnostics.keys()))
```

2. Confirm row progression:

```python
print(diagnostics["stage_rows"])
print(diagnostics["final_rows"])
```

3. Confirm plugin loading by kind:

```python
print(diagnostics["plugin_counts"])
```

4. Inspect ranked results:

```python
frame = run.dataset.to_polars()
print(frame.select("fit_rank", "fit_score", "title", "company").head())
print([entry.to_dict() for entry in run.application_plan[:5]])
```

## Expected result

- `fit_score` is bounded to `[0.0, 1.0]`
- `fit_rank` starts at `1`
- `application_plan` contains top-ranked entries up to `top_k`
- `non_fatal_errors` appears only when `fail_fast = false` and errors occur

## Next steps

- Runtime contract details: [Runtime API](../reference/runtime-api.md)
- Stage semantics: [Stage Contracts](../reference/stage-contracts.md)
