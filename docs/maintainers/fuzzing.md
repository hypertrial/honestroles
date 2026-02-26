# Fuzz Testing

## Profiles

- PR smoke: `HYPOTHESIS_PROFILE=ci_smoke pytest -m "fuzz" -q`
- Nightly deep: `HYPOTHESIS_PROFILE=nightly_deep pytest -m "fuzz" -q`

## Scope

Current fuzz suite targets:

- Runtime pipeline robustness on heterogeneous row shapes
- Plugin manifest loading and callable validation behavior
- CLI argument handling stability

## Reproduction

When Hypothesis prints a minimized counterexample, rerun with:

```bash
pytest path/to/test.py::test_name -q
```

Use `-s` to inspect emitted diagnostics.
