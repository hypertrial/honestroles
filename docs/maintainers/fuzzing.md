# Fuzz Testing

## Profiles

- PR smoke: `HYPOTHESIS_PROFILE=ci_smoke pytest -m "fuzz" -q`
- Nightly deep: `HYPOTHESIS_PROFILE=nightly_deep pytest -m "fuzz" -q`

## Deterministic Coverage Gate

Run the non-fuzz coverage gate locally:

```bash
PYTHONPATH=src:plugin_template/src pytest tests plugin_template/tests \
  -m "not fuzz" -o addopts="" \
  --cov=src --cov=plugin_template/src --cov-report=term-missing \
  --cov-fail-under=100 -q
```

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
