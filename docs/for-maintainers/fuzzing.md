# Fuzz Testing

Maintainer guide for property/fuzz coverage.

## Profiles

- PR smoke: `HYPOTHESIS_PROFILE=ci_smoke pytest -m "fuzz" -q`
- Nightly deep: `HYPOTHESIS_PROFILE=nightly_deep pytest -m "fuzz" -q`

## Deterministic Coverage Gate

```bash
$ PYTHON_BIN=.venv/bin/python bash scripts/run_coverage.sh
```

Notes:

- Coverage gate is `100%` for `src/honestroles`.
- Keep chart/EDA tests deterministic even when optional plotting deps are missing.
- Do not rely on optional extras at runtime to make coverage pass.

## Scope

Current fuzz suite targets:

- Runtime pipeline robustness on heterogeneous row shapes
- Plugin manifest loading and callable validation behavior
- CLI argument handling stability

## Reproduction

```bash
$ pytest path/to/test.py::test_name -q -s
```
