## Summary

- What changed:
- Why:

## Plugin API Impact

- [ ] No plugin API change
- [ ] Plugin runtime behavior change
- [ ] Plugin API surface change (docs + migration notes included)

If plugin API changed, describe compatibility impact:

## Validation

- [ ] `pytest -m "not performance" -q`
- [ ] `pytest tests/test_plugin_api_contract.py tests/test_plugin_compat_matrix.py -q`
- [ ] Plugin runtime coverage gate passes (`honestroles.plugins`)

## Checklist

- [ ] Added/updated tests for changed behavior
- [ ] Updated docs (`docs/plugins/*`) if relevant
- [ ] Updated `plugins-index/plugins.toml` if adding a new ecosystem plugin
- [ ] Added changelog note for user-facing behavior
