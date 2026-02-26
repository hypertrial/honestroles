## Summary

- What changed:
- Why:

## Plugin API Impact

- [ ] No plugin API change
- [ ] Plugin runtime behavior change
- [ ] Plugin API surface change (docs + migration notes included)

If plugin API changed, describe compatibility impact:

## Validation

- [ ] `pytest -m "not performance" --cov=src --cov=plugin_template/src --cov-fail-under=100 -q`
- [ ] `pytest tests/test_plugin_api_contract.py tests/test_plugin_compat_matrix.py -q`
- [ ] Repository coverage gate passes at 100%

## Checklist

- [ ] Added/updated tests for changed behavior
- [ ] Updated user docs (`README.md`, `docs/`, `examples/README.md`) if relevant
- [ ] Ran docs quality checks (`check_docs_refs`, markdown style/link checks, `pytest tests/docs -q`)
- [ ] Updated `plugins-index/plugins.toml` if adding a new ecosystem plugin
- [ ] Added changelog note for user-facing behavior
