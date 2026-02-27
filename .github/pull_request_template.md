## Summary

- What changed:
- Why:

## Plugin API Impact

- [ ] No plugin API change
- [ ] Plugin runtime behavior change
- [ ] Plugin API surface change (docs + migration notes included)

If plugin API changed, describe compatibility impact:

## Validation

- [ ] `PYTHONPATH=src:plugin_template/src pytest tests plugin_template/tests -m "not fuzz" --cov=src --cov=plugin_template/src --cov-fail-under=100 -q`
- [ ] `PYTHONPATH=src:plugin_template/src pytest -m "fuzz" -q -o addopts=""`
- [ ] Repository coverage gate passes at 100%

## Checklist

- [ ] Added/updated tests for changed behavior
- [ ] Updated user docs (`README.md`, `docs/`, `examples/README.md`) if relevant
- [ ] Ran docs quality checks (`check_docs_refs`, markdown style/link checks, `pytest tests/docs -q`)
- [ ] Updated `plugins-index/plugins.toml` if adding a new ecosystem plugin
- [ ] Added changelog note for user-facing behavior
