# Contributing Plugins

HonestRoles plugins are now manifest-driven and explicit.

## Steps

1. Implement plugin callables with the new ABI:
   - Filter: `(pl.DataFrame, FilterPluginContext) -> pl.DataFrame`
   - Label: `(pl.DataFrame, LabelPluginContext) -> pl.DataFrame`
   - Rate: `(pl.DataFrame, RatePluginContext) -> pl.DataFrame`
2. Reference callables in `plugins.toml` with `module:function`.
3. Validate before runtime execution:

```bash
honestroles plugins validate --manifest plugins.toml
```

4. Run deterministic and fuzz tests:

```bash
pytest -q
pytest -m "fuzz" -q
```

Plugins are fail-fast. Any plugin exception is wrapped as `PluginExecutionError`.
