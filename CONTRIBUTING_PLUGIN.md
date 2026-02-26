# Contributing Plugins

HonestRoles plugins are manifest-driven and loaded per runtime instance.

## Authoring Checklist

1. Implement ABI-compliant callables:
   - Filter: `(pl.DataFrame, FilterPluginContext) -> pl.DataFrame`
   - Label: `(pl.DataFrame, LabelPluginContext) -> pl.DataFrame`
   - Rate: `(pl.DataFrame, RatePluginContext) -> pl.DataFrame`
2. Reference callables in `plugins.toml` with `module:function`.
3. Validate manifest:

```bash
$ honestroles plugins validate --manifest plugins.toml
```

4. Run tests:

```bash
$ pytest -q
$ pytest -m "fuzz" -q
```

## Related Docs

- User-facing plugin guide: `docs/guides/author-plugins.md`
- Manifest and ABI reference: `docs/reference/plugin-manifest-schema.md`
- Maintainer fuzzing guide: `docs/for-maintainers/fuzzing.md`
