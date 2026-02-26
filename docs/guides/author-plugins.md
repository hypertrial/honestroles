# Author Plugins

Create ABI-compliant plugins for `filter`, `label`, or `rate` stages.

## When to use

Use this when built-in stage behavior is not sufficient.

## Prerequisites

- HonestRoles installed
- Familiarity with Polars

## Steps

1. Scaffold a package:

```bash
$ honestroles scaffold-plugin --name my-plugin --output-dir .
```

2. Implement plugin callable with strict annotations:

```python
import polars as pl
from honestroles.plugins.types import LabelPluginContext


def add_note(df: pl.DataFrame, ctx: LabelPluginContext) -> pl.DataFrame:
    return df.with_columns(pl.lit(f"plugin:{ctx.plugin_name}").alias("plugin_note"))
```

3. Register it in `plugins.toml`:

```toml
[[plugins]]
name = "add_note"
kind = "label"
callable = "my_plugin.plugins:add_note"
enabled = true
order = 10

[plugins.settings]
note = "optional-settings"
```

4. Validate then run:

```bash
$ honestroles plugins validate --manifest plugins.toml
$ honestroles run --pipeline-config pipeline.toml --plugins plugins.toml
```

## Expected result

- Manifest validates successfully.
- Plugin executes in deterministic order by `(kind, order, name)`.
- Plugin settings are immutable inside context (`ctx.settings`).

## Next steps

- Full manifest and ABI contract: [Plugin Manifest Schema](../reference/plugin-manifest-schema.md)
- Plugin failure debugging: [Common Errors](../troubleshooting/common-errors.md)
