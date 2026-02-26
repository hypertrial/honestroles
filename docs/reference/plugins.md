# Plugins

`honestroles` supports a stable plugin system for custom filtering, labeling, and rating logic without modifying core package code.

## Filter plugins

1. Register a filter plugin.
2. Apply by name through `filter_jobs(..., plugin_filters=[...])`.

```python
import pandas as pd
from honestroles.filter import filter_jobs
from honestroles.plugins import register_filter_plugin


def only_backend(df: pd.DataFrame) -> pd.Series:
    return df["title"].fillna("").str.contains("Backend", case=False)


register_filter_plugin("only_backend", only_backend)
df = filter_jobs(df, plugin_filters=["only_backend"])
```

## Label plugins

1. Register a label transform plugin.
2. Apply by name through `label_jobs(..., plugin_labelers=[...])`.

```python
import pandas as pd
from honestroles.label import label_jobs
from honestroles.plugins import register_label_plugin


def add_region_group(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    result["region_group"] = result["country"].fillna("UNK")
    return result


register_label_plugin("region_group", add_region_group)
df = label_jobs(df, use_llm=False, plugin_labelers=["region_group"])
```

## Rate plugins

Register a post-rating transform plugin and apply it through `rate_jobs(..., plugin_raters=[...])`.

```python
import pandas as pd
from honestroles.rate import rate_jobs
from honestroles.plugins import register_rate_plugin


def mark_top_tier(df: pd.DataFrame, threshold: float = 0.8) -> pd.DataFrame:
    result = df.copy()
    result["is_top_tier"] = result["rating"].fillna(0).ge(threshold)
    return result


register_rate_plugin("mark_top_tier", mark_top_tier)
df = rate_jobs(df, use_llm=False, plugin_raters=["mark_top_tier"])
```

## API

- `register_filter_plugin`
- `unregister_filter_plugin`
- `list_filter_plugins`
- `apply_filter_plugins`
- `register_label_plugin`
- `unregister_label_plugin`
- `list_label_plugins`
- `apply_label_plugins`
- `register_rate_plugin`
- `unregister_rate_plugin`
- `list_rate_plugins`
- `apply_rate_plugins`
- `load_plugins_from_entrypoints`
- `load_plugins_from_module`

## See also

- [Plugin API Contract](plugins/api_contract.md)
- [Plugin Author Guide](plugins/author_guide.md)
- [Plugin Compatibility](plugins/compatibility.md)
- [CLI Guide](../guides/cli.md)
- [FAQ](faq.md)
