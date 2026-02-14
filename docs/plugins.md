# Plugins

`honestroles` supports lightweight plugin registration for custom filtering and labeling logic without modifying core package code.

## Filter Plugins

1. Register a filter plugin.
2. Apply by name through `filter_jobs(..., plugin_filters=[...])`.

```python
import pandas as pd
import honestroles as hr

def only_backend(df: pd.DataFrame) -> pd.Series:
    return df["title"].fillna("").str.contains("Backend", case=False)

hr.register_filter_plugin("only_backend", only_backend)
df = hr.filter_jobs(df, plugin_filters=["only_backend"])
```

## Label Plugins

1. Register a label transform plugin.
2. Apply by name through `label_jobs(..., plugin_labelers=[...])`.

```python
import pandas as pd
import honestroles as hr

def add_region_group(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    result["region_group"] = result["country"].fillna("UNK")
    return result

hr.register_label_plugin("region_group", add_region_group)
df = hr.label_jobs(df, use_llm=False, plugin_labelers=["region_group"])
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
