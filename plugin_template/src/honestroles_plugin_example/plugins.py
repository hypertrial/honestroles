from __future__ import annotations

import pandas as pd

from honestroles.plugins import (
    PluginExport,
    PluginSpec,
    register_filter_plugin,
    register_label_plugin,
    register_rate_plugin,
)


def only_remote(df: pd.DataFrame) -> pd.Series:
    return df.get("remote_flag", pd.Series([False] * len(df), index=df.index)).fillna(False)


def add_source_group(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    result["source_group"] = result.get("source", pd.Series(["unknown"] * len(df))).astype(str)
    return result


def add_priority_rating(df: pd.DataFrame, *, threshold: float = 0.8) -> pd.DataFrame:
    result = df.copy()
    result["priority_role"] = result.get("rating", pd.Series([0.0] * len(df))).fillna(0).ge(
        threshold
    )
    return result


_BASE_SPEC = PluginSpec(api_version="1.0", plugin_version="0.1.0")

filter_plugin_export = PluginExport(
    kind="filter",
    name="only_remote",
    plugin=only_remote,
    spec=PluginSpec(
        api_version=_BASE_SPEC.api_version,
        plugin_version=_BASE_SPEC.plugin_version,
        capabilities=("filter", "remote"),
    ),
)

label_plugin_export = PluginExport(
    kind="label",
    name="add_source_group",
    plugin=add_source_group,
    spec=PluginSpec(
        api_version=_BASE_SPEC.api_version,
        plugin_version=_BASE_SPEC.plugin_version,
        capabilities=("label", "source"),
    ),
)

rate_plugin_export = PluginExport(
    kind="rate",
    name="add_priority_rating",
    plugin=add_priority_rating,
    spec=PluginSpec(
        api_version=_BASE_SPEC.api_version,
        plugin_version=_BASE_SPEC.plugin_version,
        capabilities=("rate", "priority"),
    ),
)


def register_plugins() -> None:
    register_filter_plugin("only_remote", only_remote, spec=filter_plugin_export.spec)
    register_label_plugin("add_source_group", add_source_group, spec=label_plugin_export.spec)
    register_rate_plugin("add_priority_rating", add_priority_rating, spec=rate_plugin_export.spec)


def register() -> None:
    register_plugins()
