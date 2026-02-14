from __future__ import annotations

import pandas as pd
import pytest

from honestroles.filter import filter_jobs
from honestroles.label import label_jobs
from honestroles.plugins import (
    apply_filter_plugins,
    apply_label_plugins,
    list_filter_plugins,
    list_label_plugins,
    register_filter_plugin,
    register_label_plugin,
    unregister_filter_plugin,
    unregister_label_plugin,
)


def test_filter_plugin_registry_and_filter_jobs_integration(sample_df: pd.DataFrame) -> None:
    plugin_name = "only_product"
    unregister_filter_plugin(plugin_name)

    def only_product(df: pd.DataFrame) -> pd.Series:
        return df["title"].fillna("").str.contains("Product")

    register_filter_plugin(plugin_name, only_product)
    assert plugin_name in list_filter_plugins()

    filtered = filter_jobs(sample_df, plugin_filters=[plugin_name])
    assert filtered["job_id"].tolist() == ["2"]

    unregister_filter_plugin(plugin_name)


def test_label_plugin_registry_and_label_jobs_integration(sample_df: pd.DataFrame) -> None:
    plugin_name = "mark_source"
    unregister_label_plugin(plugin_name)

    def mark_source(df: pd.DataFrame, suffix: str = "_ok") -> pd.DataFrame:
        result = df.copy()
        result["source_marker"] = result["source"].astype(str) + suffix
        return result

    register_label_plugin(plugin_name, mark_source)
    assert plugin_name in list_label_plugins()

    labeled = label_jobs(
        sample_df,
        use_llm=False,
        plugin_labelers=[plugin_name],
        plugin_labeler_kwargs={plugin_name: {"suffix": "_yes"}},
    )
    assert "source_marker" in labeled.columns
    assert labeled["source_marker"].tolist() == ["greenhouse_yes", "greenhouse_yes"]

    unregister_label_plugin(plugin_name)


def test_duplicate_plugin_registration_fails() -> None:
    plugin_name = "dup_filter"
    unregister_filter_plugin(plugin_name)
    register_filter_plugin(plugin_name, lambda df: pd.Series([True] * len(df)))

    with pytest.raises(ValueError, match="already registered"):
        register_filter_plugin(plugin_name, lambda df: pd.Series([True] * len(df)))

    unregister_filter_plugin(plugin_name)


def test_unknown_plugin_errors(sample_df: pd.DataFrame) -> None:
    with pytest.raises(KeyError, match="Unknown filter plugin"):
        apply_filter_plugins(sample_df, ["missing-filter"])
    with pytest.raises(KeyError, match="Unknown label plugin"):
        apply_label_plugins(sample_df, ["missing-label"])
