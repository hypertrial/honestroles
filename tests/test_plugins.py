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


def test_duplicate_label_plugin_registration_fails() -> None:
    plugin_name = "dup_label"
    unregister_label_plugin(plugin_name)
    register_label_plugin(plugin_name, lambda df: df)

    with pytest.raises(ValueError, match="already registered"):
        register_label_plugin(plugin_name, lambda df: df)

    unregister_label_plugin(plugin_name)


def test_unknown_plugin_errors(sample_df: pd.DataFrame) -> None:
    with pytest.raises(KeyError, match="Unknown filter plugin"):
        apply_filter_plugins(sample_df, ["missing-filter"])
    with pytest.raises(KeyError, match="Unknown label plugin"):
        apply_label_plugins(sample_df, ["missing-label"])


def test_filter_plugins_invalid_mode_raises(sample_df: pd.DataFrame) -> None:
    with pytest.raises(ValueError, match="mode must be 'and' or 'or'"):
        apply_filter_plugins(sample_df, [], mode="xor")


def test_plugin_name_must_be_non_empty(sample_df: pd.DataFrame) -> None:
    with pytest.raises(ValueError, match="non-empty"):
        register_filter_plugin("   ", lambda df: pd.Series([True] * len(df)))
    with pytest.raises(ValueError, match="non-empty"):
        apply_filter_plugins(sample_df, ["   "])
    with pytest.raises(ValueError, match="non-empty"):
        register_label_plugin("   ", lambda df: df)
    with pytest.raises(ValueError, match="non-empty"):
        apply_label_plugins(sample_df, ["   "])


def test_apply_filter_plugins_or_mode(sample_df: pd.DataFrame) -> None:
    register_filter_plugin("first_only", lambda df: pd.Series([True, False], index=df.index))
    register_filter_plugin("second_only", lambda df: pd.Series([False, True], index=df.index))
    filtered = apply_filter_plugins(sample_df, ["first_only", "second_only"], mode="or")
    assert filtered["job_id"].tolist() == ["1", "2"]


def test_apply_filter_plugins_and_mode(sample_df: pd.DataFrame) -> None:
    register_filter_plugin("first_only", lambda df: pd.Series([True, False], index=df.index))
    register_filter_plugin("second_only", lambda df: pd.Series([False, True], index=df.index))
    filtered = apply_filter_plugins(sample_df, ["first_only", "second_only"], mode="and")
    assert filtered.empty


def test_apply_filter_plugins_enforces_series_return_type(sample_df: pd.DataFrame) -> None:
    register_filter_plugin("bad_filter", lambda df: [True] * len(df))  # type: ignore[return-value]
    with pytest.raises(TypeError, match="must return a pandas Series mask"):
        apply_filter_plugins(sample_df, ["bad_filter"])


def test_apply_label_plugins_enforces_dataframe_return_type(sample_df: pd.DataFrame) -> None:
    register_label_plugin("bad_labeler", lambda df: ["wrong"])  # type: ignore[return-value]
    with pytest.raises(TypeError, match="must return a pandas DataFrame"):
        apply_label_plugins(sample_df, ["bad_labeler"])


def test_apply_plugins_with_empty_plugin_list_returns_input(sample_df: pd.DataFrame) -> None:
    filtered = apply_filter_plugins(sample_df, [])
    labeled = apply_label_plugins(sample_df, [])
    assert filtered.equals(sample_df)
    assert labeled.equals(sample_df)


def test_register_filter_plugin_overwrite_replaces_existing(sample_df: pd.DataFrame) -> None:
    register_filter_plugin("only_first", lambda df: pd.Series([True, False], index=df.index))
    register_filter_plugin(
        "only_first",
        lambda df: pd.Series([False, True], index=df.index),
        overwrite=True,
    )
    filtered = apply_filter_plugins(sample_df, ["only_first"])
    assert filtered["job_id"].tolist() == ["2"]


def test_register_label_plugin_overwrite_replaces_existing(sample_df: pd.DataFrame) -> None:
    def first(df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result["marker"] = "v1"
        return result

    def second(df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result["marker"] = "v2"
        return result

    register_label_plugin("mark", first)
    register_label_plugin("mark", second, overwrite=True)
    labeled = apply_label_plugins(sample_df, ["mark"])
    assert labeled["marker"].tolist() == ["v2", "v2"]
