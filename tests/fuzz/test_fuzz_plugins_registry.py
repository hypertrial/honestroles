from __future__ import annotations

import pandas as pd
import pytest
from hypothesis import given
from hypothesis import strategies as st

from honestroles.plugins import (
    apply_filter_plugins,
    apply_label_plugins,
    apply_rate_plugins,
    list_filter_plugins,
    list_label_plugins,
    list_rate_plugins,
    register_filter_plugin,
    register_label_plugin,
    register_rate_plugin,
    reset_plugins,
    unregister_filter_plugin,
    unregister_label_plugin,
    unregister_rate_plugin,
)

from .strategies import PLUGIN_NAME_VALUES


@pytest.mark.fuzz
@given(name=PLUGIN_NAME_VALUES, overwrite=st.booleans())
def test_fuzz_plugin_registry_lifecycle(name: str, overwrite: bool) -> None:
    reset_plugins()
    df = pd.DataFrame({"title": ["a", "bb", "ccc"]})

    def filt(frame: pd.DataFrame, *, min_len: int = 1) -> pd.Series:
        return frame["title"].astype("string").fillna("").str.len().ge(min_len)

    register_filter_plugin(name, filt)
    if overwrite:
        register_filter_plugin(name, filt, overwrite=True)
    else:
        with pytest.raises(ValueError):
            register_filter_plugin(name, filt)

    filtered = apply_filter_plugins(df, [name], plugin_kwargs={name: {"min_len": 2}})
    assert len(filtered) <= len(df)
    assert name in list_filter_plugins()
    unregister_filter_plugin(name)
    assert name not in list_filter_plugins()


@pytest.mark.fuzz
@given(label_name=PLUGIN_NAME_VALUES, rate_name=PLUGIN_NAME_VALUES)
def test_fuzz_label_and_rate_plugin_apply(label_name: str, rate_name: str) -> None:
    reset_plugins()
    df = pd.DataFrame({"x": [1, 2]})

    def labeler(frame: pd.DataFrame, *, tag: str = "ok") -> pd.DataFrame:
        return frame.assign(label_tag=tag)

    def rater(frame: pd.DataFrame, *, bonus: float = 0.1) -> pd.DataFrame:
        return frame.assign(rating=bonus)

    register_label_plugin(label_name, labeler)
    register_rate_plugin(rate_name, rater)

    labeled = apply_label_plugins(df, [label_name], plugin_kwargs={label_name: {"tag": "x"}})
    rated = apply_rate_plugins(labeled, [rate_name], plugin_kwargs={rate_name: {"bonus": 0.25}})

    assert "label_tag" in rated.columns
    assert "rating" in rated.columns
    assert label_name in list_label_plugins()
    assert rate_name in list_rate_plugins()

    unregister_label_plugin(label_name)
    unregister_rate_plugin(rate_name)
