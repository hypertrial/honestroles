from __future__ import annotations

import polars as pl

from honestroles.plugins.types import (
    FilterPluginContext,
    LabelPluginContext,
    RatePluginContext,
)
from honestroles_plugin_example.plugins import example_filter, example_label, example_rate


def test_plugin_examples_follow_contract() -> None:
    frame = pl.DataFrame({"remote": [True, False], "rate_composite": [0.5, 0.8]})

    filtered = example_filter(frame, FilterPluginContext(plugin_name="f"))
    assert isinstance(filtered, pl.DataFrame)

    labeled = example_label(frame, LabelPluginContext(plugin_name="l"))
    assert "plugin_label_source" in labeled.columns

    rated = example_rate(
        frame,
        RatePluginContext(plugin_name="r", settings={"bonus": 0.1}),
    )
    assert rated["rate_composite"].max() <= 1.0
