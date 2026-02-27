from __future__ import annotations

import polars as pl

from honestroles import JobDataset
from honestroles.plugins.types import (
    FilterStageContext,
    LabelStageContext,
    RateStageContext,
)
from honestroles_plugin_example.plugins import example_filter, example_label, example_rate


def test_plugin_examples_follow_contract() -> None:
    dataset = JobDataset.from_polars(pl.DataFrame({"remote": [True, False], "rate_composite": [0.5, 0.8]}))

    filtered = example_filter(dataset, FilterStageContext(plugin_name="f"))
    assert isinstance(filtered, JobDataset)

    labeled = example_label(dataset, LabelStageContext(plugin_name="l"))
    assert "plugin_label_source" in labeled.to_polars().columns

    rated = example_rate(
        dataset,
        RateStageContext(plugin_name="r", settings={"bonus": 0.1}),
    )
    assert rated.to_polars()["rate_composite"].max() <= 1.0


def test_plugin_filter_fallback_when_remote_missing() -> None:
    dataset = JobDataset.from_polars(pl.DataFrame({"title": ["a", "b"]}))
    filtered = example_filter(dataset, FilterStageContext(plugin_name="f"))
    assert filtered.to_polars().equals(dataset.to_polars())


def test_plugin_rate_fallback_when_rate_missing() -> None:
    dataset = JobDataset.from_polars(pl.DataFrame({"title": ["a"]}))
    rated = example_rate(dataset, RateStageContext(plugin_name="r"))
    assert "rate_composite" in rated.to_polars().columns
    assert rated.to_polars()["rate_composite"].to_list() == [0.0]
