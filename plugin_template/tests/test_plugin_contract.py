from __future__ import annotations

import polars as pl

from honestroles import JobDataset
from honestroles.plugins.types import (
    FilterStageContext,
    LabelStageContext,
    RateStageContext,
)
from honestroles_plugin_example.plugins import example_filter, example_label, example_rate


def _canonical_dataset(**overrides) -> JobDataset:
    payload = {
        "id": ["1", "2"],
        "title": ["a", "b"],
        "company": ["x", "y"],
        "location": ["Remote", "NYC"],
        "remote": [True, False],
        "description_text": ["desc", "desc"],
        "description_html": [None, None],
        "skills": [["python"], ["sql"]],
        "salary_min": [None, None],
        "salary_max": [None, None],
        "apply_url": ["https://x/1", "https://x/2"],
        "posted_at": ["2026-01-01", "2026-01-02"],
    }
    payload.update(overrides)
    return JobDataset.from_polars(pl.DataFrame(payload))


def test_plugin_examples_follow_contract() -> None:
    dataset = _canonical_dataset(rate_composite=[0.5, 0.8])

    filtered = example_filter(dataset, FilterStageContext(plugin_name="f"))
    assert isinstance(filtered, JobDataset)

    labeled = example_label(dataset, LabelStageContext(plugin_name="l"))
    assert "plugin_label_source" in labeled.to_polars().columns

    rated = example_rate(
        dataset,
        RateStageContext(plugin_name="r", settings={"bonus": 0.1}),
    )
    assert rated.to_polars()["rate_composite"].max() <= 1.0


def test_plugin_filter_handles_null_remote_values() -> None:
    dataset = _canonical_dataset(remote=[None, True])
    filtered = example_filter(dataset, FilterStageContext(plugin_name="f"))
    assert filtered.to_polars()["id"].to_list() == ["2"]


def test_plugin_rate_fallback_when_rate_missing() -> None:
    dataset = _canonical_dataset(
        id=["1"],
        title=["a"],
        company=["x"],
        location=["Remote"],
        remote=[True],
        description_text=["desc"],
        description_html=[None],
        skills=[["python"]],
        salary_min=[None],
        salary_max=[None],
        apply_url=["https://x/1"],
        posted_at=["2026-01-01"],
    )
    rated = example_rate(dataset, RateStageContext(plugin_name="r"))
    assert "rate_composite" in rated.to_polars().columns
    assert rated.to_polars()["rate_composite"].to_list() == [0.0]
