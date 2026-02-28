from __future__ import annotations

import polars as pl

from honestroles import JobDataset
from honestroles.plugins.types import (
    FilterStageContext,
    LabelStageContext,
    RateStageContext,
)


def example_filter(dataset: JobDataset, ctx: FilterStageContext) -> JobDataset:
    _ = ctx
    return dataset.transform(
        lambda frame: frame.filter(pl.col("remote").fill_null(False))
    )


def example_label(dataset: JobDataset, ctx: LabelStageContext) -> JobDataset:
    return dataset.transform(
        lambda frame: frame.with_columns(pl.lit(ctx.plugin_name).alias("plugin_label_source"))
    )


def example_rate(dataset: JobDataset, ctx: RateStageContext) -> JobDataset:
    bonus = float(ctx.settings.get("bonus", 0.0))
    if "rate_composite" not in dataset.columns():
        return dataset.transform(
            lambda frame: frame.with_columns(pl.lit(0.0).alias("rate_composite"))
        )
    return dataset.transform(
        lambda frame: frame.with_columns(
            (pl.col("rate_composite").fill_null(0.0) + bonus).clip(0.0, 1.0).alias("rate_composite")
        )
    )
