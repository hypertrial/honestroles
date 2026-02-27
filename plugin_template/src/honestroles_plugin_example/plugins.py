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
    df = dataset.to_polars()
    if "remote" not in df.columns:
        return dataset
    return dataset.with_frame(df.filter(pl.col("remote").cast(pl.Boolean, strict=False).fill_null(False)))


def example_label(dataset: JobDataset, ctx: LabelStageContext) -> JobDataset:
    return dataset.with_frame(
        dataset.to_polars().with_columns(pl.lit(ctx.plugin_name).alias("plugin_label_source"))
    )


def example_rate(dataset: JobDataset, ctx: RateStageContext) -> JobDataset:
    bonus = float(ctx.settings.get("bonus", 0.0))
    df = dataset.to_polars()
    if "rate_composite" not in df.columns:
        return dataset.with_frame(df.with_columns(pl.lit(0.0).alias("rate_composite")))
    return dataset.with_frame(
        df.with_columns(
            (pl.col("rate_composite").fill_null(0.0) + bonus).clip(0.0, 1.0).alias(
                "rate_composite"
            )
        )
    )
