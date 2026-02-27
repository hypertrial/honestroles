from __future__ import annotations

from typing import ForwardRef

import polars as pl

from honestroles.domain import JobDataset
from honestroles.plugins.types import (
    FilterStageContext,
    LabelStageContext,
    RateStageContext,
)


def filter_min_quality(dataset: JobDataset, ctx: FilterStageContext) -> JobDataset:
    threshold = float(ctx.settings.get("min_quality", 0.0))
    df = dataset.to_polars()
    if "rate_quality" in df.columns:
        return dataset.with_frame(df.filter(pl.col("rate_quality").fill_null(0.0) >= threshold))
    return dataset


def label_note(dataset: JobDataset, ctx: LabelStageContext) -> JobDataset:
    return dataset.with_frame(
        dataset.to_polars().with_columns(pl.lit(f"plugin:{ctx.plugin_name}").alias("plugin_label_note"))
    )


def rate_bonus(dataset: JobDataset, ctx: RateStageContext) -> JobDataset:
    bonus = float(ctx.settings.get("bonus", 0.0))
    return dataset.with_frame(
        dataset.to_polars().with_columns(
            (pl.col("rate_composite").fill_null(0.0) + bonus).clip(0.0, 1.0).alias(
                "rate_composite"
            )
        )
    )


def fail_filter(dataset: JobDataset, ctx: FilterStageContext) -> JobDataset:
    _ = (dataset, ctx)
    raise RuntimeError("intentional plugin failure")


def untyped_plugin(dataset, ctx):
    _ = ctx
    return dataset


_UnknownRef = ForwardRef("UnknownType")


def bad_annotation_plugin(
    dataset: _UnknownRef,
    ctx: "FilterStageContext",
) -> "JobDataset":
    _ = ctx
    return dataset


def bad_signature(dataset: JobDataset) -> JobDataset:
    return dataset


NOT_CALLABLE = 123


def kw_only_filter(*, dataset: JobDataset, ctx: FilterStageContext) -> JobDataset:
    _ = ctx
    return dataset


def wrong_return_annotation(
    dataset: JobDataset, ctx: FilterStageContext
) -> int:
    _ = (dataset, ctx)
    return 1


def wrong_context_annotation(
    dataset: JobDataset, ctx: LabelStageContext
) -> JobDataset:
    _ = ctx
    return dataset
