from __future__ import annotations

from typing import ForwardRef

import polars as pl

from honestroles.plugins.types import (
    FilterPluginContext,
    LabelPluginContext,
    RatePluginContext,
)


def filter_min_quality(df: pl.DataFrame, ctx: FilterPluginContext) -> pl.DataFrame:
    threshold = float(ctx.settings.get("min_quality", 0.0))
    if "rate_quality" in df.columns:
        return df.filter(pl.col("rate_quality").fill_null(0.0) >= threshold)
    return df


def label_note(df: pl.DataFrame, ctx: LabelPluginContext) -> pl.DataFrame:
    return df.with_columns(pl.lit(f"plugin:{ctx.plugin_name}").alias("plugin_label_note"))


def rate_bonus(df: pl.DataFrame, ctx: RatePluginContext) -> pl.DataFrame:
    bonus = float(ctx.settings.get("bonus", 0.0))
    return df.with_columns(
        (pl.col("rate_composite").fill_null(0.0) + bonus).clip(0.0, 1.0).alias(
            "rate_composite"
        )
    )


def fail_filter(df: pl.DataFrame, ctx: FilterPluginContext) -> pl.DataFrame:
    _ = (df, ctx)
    raise RuntimeError("intentional plugin failure")


def untyped_plugin(df, ctx):
    _ = ctx
    return df


_UnknownRef = ForwardRef("UnknownType")


def bad_annotation_plugin(
    df: _UnknownRef,
    ctx: "FilterPluginContext",
) -> "pl.DataFrame":
    return df


def bad_signature(df: pl.DataFrame) -> pl.DataFrame:
    return df


NOT_CALLABLE = 123


def kw_only_filter(*, df: pl.DataFrame, ctx: FilterPluginContext) -> pl.DataFrame:
    _ = ctx
    return df


def wrong_return_annotation(
    df: pl.DataFrame, ctx: FilterPluginContext
) -> int:
    _ = (df, ctx)
    return 1


def wrong_context_annotation(
    df: pl.DataFrame, ctx: LabelPluginContext
) -> pl.DataFrame:
    _ = ctx
    return df
