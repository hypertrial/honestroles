from __future__ import annotations

import polars as pl

from honestroles.plugins.types import LabelPluginContext


def example_label(df: pl.DataFrame, ctx: LabelPluginContext) -> pl.DataFrame:
    return df.with_columns(pl.lit(ctx.plugin_name).alias("example_plugin_label"))
