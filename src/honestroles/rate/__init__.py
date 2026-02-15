from __future__ import annotations

import pandas as pd

from honestroles.plugins import apply_rate_plugins
from honestroles.rate.completeness import rate_completeness
from honestroles.rate.composite import rate_composite
from honestroles.rate.quality import rate_quality

__all__ = ["rate_jobs", "rate_completeness", "rate_quality", "rate_composite"]


def rate_jobs(
    df: pd.DataFrame,
    *,
    use_llm: bool = False,
    model: str = "llama3",
    ollama_url: str = "http://localhost:11434",
    plugin_raters: list[str] | None = None,
    plugin_rater_kwargs: dict[str, dict[str, object]] | None = None,
) -> pd.DataFrame:
    rated = rate_completeness(df)
    rated = rate_quality(rated, use_llm=use_llm, model=model, ollama_url=ollama_url)
    rated = rate_composite(rated)
    if plugin_raters:
        rated = apply_rate_plugins(
            rated,
            plugin_raters,
            plugin_kwargs=plugin_rater_kwargs,
        )
    return rated
