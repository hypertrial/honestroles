from __future__ import annotations

import pandas as pd

from honestroles.label.heuristic import (
    label_role_category,
    label_seniority,
    label_tech_stack,
)
from honestroles.label.llm import label_with_llm
from honestroles.plugins import apply_label_plugins
from honestroles.schema import DESCRIPTION_TEXT

__all__ = [
    "label_jobs",
    "label_seniority",
    "label_role_category",
    "label_tech_stack",
    "label_with_llm",
]


def label_jobs(
    df: pd.DataFrame,
    *,
    use_llm: bool = False,
    model: str = "llama3",
    labels: list[str] | None = None,
    column: str = DESCRIPTION_TEXT,
    ollama_url: str = "http://localhost:11434",
    batch_size: int = 8,
    plugin_labelers: list[str] | None = None,
    plugin_labeler_kwargs: dict[str, dict[str, object]] | None = None,
) -> pd.DataFrame:
    labeled = label_seniority(df)
    labeled = label_role_category(labeled)
    labeled = label_tech_stack(labeled)
    if use_llm:
        labeled = label_with_llm(
            labeled,
            model=model,
            labels=labels,
            column=column,
            ollama_url=ollama_url,
            batch_size=batch_size,
        )
    if plugin_labelers:
        labeled = apply_label_plugins(
            labeled, plugin_labelers, plugin_kwargs=plugin_labeler_kwargs
        )
    return labeled
