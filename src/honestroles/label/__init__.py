from __future__ import annotations

import pandas as pd

from honestroles.label.heuristic import (
    label_role_category,
    label_seniority,
    label_tech_stack,
)
from honestroles.label.llm import label_with_llm

__all__ = [
    "label_jobs",
    "label_seniority",
    "label_role_category",
    "label_tech_stack",
    "label_with_llm",
]


def label_jobs(df: pd.DataFrame, *, use_llm: bool = False, **kwargs: object) -> pd.DataFrame:
    labeled = label_seniority(df)
    labeled = label_role_category(labeled)
    labeled = label_tech_stack(labeled)
    if use_llm:
        labeled = label_with_llm(labeled, **kwargs)
    return labeled
