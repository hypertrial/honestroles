from __future__ import annotations

import pandas as pd

from honestroles.schema import CONTENT_HASH


def deduplicate(
    df: pd.DataFrame, *, subset: list[str] | None = None, keep: str = "first"
) -> pd.DataFrame:
    dedup_subset = subset or ([CONTENT_HASH] if CONTENT_HASH in df.columns else None)
    if not dedup_subset:
        return df
    return df.drop_duplicates(subset=dedup_subset, keep=keep).reset_index(drop=True)
