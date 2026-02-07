from __future__ import annotations

import pandas as pd


def rate_composite(
    df: pd.DataFrame,
    *,
    completeness_column: str = "completeness_score",
    quality_column: str = "quality_score",
    output_column: str = "rating",
    weights: dict[str, float] | None = None,
) -> pd.DataFrame:
    result = df.copy()
    weights = weights or {completeness_column: 0.5, quality_column: 0.5}
    total_weight = sum(weights.values())
    if total_weight == 0:
        return result

    score = 0.0
    for column, weight in weights.items():
        if column not in result.columns:
            continue
        score += result[column].fillna(0) * weight
    result[output_column] = score / total_weight
    return result
