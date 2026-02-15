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
    available_weights = {column: weight for column, weight in weights.items() if column in result.columns}
    total_weight = sum(available_weights.values())
    if total_weight == 0:
        return result

    score = pd.Series(0.0, index=result.index)
    for column, weight in available_weights.items():
        score += result[column].fillna(0) * weight
    result[output_column] = score / total_weight
    return result
