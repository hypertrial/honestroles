from __future__ import annotations

from collections.abc import Callable

import pandas as pd

Predicate = Callable[..., pd.Series]


class FilterChain:
    def __init__(self, *, mode: str = "and") -> None:
        if mode not in {"and", "or"}:
            raise ValueError("mode must be 'and' or 'or'")
        self._mode = mode
        self._steps: list[tuple[Predicate, dict[str, object]]] = []

    def add(self, predicate: Predicate, **kwargs: object) -> "FilterChain":
        self._steps.append((predicate, kwargs))
        return self

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self._steps:
            return df
        masks = [predicate(df, **kwargs) for predicate, kwargs in self._steps]
        if self._mode == "and":
            mask = masks[0]
            for next_mask in masks[1:]:
                mask &= next_mask
        else:
            mask = masks[0]
            for next_mask in masks[1:]:
                mask |= next_mask
        return df[mask].reset_index(drop=True)
