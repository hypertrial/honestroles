from __future__ import annotations

import pandas as pd
import pytest
from hypothesis import given

import honestroles.rate.quality as rate_quality_module
from honestroles.rate.quality import rate_quality

from .strategies import TEXT_VALUES, dataframe_for_columns


@pytest.mark.fuzz
@given(df=dataframe_for_columns({"description_text": TEXT_VALUES}, max_rows=12))
def test_fuzz_rate_quality_heuristic_bounds(df: pd.DataFrame) -> None:
    result = rate_quality(df, use_llm=False)
    assert len(result) == len(df)
    numeric = pd.to_numeric(result["quality_score"], errors="coerce").dropna()
    assert ((numeric >= 0.0) & (numeric <= 1.0)).all()


@pytest.mark.fuzz
@given(
    df=dataframe_for_columns({"description_text": TEXT_VALUES}, max_rows=8),
    llm_response=TEXT_VALUES,
)
def test_fuzz_rate_quality_llm_fallback_is_safe(
    df: pd.DataFrame,
    llm_response: str,
) -> None:
    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(rate_quality_module.OllamaClient, "is_available", lambda self: True)
        monkeypatch.setattr(
            rate_quality_module.OllamaClient,
            "generate",
            lambda self, prompt, model: llm_response,
        )
        result = rate_quality(df, use_llm=True)
    assert len(result) == len(df)
    assert "quality_score" in result.columns
    assert "quality_score_llm" in result.columns

    numeric = pd.to_numeric(result["quality_score_llm"], errors="coerce").dropna()
    assert ((numeric >= 0.0) & (numeric <= 1.0)).all()
