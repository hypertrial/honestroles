from __future__ import annotations

import pandas as pd
import pytest
from hypothesis import given

import honestroles.label.llm as label_llm
from honestroles.label.llm import label_with_llm

from .strategies import TEXT_VALUES, dataframe_for_columns


@pytest.mark.fuzz
@given(
    df=dataframe_for_columns({"description_text": TEXT_VALUES}, max_rows=8),
    llm_response=TEXT_VALUES,
)
def test_fuzz_label_with_llm_graceful_json_fallback(
    df: pd.DataFrame,
    llm_response: str,
) -> None:
    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(label_llm.OllamaClient, "is_available", lambda self: True)
        monkeypatch.setattr(
            label_llm.OllamaClient,
            "generate",
            lambda self, prompt, model: llm_response,
        )
        result = label_with_llm(df, labels=["engineering", "data"], batch_size=3)
    assert len(result) == len(df)
    assert "llm_labels" in result.columns
    for value in result["llm_labels"].tolist():
        assert isinstance(value, list)
        assert all(isinstance(item, str) for item in value)


@pytest.mark.fuzz
@given(df=dataframe_for_columns({"description_text": TEXT_VALUES}, max_rows=8))
def test_fuzz_label_with_llm_unavailable_returns_input(
    df: pd.DataFrame,
) -> None:
    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(label_llm.OllamaClient, "is_available", lambda self: False)
        result = label_with_llm(df)
    assert result.equals(df)
