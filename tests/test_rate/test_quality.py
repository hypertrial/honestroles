import json

import pandas as pd
import pytest

import honestroles.rate.quality as quality_module
from honestroles.rate.quality import rate_quality


def test_rate_quality(sample_df: pd.DataFrame) -> None:
    rated = rate_quality(sample_df)
    assert "quality_score" in rated.columns
    assert rated["quality_score"].max() <= 1.0


def test_rate_quality_empty_description() -> None:
    df = pd.DataFrame({"description_text": [None, ""]})
    rated = rate_quality(df)
    assert rated["quality_score"].tolist() == [0.0, 0.0]


def test_rate_quality_long_description() -> None:
    df = pd.DataFrame({"description_text": ["x" * 5000]})
    rated = rate_quality(df)
    assert rated.loc[0, "quality_score"] <= 1.0
    assert rated.loc[0, "quality_score"] > 0.7


def test_rate_quality_bullet_bonus() -> None:
    df = pd.DataFrame({"description_text": ["- item one\n- item two"]})
    rated = rate_quality(df)
    assert rated.loc[0, "quality_score"] > 0.0


def test_rate_quality_missing_column(sample_df: pd.DataFrame) -> None:
    df = sample_df.drop(columns=["description_text"])
    rated = rate_quality(df)
    assert rated.equals(df)


def test_rate_quality_heuristic_handles_nan_and_non_strings() -> None:
    df = pd.DataFrame({"description_text": [float("nan"), 123, ["x"]]})
    rated = rate_quality(df)
    assert rated["quality_score"].tolist() == [0.0, 0.0, 0.0]


def test_rate_quality_llm_unavailable_uses_heuristic_only(
    sample_df: pd.DataFrame,
    monkeypatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    class _UnavailableClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def is_available(self) -> bool:
            return False

    monkeypatch.setattr(quality_module, "OllamaClient", _UnavailableClient)
    rated = rate_quality(sample_df, use_llm=True)
    assert "quality_score" in rated.columns
    assert "quality_score_llm" not in rated.columns
    assert any("Ollama is not available" in record.message for record in caplog.records)


def test_rate_quality_llm_parses_scores_and_handles_bad_payloads(
    monkeypatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    df = pd.DataFrame({"description_text": ["good text", "bad type", "bad json", "   "]})

    class _FakeClient:
        def __init__(self, *args, **kwargs) -> None:
            self._calls = 0

        def is_available(self) -> bool:
            return True

        def generate(
            self, prompt: str, *, model: str, temperature: float = 0.1, max_tokens=None
        ) -> str:
            responses = [
                json.dumps({"score": 0.8, "reason": "strong structure"}),
                json.dumps({"score": "bad", "reason": "not numeric"}),
                "not-json",
            ]
            response = responses[self._calls]
            self._calls += 1
            return response

    monkeypatch.setattr(quality_module, "OllamaClient", _FakeClient)
    rated = rate_quality(df, use_llm=True)
    assert rated["quality_score_llm"].tolist() == [0.8, 0.0, 0.0, 0.0]
    assert rated["quality_reason_llm"].tolist() == [
        "strong structure",
        "",
        "",
        "",
    ]
    assert any("Failed to parse LLM response" in record.message for record in caplog.records)


def test_rate_quality_llm_clamps_out_of_range_scores(monkeypatch) -> None:
    df = pd.DataFrame({"description_text": ["high", "low"]})

    class _FakeClient:
        def __init__(self, *args, **kwargs) -> None:
            self._calls = 0

        def is_available(self) -> bool:
            return True

        def generate(
            self, prompt: str, *, model: str, temperature: float = 0.1, max_tokens=None
        ) -> str:
            responses = [
                json.dumps({"score": 1.7, "reason": "too high"}),
                json.dumps({"score": -0.2, "reason": "too low"}),
            ]
            response = responses[self._calls]
            self._calls += 1
            return response

    monkeypatch.setattr(quality_module, "OllamaClient", _FakeClient)
    rated = rate_quality(df, use_llm=True)
    assert rated["quality_score_llm"].tolist() == [1.0, 0.0]


def test_rate_quality_llm_handles_non_finite_scores(monkeypatch) -> None:
    df = pd.DataFrame({"description_text": ["nan score", "inf score"]})

    class _FakeClient:
        def __init__(self, *args, **kwargs) -> None:
            self._calls = 0

        def is_available(self) -> bool:
            return True

        def generate(
            self, prompt: str, *, model: str, temperature: float = 0.1, max_tokens=None
        ) -> str:
            responses = [
                json.dumps({"score": "NaN", "reason": "not finite"}),
                json.dumps({"score": "Infinity", "reason": "not finite"}),
            ]
            response = responses[self._calls]
            self._calls += 1
            return response

    monkeypatch.setattr(quality_module, "OllamaClient", _FakeClient)
    rated = rate_quality(df, use_llm=True)
    assert rated["quality_score_llm"].tolist() == [0.0, 0.0]
    assert rated["quality_reason_llm"].tolist() == ["", ""]


def test_parse_llm_score_rejects_non_numeric_types() -> None:
    with pytest.raises(ValueError, match="not numeric"):
        quality_module._parse_llm_score({})  # type: ignore[arg-type]


def test_rate_quality_heuristic_none_value_branch() -> None:
    df = pd.DataFrame({"description_text": pd.Series([None], dtype="object")})
    rated = rate_quality(df)
    assert rated["quality_score"].tolist() == [0.0]


def test_rate_quality_empty_string_dtype_outputs_float_column() -> None:
    df = pd.DataFrame({"description_text": pd.Series([], dtype="str")})
    rated = rate_quality(df)
    assert rated["quality_score"].dtype == "float64"
    assert rated.empty
