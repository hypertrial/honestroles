import json

import pandas as pd
import pytest

import honestroles.label.llm as label_llm


class _FakeClient:
    def __init__(self, *args, **kwargs) -> None:
        pass

    def is_available(self) -> bool:
        return True

    def generate(self, prompt: str, *, model: str, temperature: float = 0.1, max_tokens=None) -> str:
        return json.dumps({"labels": ["engineering"]})


def test_label_with_llm(sample_df: pd.DataFrame, monkeypatch) -> None:
    monkeypatch.setattr(label_llm, "OllamaClient", _FakeClient)
    labeled = label_llm.label_with_llm(sample_df, model="llama3")
    assert labeled.loc[0, "llm_labels"] == ["engineering"]


class _UnavailableClient:
    def __init__(self, *args, **kwargs) -> None:
        pass

    def is_available(self) -> bool:
        return False


def test_label_with_llm_missing_column(sample_df: pd.DataFrame, caplog: pytest.LogCaptureFixture) -> None:
    df = sample_df.drop(columns=["description_text"])
    labeled = label_llm.label_with_llm(df)
    assert labeled.equals(df)
    assert any("Column" in record.message for record in caplog.records)


def test_label_with_llm_unavailable(sample_df: pd.DataFrame, monkeypatch, caplog: pytest.LogCaptureFixture) -> None:
    monkeypatch.setattr(label_llm, "OllamaClient", _UnavailableClient)
    labeled = label_llm.label_with_llm(sample_df)
    assert labeled.equals(sample_df)
    assert any("Ollama is not available" in record.message for record in caplog.records)


def test_label_with_llm_empty_descriptions(monkeypatch) -> None:
    monkeypatch.setattr(label_llm, "OllamaClient", _FakeClient)
    df = pd.DataFrame({"description_text": ["", "  "]})
    labeled = label_llm.label_with_llm(df)
    assert labeled["llm_labels"].tolist() == [[], []]


class _BadJsonClient:
    def __init__(self, *args, **kwargs) -> None:
        pass

    def is_available(self) -> bool:
        return True

    def generate(self, prompt: str, *, model: str, temperature: float = 0.1, max_tokens=None) -> str:
        return "not json"


def test_label_with_llm_bad_json(monkeypatch, caplog: pytest.LogCaptureFixture) -> None:
    monkeypatch.setattr(label_llm, "OllamaClient", _BadJsonClient)
    df = pd.DataFrame({"description_text": ["test"]})
    labeled = label_llm.label_with_llm(df)
    assert labeled["llm_labels"].tolist() == [[]]
    assert any("Failed to parse LLM response" in record.message for record in caplog.records)


class _NonListLabelsClient:
    def __init__(self, *args, **kwargs) -> None:
        pass

    def is_available(self) -> bool:
        return True

    def generate(self, prompt: str, *, model: str, temperature: float = 0.1, max_tokens=None) -> str:
        return json.dumps({"labels": "engineering"})


def test_label_with_llm_non_list_labels_fallback(monkeypatch) -> None:
    monkeypatch.setattr(label_llm, "OllamaClient", _NonListLabelsClient)
    df = pd.DataFrame({"description_text": ["test"]})
    labeled = label_llm.label_with_llm(df)
    assert labeled["llm_labels"].tolist() == [[]]
