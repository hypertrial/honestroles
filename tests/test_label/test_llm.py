import json

import pandas as pd

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
