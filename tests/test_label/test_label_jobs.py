import pandas as pd

import honestroles.label as label_module
from honestroles.label import label_jobs


def test_label_jobs_integration(sample_df: pd.DataFrame) -> None:
    labeled = label_jobs(sample_df, use_llm=False)
    assert "seniority" in labeled.columns
    assert "role_category" in labeled.columns
    assert "tech_stack" in labeled.columns


def test_label_jobs_empty_dataframe(empty_df: pd.DataFrame) -> None:
    labeled = label_jobs(empty_df, use_llm=False)
    assert labeled.empty


def test_label_jobs_use_llm_path(sample_df: pd.DataFrame, monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_label_with_llm(
        df: pd.DataFrame,
        *,
        model: str,
        labels: list[str] | None,
        column: str,
        ollama_url: str,
        batch_size: int,
    ) -> pd.DataFrame:
        captured.update(
            {
                "model": model,
                "labels": labels,
                "column": column,
                "ollama_url": ollama_url,
                "batch_size": batch_size,
            }
        )
        result = df.copy()
        result["llm_labels"] = [["engineering"]] * len(result)
        return result

    monkeypatch.setattr(label_module, "label_with_llm", _fake_label_with_llm)
    labeled = label_jobs(
        sample_df,
        use_llm=True,
        model="llama3.2",
        labels=["engineering"],
        column="description_text",
        ollama_url="http://localhost:11434",
        batch_size=2,
    )
    assert "llm_labels" in labeled.columns
    assert captured == {
        "model": "llama3.2",
        "labels": ["engineering"],
        "column": "description_text",
        "ollama_url": "http://localhost:11434",
        "batch_size": 2,
    }
