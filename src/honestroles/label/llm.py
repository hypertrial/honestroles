from __future__ import annotations

import json
import logging

import pandas as pd

from honestroles.llm.client import OllamaClient
from honestroles.llm.prompts import build_label_prompt
from honestroles.schema import DESCRIPTION_TEXT

LOGGER = logging.getLogger(__name__)

DEFAULT_LABELS = [
    "engineering",
    "data",
    "design",
    "product",
    "marketing",
    "sales",
    "operations",
    "finance",
    "hr",
    "legal",
    "support",
]


def label_with_llm(
    df: pd.DataFrame,
    *,
    model: str = "llama3",
    labels: list[str] | None = None,
    column: str = DESCRIPTION_TEXT,
    ollama_url: str = "http://localhost:11434",
    batch_size: int = 8,
) -> pd.DataFrame:
    if column not in df.columns:
        LOGGER.warning("Column %s not found for LLM labeling.", column)
        return df
    client = OllamaClient(base_url=ollama_url)
    if not client.is_available():
        LOGGER.warning("Ollama is not available at %s.", ollama_url)
        return df

    result = df.copy()
    labels = labels or DEFAULT_LABELS
    llm_labels: list[list[str]] = []

    texts = result[column].fillna("").tolist()
    for idx in range(0, len(texts), batch_size):
        batch = texts[idx : idx + batch_size]
        for text in batch:
            if not text.strip():
                llm_labels.append([])
                continue
            prompt = build_label_prompt(text, labels)
            response = client.generate(prompt, model=model)
            try:
                payload = json.loads(response)
                row_labels = payload.get("labels", [])
                if not isinstance(row_labels, list):
                    row_labels = []
                llm_labels.append([str(label) for label in row_labels])
            except json.JSONDecodeError:
                LOGGER.warning("Failed to parse LLM response: %s", response)
                llm_labels.append([])

    result["llm_labels"] = llm_labels
    return result
