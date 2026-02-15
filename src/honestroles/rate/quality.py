from __future__ import annotations

import json
import logging
import math

import pandas as pd

from honestroles.llm.client import OllamaClient
from honestroles.llm.prompts import build_quality_prompt
from honestroles.schema import DESCRIPTION_TEXT

LOGGER = logging.getLogger(__name__)


def _parse_llm_score(value: object) -> float:
    if not isinstance(value, (int, float, str, bytes, bytearray)):
        raise ValueError("LLM score is not numeric.")
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        raise ValueError("LLM score is not numeric.")
    if not math.isfinite(parsed):
        raise ValueError("LLM score is not finite.")
    return max(0.0, min(1.0, parsed))


def rate_quality(
    df: pd.DataFrame,
    *,
    column: str = DESCRIPTION_TEXT,
    output_column: str = "quality_score",
    use_llm: bool = False,
    model: str = "llama3",
    ollama_url: str = "http://localhost:11434",
) -> pd.DataFrame:
    if column not in df.columns:
        return df
    result = df.copy()

    def heuristic(text: str | None) -> float:
        if text is None:
            return 0.0
        if isinstance(text, float) and pd.isna(text):
            return 0.0
        if not isinstance(text, str):
            return 0.0
        if not text:
            return 0.0
        length_score = min(1.0, len(text) / 2000)
        bullet_score = 0.2 if ("- " in text or "\n•" in text) else 0.0
        return min(1.0, length_score * 0.8 + bullet_score)

    result[output_column] = result[column].apply(heuristic)

    if not use_llm:
        return result

    client = OllamaClient(base_url=ollama_url)
    if not client.is_available():
        LOGGER.warning("Ollama is not available at %s.", ollama_url)
        return result

    llm_scores: list[float] = []
    llm_reasons: list[str] = []
    for text in result[column].fillna("").tolist():
        if not text.strip():
            llm_scores.append(0.0)
            llm_reasons.append("")
            continue
        prompt = build_quality_prompt(text)
        response = client.generate(prompt, model=model)
        try:
            payload = json.loads(response)
            score = _parse_llm_score(payload.get("score", 0.0))
            llm_scores.append(score)
            llm_reasons.append(str(payload.get("reason", "")))
        except (json.JSONDecodeError, ValueError, TypeError):
            LOGGER.warning("Failed to parse LLM response: %s", response)
            llm_scores.append(0.0)
            llm_reasons.append("")

    result["quality_score_llm"] = llm_scores
    result["quality_reason_llm"] = llm_reasons
    return result
