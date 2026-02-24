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
    values = result[column]
    text = values.where(values.map(lambda value: isinstance(value, str)), "")
    text = text.fillna("").astype("string")

    length_score = (
        text.str.len().fillna(0).clip(upper=2000).astype("float64") / 2000.0
    )
    bullet_score = (
        text.str.contains(r"- |\n•", regex=True).fillna(False).astype("float64") * 0.2
    )
    result[output_column] = (length_score * 0.8 + bullet_score).clip(upper=1.0)

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
