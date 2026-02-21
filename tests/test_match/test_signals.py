from __future__ import annotations

import json

import pandas as pd

from honestroles.match import extract_job_signals
from honestroles.match.models import DEFAULT_RESULT_COLUMNS


def test_extract_job_signals_heuristic_fields() -> None:
    df = pd.DataFrame(
        [
            {
                "title": "Entry Level Data Scientist",
                "description_text": (
                    "New graduate role. 0-2 years experience required. "
                    "Must have Python and SQL. Nice to have Spark. "
                    "Visa sponsorship available."
                ),
                "apply_url": "https://jobs.example.com/greenhouse/1",
                "skills": ["Python"],
                "remote_flag": True,
            }
        ]
    )

    enriched = extract_job_signals(df, use_llm=False)
    columns = DEFAULT_RESULT_COLUMNS

    assert columns.required_skills_extracted in enriched.columns
    assert columns.entry_level_likely in enriched.columns
    assert columns.application_friction_score in enriched.columns

    row = enriched.iloc[0]
    required = row[columns.required_skills_extracted]
    assert "python" in required
    assert "sql" in required
    assert row[columns.entry_level_likely] is True
    assert row[columns.visa_sponsorship_signal] is True
    assert row[columns.experience_years_min] == 0
    assert row[columns.experience_years_max] == 2


def test_extract_job_signals_ollama_fallback_enrichment(monkeypatch) -> None:
    df = pd.DataFrame(
        [
            {
                "title": "Data Role",
                "description_text": "Help data team.",
                "apply_url": "https://example.com/workday/1",
                "skills": [],
            }
        ]
    )

    class _FakeClient:
        def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
            pass

        def is_available(self) -> bool:
            return True

        def generate(self, prompt: str, *, model: str) -> str:
            payload = {
                "required_skills": ["python", "sql"],
                "preferred_skills": ["spark"],
                "experience_years_min": 1,
                "experience_years_max": 2,
                "entry_level_likely": True,
                "visa_sponsorship_signal": None,
                "application_friction_score": 0.7,
                "role_clarity_score": 0.8,
                "confidence": 0.9,
                "reason": "parsed",
            }
            return json.dumps(payload)

    monkeypatch.setattr("honestroles.match.signals.OllamaClient", _FakeClient)
    enriched = extract_job_signals(df, use_llm=True, llm_min_confidence=0.9)
    columns = DEFAULT_RESULT_COLUMNS
    row = enriched.iloc[0]

    assert row[columns.signal_source] == "heuristic+ollama"
    assert row[columns.experience_years_min] == 1
    assert "spark" in row[columns.preferred_skills_extracted]
    assert row[columns.role_clarity_score] == 0.8


def test_extract_job_signals_does_not_false_match_single_letter_r() -> None:
    df = pd.DataFrame(
        [
            {
                "title": "Office Assistant",
                "description_text": "Coordinate meetings and reports across teams.",
                "apply_url": "https://example.com",
            }
        ]
    )

    enriched = extract_job_signals(df, use_llm=False)
    columns = DEFAULT_RESULT_COLUMNS
    assert "r" not in enriched.iloc[0][columns.required_skills_extracted]
