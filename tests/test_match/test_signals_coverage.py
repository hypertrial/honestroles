from __future__ import annotations

import json

import pandas as pd
import pytest

import honestroles.match.signals as signals_module
from honestroles.match.models import DEFAULT_RESULT_COLUMNS


def test_signal_helpers_text_and_list_variants() -> None:
    assert signals_module._list_from_value(None) == []
    assert signals_module._list_from_value(float("nan")) == []
    assert signals_module._list_from_value("   ") == []
    assert signals_module._list_from_value("python") == ["python"]
    assert signals_module._list_from_value(42) == ["42"]


def test_extract_years_skips_unreasonably_high_values() -> None:
    assert signals_module._extract_years("Need 25-30 years and 40 years experience.") == (
        None,
        None,
    )


def test_extract_skills_unhinted_terms_default_to_required() -> None:
    required, preferred = signals_module._extract_skills("Python and SQL used daily.", [])
    assert "python" in required
    assert "sql" in required
    assert preferred == []


def test_extract_skills_required_and_preferred_hints_and_series_edges(monkeypatch) -> None:
    required, preferred = signals_module._extract_skills(
        "Required: Python. Preferred: SQL.",
        [],
    )
    assert "python" in required
    assert "sql" in preferred

    empty_series = signals_module._extract_required_skills_series(pd.Series([], dtype="string"))
    assert empty_series.empty

    monkeypatch.setattr(signals_module, "_canonical_skill_from_match", lambda _: None)
    dropped = signals_module._extract_required_skills_series(pd.Series(["python"], dtype="string"))
    assert dropped.loc[0] == []


def test_signal_boolean_and_numeric_parsers_edge_cases() -> None:
    assert signals_module._entry_level_signal("General role", 3) is False
    assert signals_module._role_clarity_score("") == 0.0
    assert signals_module._application_friction_score(
        "cover letter required", "https://company.workday.com/job"
    ) < 0.8

    assert signals_module._parse_optional_bool("true") is True
    assert signals_module._parse_optional_bool("false") is False
    assert signals_module._parse_optional_bool("unknown") is None

    assert signals_module._parse_optional_int(True) is None
    assert signals_module._parse_optional_int(None) is None
    assert signals_module._parse_optional_int(float("inf")) is None
    assert signals_module._parse_optional_int("") is None
    assert signals_module._parse_optional_int("abc") is None
    assert signals_module._parse_optional_int("-1") is None
    assert signals_module._parse_optional_int("99") is None
    assert signals_module._parse_optional_int(object()) is None

    assert signals_module._parse_score(None) is None
    assert signals_module._parse_score("nope") is None
    assert signals_module._parse_score(object()) is None
    assert signals_module._parse_score(float("inf")) is None
    assert signals_module._parse_skill_list("python") == []


def test_extract_job_signals_warns_when_ollama_unavailable(
    monkeypatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    class _UnavailableClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def is_available(self) -> bool:
            return False

    monkeypatch.setattr(signals_module, "OllamaClient", _UnavailableClient)
    df = pd.DataFrame([{"title": "Data Scientist", "description_text": "Role text"}])
    enriched = signals_module.extract_job_signals(df, use_llm=True)
    assert DEFAULT_RESULT_COLUMNS.signal_source in enriched.columns
    assert any("Ollama is not available" in record.message for record in caplog.records)


def test_extract_job_signals_llm_visa_update_and_parse_failure(
    monkeypatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    class _FakeClient:
        def __init__(self, *args, **kwargs) -> None:
            self._calls = 0

        def is_available(self) -> bool:
            return True

        def generate(self, prompt: str, *, model: str) -> str:
            responses = [
                json.dumps(
                    {
                        "required_skills": ["python"],
                        "preferred_skills": [],
                        "experience_years_min": 1,
                        "experience_years_max": 2,
                        "entry_level_likely": True,
                        "visa_sponsorship_signal": True,
                        "application_friction_score": 0.9,
                        "role_clarity_score": 0.8,
                        "confidence": 0.9,
                        "reason": "ok",
                    }
                ),
                "not-json",
            ]
            response = responses[self._calls]
            self._calls += 1
            return response

    monkeypatch.setattr(signals_module, "OllamaClient", _FakeClient)
    df = pd.DataFrame(
        [
            {"title": "Role one", "description_text": "Text one"},
            {"title": "Role two", "description_text": "Text two"},
        ]
    )
    enriched = signals_module.extract_job_signals(
        df,
        use_llm=True,
        llm_min_confidence=1.0,
    )
    columns = DEFAULT_RESULT_COLUMNS
    assert enriched.loc[0, columns.visa_sponsorship_signal] is True
    assert any("Failed to parse Ollama signal response" in record.message for record in caplog.records)


def test_extract_job_signals_backfills_missing_year_bounds(monkeypatch) -> None:
    calls = {"count": 0}

    def _fake_extract_years(text: str) -> tuple[int | None, int | None]:
        del text
        calls["count"] += 1
        if calls["count"] == 1:
            return None, 2
        return 2, None

    monkeypatch.setattr(signals_module, "_extract_years", _fake_extract_years)
    df = pd.DataFrame(
        [
            {"title": "Role one", "description_text": "A"},
            {"title": "Role two", "description_text": "B"},
        ]
    )
    enriched = signals_module.extract_job_signals(df, use_llm=False)
    columns = DEFAULT_RESULT_COLUMNS
    assert enriched.loc[0, columns.experience_years_min] == 2
    assert enriched.loc[0, columns.experience_years_max] == 2
    assert enriched.loc[1, columns.experience_years_min] == 2
    assert enriched.loc[1, columns.experience_years_max] == 2
