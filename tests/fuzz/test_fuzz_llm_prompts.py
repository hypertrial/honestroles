from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st

from honestroles.llm.prompts import build_job_signal_prompt, build_label_prompt, build_quality_prompt

from .strategies import TEXT_VALUES


@pytest.mark.fuzz
@given(text=TEXT_VALUES, labels=st.lists(TEXT_VALUES, min_size=0, max_size=8))
def test_fuzz_build_label_prompt_handles_arbitrary_text(text: str, labels: list[str]) -> None:
    prompt = build_label_prompt(text, labels)
    assert isinstance(prompt, str)
    assert "Job description:" in prompt
    assert text in prompt


@pytest.mark.fuzz
@given(text=TEXT_VALUES)
def test_fuzz_build_quality_prompt_handles_arbitrary_text(text: str) -> None:
    prompt = build_quality_prompt(text)
    assert isinstance(prompt, str)
    assert "Rate the quality" in prompt
    assert text in prompt


@pytest.mark.fuzz
@given(title=TEXT_VALUES, description=TEXT_VALUES)
def test_fuzz_build_job_signal_prompt_handles_unicode(title: str, description: str) -> None:
    prompt = build_job_signal_prompt(title=title, description=description)
    assert isinstance(prompt, str)
    assert "Extract structured job-matching signals" in prompt
    assert title in prompt
    assert description in prompt
