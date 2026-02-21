from honestroles.llm.prompts import (
    build_job_signal_prompt,
    build_label_prompt,
    build_quality_prompt,
)


def test_build_label_prompt_contains_labels_and_text() -> None:
    prompt = build_label_prompt("job description", ["engineering", "data"])
    assert "engineering" in prompt
    assert "data" in prompt
    assert "job description" in prompt


def test_build_quality_prompt_contains_text() -> None:
    prompt = build_quality_prompt("job description")
    assert "job description" in prompt
    assert "quality" in prompt.lower()


def test_build_job_signal_prompt_contains_schema_and_inputs() -> None:
    prompt = build_job_signal_prompt(title="Data Scientist", description="Need 1+ years Python.")
    assert "required_skills" in prompt
    assert "experience_years_min" in prompt
    assert "Data Scientist" in prompt
    assert "Need 1+ years Python." in prompt
