from honestroles.llm.prompts import build_label_prompt, build_quality_prompt


def test_build_label_prompt_contains_labels_and_text() -> None:
    prompt = build_label_prompt("job description", ["engineering", "data"])
    assert "engineering" in prompt
    assert "data" in prompt
    assert "job description" in prompt


def test_build_quality_prompt_contains_text() -> None:
    prompt = build_quality_prompt("job description")
    assert "job description" in prompt
    assert "quality" in prompt.lower()
