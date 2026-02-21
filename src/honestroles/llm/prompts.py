from __future__ import annotations

import json


def build_label_prompt(text: str, labels: list[str]) -> str:
    labels_json = json.dumps(labels)
    return (
        "You are labeling job descriptions. "
        "Return a JSON object with a single key 'labels' containing a list of labels "
        f"from this allowed set: {labels_json}. "
        "Only include labels that apply. "
        "Return JSON only, no extra text.\n\n"
        f"Job description:\n{text}"
    )


def build_quality_prompt(text: str) -> str:
    return (
        "Rate the quality of this job description on a 0-1 scale. "
        "Return a JSON object with keys 'score' (float) and 'reason' (short string). "
        "Return JSON only, no extra text.\n\n"
        f"Job description:\n{text}"
    )


def build_job_signal_prompt(*, title: str, description: str) -> str:
    return (
        "Extract structured job-matching signals for an early-career data candidate. "
        "Return valid JSON only with this exact schema: "
        "{"
        '"required_skills": ["..."], '
        '"preferred_skills": ["..."], '
        '"experience_years_min": number|null, '
        '"experience_years_max": number|null, '
        '"entry_level_likely": true|false|null, '
        '"visa_sponsorship_signal": true|false|null, '
        '"application_friction_score": number 0..1, '
        '"role_clarity_score": number 0..1, '
        '"confidence": number 0..1, '
        '"reason": "short text"'
        "}. "
        "Do not include markdown.\n\n"
        f"Title:\n{title}\n\nDescription:\n{description}"
    )
