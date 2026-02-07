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
