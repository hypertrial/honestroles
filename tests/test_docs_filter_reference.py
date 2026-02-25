from __future__ import annotations

from inspect import signature
from pathlib import Path

from honestroles.filter import filter_jobs


ADVANCED_PARAMS = [
    "employment_types",
    "entry_level_only",
    "max_experience_years",
    "needs_visa_sponsorship",
    "include_unknown_visa",
    "max_application_friction",
    "active_within_days",
    "min_active_likelihood",
]


def test_filter_reference_mentions_advanced_filter_params() -> None:
    sig = signature(filter_jobs)
    for param in ADVANCED_PARAMS:
        assert param in sig.parameters

    docs_path = Path(__file__).resolve().parents[1] / "docs" / "reference" / "filter.md"
    docs_text = docs_path.read_text(encoding="utf-8")

    for param in ADVANCED_PARAMS:
        assert param in docs_text

    assert "mkdocstrings" in docs_text
