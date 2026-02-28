from __future__ import annotations

from pathlib import Path

import polars as pl

from honestroles.config.models import CleanStageOptions
from honestroles.domain import JobDataset
from honestroles.plugins.types import RuntimeExecutionContext
from honestroles.stages import clean_stage


def test_clean_stage_preserves_existing_text_when_html_missing() -> None:
    frame = pl.DataFrame(
        {
            "title": ["Role"],
            "company": ["Co"],
            "description_text": ["already clean text"],
            "description_html": [None],
            "remote": [True],
            "location": ["Remote"],
            "skills": [["python", "sql"]],
            "salary_min": [100.0],
            "salary_max": [200.0],
            "apply_url": ["https://x"],
            "posted_at": ["2026-01-01"],
            "id": ["1"],
        }
    )

    cleaned = clean_stage(
        JobDataset.from_polars(frame),
        CleanStageOptions(strip_html=True),
        RuntimeExecutionContext(
            pipeline_config_path=Path("pipeline.toml"),
            plugin_manifest_path=None,
            stage_options={},
        ),
    )
    assert cleaned.to_polars()["description_text"].to_list() == ["already clean text"]
