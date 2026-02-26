from __future__ import annotations

import os
from pathlib import Path

import polars as pl
import pytest
from hypothesis import HealthCheck, settings


settings.register_profile(
    "ci_smoke",
    max_examples=30,
    derandomize=True,
    deadline=None,
    suppress_health_check=(HealthCheck.too_slow,),
)
settings.register_profile(
    "nightly_deep",
    max_examples=300,
    derandomize=True,
    deadline=None,
    suppress_health_check=(HealthCheck.too_slow,),
)
settings.load_profile(os.getenv("HYPOTHESIS_PROFILE", "ci_smoke"))


@pytest.fixture()
def sample_jobs_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "id": ["1", "2", "3"],
            "title": ["Data Engineer", "Senior ML Engineer", "Intern Analyst"],
            "company": ["A", "B", "C"],
            "location": ["Remote", "NYC", "Remote"],
            "remote": ["true", "false", "1"],
            "description_text": [
                "Python SQL data pipelines",
                "Build ML systems with Python and AWS",
                "Excel and reporting",
            ],
            "description_html": ["<p>Python SQL</p>", "<b>ML</b>", "<i>intern</i>"],
            "skills": ["python,sql", "python,aws", None],
            "salary_min": [120000, 180000, None],
            "salary_max": [160000, 220000, None],
            "apply_url": ["https://x/1", "https://x/2", "https://x/3"],
            "posted_at": ["2026-01-01", "2026-01-02", "2026-01-03"],
        }
    )


@pytest.fixture()
def sample_parquet(sample_jobs_df: pl.DataFrame, tmp_path: Path) -> Path:
    path = tmp_path / "jobs.parquet"
    sample_jobs_df.write_parquet(path)
    return path


@pytest.fixture()
def pipeline_config_path(sample_parquet: Path, tmp_path: Path) -> Path:
    output_path = tmp_path / "output.parquet"
    config = f"""
[input]
kind = "parquet"
path = "{sample_parquet}"

[output]
path = "{output_path}"

[stages.clean]
enabled = true
drop_null_titles = true
strip_html = true

[stages.filter]
enabled = true
remote_only = false
min_salary = 100000.0
required_keywords = ["python"]

[stages.label]
enabled = true

[stages.rate]
enabled = true
completeness_weight = 0.5
quality_weight = 0.5

[stages.match]
enabled = true
top_k = 10

[runtime]
fail_fast = true
random_seed = 42
""".strip()
    path = tmp_path / "pipeline.toml"
    path.write_text(config, encoding="utf-8")
    return path


@pytest.fixture()
def plugin_manifest_path(tmp_path: Path) -> Path:
    text = """
[[plugins]]
name = "high_quality_gate"
kind = "filter"
callable = "tests.plugins.fixture_plugins:filter_min_quality"
enabled = true
order = 10

[plugins.settings]
min_quality = 0.0

[[plugins]]
name = "label_note"
kind = "label"
callable = "tests.plugins.fixture_plugins:label_note"
enabled = true
order = 5

[[plugins]]
name = "rate_bonus"
kind = "rate"
callable = "tests.plugins.fixture_plugins:rate_bonus"
enabled = true
order = 1

[plugins.settings]
bonus = 0.05
""".strip()
    path = tmp_path / "plugins.toml"
    path.write_text(text, encoding="utf-8")
    return path


@pytest.fixture()
def fail_plugin_manifest_path(tmp_path: Path) -> Path:
    text = """
[[plugins]]
name = "failing_filter"
kind = "filter"
callable = "tests.plugins.fixture_plugins:fail_filter"
enabled = true
order = 1
""".strip()
    path = tmp_path / "plugins_fail.toml"
    path.write_text(text, encoding="utf-8")
    return path


@pytest.fixture()
def pipeline_config_non_fail_fast_path(sample_parquet: Path, tmp_path: Path) -> Path:
    config = f"""
[input]
kind = "parquet"
path = "{sample_parquet}"

[stages.clean]
enabled = true

[stages.filter]
enabled = true
remote_only = false

[stages.label]
enabled = true

[stages.rate]
enabled = true

[stages.match]
enabled = true
top_k = 5

[runtime]
fail_fast = false
random_seed = 7
""".strip()
    path = tmp_path / "pipeline_non_fail_fast.toml"
    path.write_text(config, encoding="utf-8")
    return path
