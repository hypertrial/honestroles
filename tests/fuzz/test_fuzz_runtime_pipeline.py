from __future__ import annotations

import tempfile
from pathlib import Path

import pytest
from hypothesis import given, settings

import polars as pl

from honestroles.runtime import HonestRolesRuntime
from tests.fuzz.invariants import assert_dataframe, assert_score_bounds
from tests.fuzz.strategies import rows_strategy


def _coerce_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    coerced: list[dict[str, object]] = []
    for row in rows:
        item = dict(row)
        skills = item.get("skills")
        if isinstance(skills, list):
            item["skills"] = ",".join(str(part) for part in skills)
        coerced.append(item)
    return coerced


@pytest.mark.fuzz
@settings(max_examples=60)
@given(rows=rows_strategy)
def test_runtime_pipeline_no_crash_on_mixed_rows(rows):
    tmp_path = Path(tempfile.mkdtemp(prefix="honestroles-fuzz-"))
    rows = _coerce_rows(rows)
    if rows:
        frame = pl.from_dicts(rows, strict=False, infer_schema_length=None)
    else:
        frame = pl.DataFrame({"id": []}, schema={"id": pl.String})
    parquet_path = tmp_path / "fuzz_jobs.parquet"
    frame.write_parquet(parquet_path)

    pipeline_text = f"""
[input]
kind = "parquet"
path = "{parquet_path}"

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
top_k = 20

[runtime]
fail_fast = true
random_seed = 0
""".strip()
    pipeline_path = tmp_path / "pipeline.toml"
    pipeline_path.write_text(pipeline_text, encoding="utf-8")

    runtime = HonestRolesRuntime.from_configs(pipeline_path)
    result = runtime.run()

    assert_dataframe(result.dataframe)
    assert_score_bounds(result.dataframe, ["fit_score", "rate_composite", "rate_quality"])
    assert result.diagnostics["final_rows"] == result.dataframe.height
