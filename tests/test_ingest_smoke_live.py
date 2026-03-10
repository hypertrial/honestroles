from __future__ import annotations

import json
import os
from pathlib import Path

import polars as pl
import pytest

from honestroles.cli.main import main
from honestroles.ingest.normalize import INGEST_METADATA_FIELDS
from honestroles.schema import CANONICAL_SOURCE_FIELDS

_RUN_SMOKE_ENV = "HONESTROLES_RUN_INGEST_SMOKE"
_SOURCE_REF_ENV_BY_SOURCE: dict[str, str] = {
    "greenhouse": "HONESTROLES_SMOKE_GREENHOUSE_REF",
    "lever": "HONESTROLES_SMOKE_LEVER_REF",
    "ashby": "HONESTROLES_SMOKE_ASHBY_REF",
    "workable": "HONESTROLES_SMOKE_WORKABLE_REF",
}

pytestmark = pytest.mark.skipif(
    os.getenv(_RUN_SMOKE_ENV) != "1",
    reason=(
        "live ingestion smoke tests are disabled by default; set "
        f"{_RUN_SMOKE_ENV}=1 to enable"
    ),
)
pytestmark = [pytest.mark.smoke, pytestmark]


@pytest.mark.parametrize("source", ("greenhouse", "lever", "ashby", "workable"))
def test_ingest_sync_live_source_end_to_end(
    source: str,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    source_ref_env = _SOURCE_REF_ENV_BY_SOURCE[source]
    source_ref = os.getenv(source_ref_env, "").strip()
    if not source_ref:
        pytest.skip(f"set {source_ref_env} to run live smoke for source '{source}'")

    out_root = tmp_path / source
    output_parquet = out_root / "jobs.parquet"
    report_file = out_root / "sync_report.json"
    state_file = out_root / "state.json"

    code = main(
        [
            "ingest",
            "sync",
            "--source",
            source,
            "--source-ref",
            source_ref,
            "--output-parquet",
            str(output_parquet),
            "--report-file",
            str(report_file),
            "--state-file",
            str(state_file),
            "--max-pages",
            "1",
            "--max-jobs",
            "50",
            "--full-refresh",
            "--write-raw",
        ]
    )
    assert code == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "pass"
    assert payload["source"] == source
    assert payload["source_ref"] == source_ref
    assert int(payload["request_count"]) >= 1
    assert Path(payload["output_parquet"]).exists()
    assert Path(payload["report_file"]).exists()
    assert Path(payload["raw_file"]).exists()

    report_payload = json.loads(report_file.read_text(encoding="utf-8"))
    assert report_payload["status"] == "pass"
    assert report_payload["source"] == source
    assert report_payload["source_ref"] == source_ref
    assert int(report_payload["duration_ms"]) >= 0
    assert report_payload["error"] is None

    frame = pl.read_parquet(output_parquet)
    assert frame.height == int(payload["rows_written"])
    for column in CANONICAL_SOURCE_FIELDS:
        assert column in frame.columns
    for column in INGEST_METADATA_FIELDS:
        assert column in frame.columns
