#!/usr/bin/env bash
set -euo pipefail

PYTHON_BIN="${PYTHON_BIN:-python}"
DATABASE_URL_ENV="${DATABASE_URL_ENV:-NEON_DATABASE_URL}"
SCHEMA="${SCHEMA:-honestroles_api}"
WORK_DIR="${WORK_DIR:-dist/neondb-smoke}"

if [[ -z "${!DATABASE_URL_ENV:-}" ]]; then
  echo "Missing database URL env var '$DATABASE_URL_ENV'."
  exit 2
fi

mkdir -p "$WORK_DIR"
JOBS_PARQUET="$WORK_DIR/jobs.parquet"
INDEX_DIR="$WORK_DIR/index"
SYNC_REPORT="$WORK_DIR/sync_report.json"

PYTHONPATH=src:plugin_template/src "$PYTHON_BIN" - <<'PY'
from __future__ import annotations

import json
import os
from pathlib import Path

import polars as pl

from honestroles.recommend import build_retrieval_index

work_dir = Path(os.environ["WORK_DIR"]).expanduser().resolve()
work_dir.mkdir(parents=True, exist_ok=True)
jobs_parquet = Path(os.environ["JOBS_PARQUET"]).expanduser().resolve()
index_dir = Path(os.environ["INDEX_DIR"]).expanduser().resolve()
sync_report = Path(os.environ["SYNC_REPORT"]).expanduser().resolve()

pl.DataFrame(
    {
        "id": ["sample-1", "sample-2", "sample-3"],
        "source_job_id": ["gh-1001", "gh-1002", "gh-1003"],
        "title": ["Senior Data Engineer", "Backend Engineer", "ML Engineer"],
        "company": ["Acme", "Acme", "Beta"],
        "location": ["Remote US", "Lisbon", "Remote EU"],
        "work_mode": ["remote", "onsite", "remote"],
        "seniority": ["senior", "mid", "mid"],
        "employment_type": ["full_time", "full_time", "contract"],
        "remote": [True, False, True],
        "description_text": [
            "Build python sql pipelines in dbt and postgres",
            "Scale backend APIs in go and postgres",
            "Train and ship ml models with python",
        ],
        "description_html": ["<p>desc1</p>", "<p>desc2</p>", "<p>desc3</p>"],
        "skills": [
            ["python", "sql", "dbt", "postgres"],
            ["go", "postgres", "api"],
            ["python", "ml", "pytorch"],
        ],
        "salary_min": [130000.0, 90000.0, 110000.0],
        "salary_max": [180000.0, 120000.0, 150000.0],
        "salary_currency": ["USD", "EUR", "EUR"],
        "salary_interval": ["year", "year", "year"],
        "apply_url": [
            "https://example.com/jobs/1",
            "https://example.com/jobs/2",
            "https://example.com/jobs/3",
        ],
        "posted_at": [
            "2026-03-01T00:00:00Z",
            "2026-03-02T00:00:00Z",
            "2026-03-03T00:00:00Z",
        ],
        "source_updated_at": [
            "2026-03-04T00:00:00Z",
            "2026-03-04T00:00:00Z",
            "2026-03-04T00:00:00Z",
        ],
        "source": ["greenhouse", "greenhouse", "lever"],
        "source_ref": ["smoke", "smoke", "smoke"],
        "job_url": [
            "https://example.com/jobs/1",
            "https://example.com/jobs/2",
            "https://example.com/jobs/3",
        ],
    }
).write_parquet(jobs_parquet)

build_retrieval_index(input_parquet=jobs_parquet, output_dir=index_dir)
sync_report.write_text(
    json.dumps({"schema_version": "1.0", "status": "pass", "quality_status": "pass"}),
    encoding="utf-8",
)

print(f"Prepared smoke artifacts in {work_dir}")
PY

PYTHONPATH=src:plugin_template/src "$PYTHON_BIN" -m honestroles.cli.main \
  publish neondb migrate \
  --database-url-env "$DATABASE_URL_ENV" \
  --schema "$SCHEMA" \
  --format table

PYTHONPATH=src:plugin_template/src "$PYTHON_BIN" -m honestroles.cli.main \
  publish neondb sync \
  --database-url-env "$DATABASE_URL_ENV" \
  --schema "$SCHEMA" \
  --jobs-parquet "$JOBS_PARQUET" \
  --index-dir "$INDEX_DIR" \
  --sync-report "$SYNC_REPORT" \
  --require-quality-pass \
  --full-refresh \
  --batch-id "smoke-$(date -u +%Y%m%d%H%M%S)" \
  --format table

PYTHONPATH=src:plugin_template/src "$PYTHON_BIN" -m honestroles.cli.main \
  publish neondb verify \
  --database-url-env "$DATABASE_URL_ENV" \
  --schema "$SCHEMA" \
  --format table

PYTHONPATH=src:plugin_template/src "$PYTHON_BIN" - <<'PY'
from __future__ import annotations

import json
import os
from pathlib import Path

import psycopg

schema = os.environ["SCHEMA"]
database_url = os.environ[os.environ["DATABASE_URL_ENV"]]

candidate = {
    "profile_id": "smoke-agent",
    "skills": ["python", "sql", "postgres"],
    "titles": ["data engineer"],
    "locations": ["remote"],
    "work_mode_preferences": ["remote", "hybrid"],
    "seniority_targets": ["mid", "senior"],
}

query = f"""
SELECT
  job_id,
  score,
  match_reasons,
  required_missing_skills,
  apply_url,
  posted_at,
  source,
  quality_flags,
  excluded,
  exclude_reasons
FROM {schema}.match_jobs_v1(%s::jsonb, %s, %s, %s::jsonb)
"""

with psycopg.connect(database_url) as conn:
    with conn.cursor() as cur:
        cur.execute(query, (json.dumps(candidate), 10, False, json.dumps({})))
        rows = cur.fetchall()

if not rows:
    raise SystemExit("Smoke query returned no rows")

first = rows[0]
if len(first) != 10:
    raise SystemExit(f"Unexpected match_jobs_v1 tuple width: {len(first)}")

response_schema = json.loads(
    Path("contracts/agent_response.v1.json").read_text(encoding="utf-8")
)
required_result_keys = set(
    response_schema["properties"]["results"]["items"]["required"]
)
present_result_keys = {
    "job_id",
    "score",
    "match_reasons",
    "required_missing_skills",
    "apply_url",
    "posted_at",
    "source",
    "quality_flags",
}
missing = sorted(required_result_keys - present_result_keys)
if missing:
    raise SystemExit(f"Schema contract mismatch, missing keys: {missing}")

print(f"Neon smoke match_rows={len(rows)} first_job_id={first[0]}")
PY
