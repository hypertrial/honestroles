from __future__ import annotations

import re
from pathlib import Path

from honestroles import schema


def test_required_columns_subset_all_columns() -> None:
    assert schema.REQUIRED_COLUMNS.issubset(set(schema.ALL_COLUMNS))


def test_jobs_current_row_fields_match_all_columns() -> None:
    typed_keys = set(schema.JobsCurrentRow.__annotations__.keys())
    assert typed_keys == set(schema.ALL_COLUMNS)


def test_schema_constants_are_strings() -> None:
    constants = [
        schema.JOB_KEY,
        schema.COMPANY,
        schema.SOURCE,
        schema.JOB_ID,
        schema.TITLE,
        schema.TEAM,
        schema.LOCATION_RAW,
        schema.REMOTE_FLAG,
        schema.EMPLOYMENT_TYPE,
        schema.POSTED_AT,
        schema.UPDATED_AT,
        schema.APPLY_URL,
        schema.DESCRIPTION_HTML,
        schema.DESCRIPTION_TEXT,
        schema.INGESTED_AT,
        schema.CONTENT_HASH,
        schema.SALARY_MIN,
        schema.SALARY_MAX,
        schema.SALARY_CURRENCY,
        schema.SALARY_INTERVAL,
        schema.CITY,
        schema.COUNTRY,
        schema.REGION,
        schema.REMOTE_TYPE,
        schema.SKILLS,
        schema.LAST_SEEN,
        schema.SALARY_TEXT,
        schema.LANGUAGES,
        schema.BENEFITS,
        schema.VISA_SPONSORSHIP,
    ]
    assert all(isinstance(value, str) for value in constants)


def test_contract_doc_required_fields_match_schema_required_columns() -> None:
    doc_path = (
        Path(__file__).resolve().parents[1]
        / "docs"
        / "source_data_contract_v1.md"
    )
    assert doc_path.exists()
    text = doc_path.read_text(encoding="utf-8")

    doc_required_fields = set(
        re.findall(r"^\|\s*`([a-z_]+)`\s*\|", text, flags=re.MULTILINE)
    )
    assert doc_required_fields == schema.REQUIRED_COLUMNS
