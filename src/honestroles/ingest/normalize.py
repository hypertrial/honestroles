from __future__ import annotations

from datetime import UTC, datetime
import hashlib
import json
from typing import Any, Mapping

import polars as pl

from honestroles.io import normalize_source_data_contract
from honestroles.schema import CANONICAL_JOB_SCHEMA

INGEST_METADATA_FIELDS: tuple[str, ...] = (
    "source",
    "source_ref",
    "source_job_id",
    "job_url",
    "ingested_at_utc",
    "source_payload_hash",
)
INGEST_ADDITIONAL_FIELDS: tuple[str, ...] = (
    "source_updated_at",
    "work_mode",
    "salary_currency",
    "salary_interval",
    "employment_type",
    "seniority",
)


def normalize_records(
    records: list[dict[str, Any]],
    *,
    source: str,
    source_ref: str,
    ingested_at_utc: str,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for raw in records:
        normalized = _normalize_one(raw, source=source, source_ref=source_ref)
        normalized["ingested_at_utc"] = ingested_at_utc
        normalized["source_payload_hash"] = _payload_hash(raw)
        out.append(normalized)
    return out


def normalized_dataframe(records: list[dict[str, Any]]) -> pl.DataFrame:
    if records:
        frame = pl.DataFrame(records, infer_schema_length=None)
    else:
        return _empty_normalized_frame()
    frame = normalize_source_data_contract(frame)
    return _ensure_metadata_columns(frame)


def _normalize_one(raw: Mapping[str, Any], *, source: str, source_ref: str) -> dict[str, Any]:
    extractor = {
        "greenhouse": _extract_greenhouse,
        "lever": _extract_lever,
        "ashby": _extract_ashby,
        "workable": _extract_workable,
    }.get(source, _extract_generic)

    base = extractor(raw)
    base["source"] = source
    base["source_ref"] = source_ref
    base["source_job_id"] = _text_or_none(base.get("source_job_id"))
    base["job_url"] = _text_or_none(base.get("job_url") or base.get("apply_url"))
    base["source_updated_at"] = _coerce_timestamp(base.get("source_updated_at"))
    base["work_mode"] = _normalize_work_mode(base.get("work_mode"), base.get("location"))
    base["salary_currency"] = _text_or_none(base.get("salary_currency"))
    base["salary_interval"] = _text_or_none(base.get("salary_interval"))
    base["employment_type"] = _text_or_none(base.get("employment_type"))
    base["seniority"] = _text_or_none(base.get("seniority"))
    return base


def _extract_greenhouse(raw: Mapping[str, Any]) -> dict[str, Any]:
    location = None
    if isinstance(raw.get("location"), Mapping):
        location = _text_or_none(raw["location"].get("name"))
    return {
        "id": _text_or_none(raw.get("id")),
        "title": _text_or_none(raw.get("title")),
        "company": _text_or_none(raw.get("company") or raw.get("office")),
        "location": location,
        "remote": _infer_remote(location),
        "description_text": _text_or_none(raw.get("content")),
        "description_html": _text_or_none(raw.get("content")),
        "skills": (),
        "salary_min": None,
        "salary_max": None,
        "apply_url": _text_or_none(raw.get("absolute_url")),
        "posted_at": _coerce_timestamp(raw.get("updated_at") or raw.get("created_at")),
        "source_updated_at": _coerce_timestamp(raw.get("updated_at")),
        "work_mode": _infer_work_mode(location),
        "salary_currency": _text_or_none(raw.get("currency")),
        "salary_interval": None,
        "employment_type": _text_or_none(raw.get("employment_type")),
        "seniority": _text_or_none(raw.get("seniority")),
        "source_job_id": _text_or_none(raw.get("id")),
        "job_url": _text_or_none(raw.get("absolute_url")),
    }


def _extract_lever(raw: Mapping[str, Any]) -> dict[str, Any]:
    categories = raw.get("categories")
    location = None
    company = None
    if isinstance(categories, Mapping):
        location = _text_or_none(categories.get("location"))
        company = _text_or_none(categories.get("team"))
    created_at = raw.get("createdAt")
    if isinstance(created_at, (int, float)):
        created_text = datetime.fromtimestamp(float(created_at) / 1000.0, tz=UTC).isoformat()
    else:
        created_text = _coerce_timestamp(created_at)
    apply_url = _text_or_none(raw.get("hostedUrl") or raw.get("applyUrl"))
    return {
        "id": _text_or_none(raw.get("id")),
        "title": _text_or_none(raw.get("text") or raw.get("title")),
        "company": company,
        "location": location,
        "remote": _infer_remote(location),
        "description_text": _text_or_none(raw.get("descriptionPlain") or raw.get("description")),
        "description_html": _text_or_none(raw.get("description")),
        "skills": (),
        "salary_min": None,
        "salary_max": None,
        "apply_url": apply_url,
        "posted_at": created_text,
        "source_updated_at": _coerce_timestamp(raw.get("updatedAt") or raw.get("createdAt")),
        "work_mode": _infer_work_mode(location),
        "salary_currency": _text_or_none(raw.get("salaryCurrency")),
        "salary_interval": _text_or_none(raw.get("salaryInterval")),
        "employment_type": _text_or_none(raw.get("categories", {}).get("commitment"))
        if isinstance(raw.get("categories"), Mapping)
        else None,
        "seniority": _text_or_none(raw.get("categories", {}).get("level"))
        if isinstance(raw.get("categories"), Mapping)
        else None,
        "source_job_id": _text_or_none(raw.get("id")),
        "job_url": apply_url,
    }


def _extract_ashby(raw: Mapping[str, Any]) -> dict[str, Any]:
    location = _text_or_none(
        raw.get("location")
        or raw.get("locationName")
        or raw.get("locationLabel")
    )
    apply_url = _text_or_none(raw.get("jobUrl") or raw.get("jobPostUrl") or raw.get("applyUrl"))
    return {
        "id": _text_or_none(raw.get("id") or raw.get("jobId")),
        "title": _text_or_none(raw.get("title") or raw.get("jobTitle")),
        "company": _text_or_none(raw.get("companyName")),
        "location": location,
        "remote": _infer_remote(location),
        "description_text": _text_or_none(raw.get("description") or raw.get("descriptionText")),
        "description_html": _text_or_none(raw.get("descriptionHtml") or raw.get("description")),
        "skills": (),
        "salary_min": _coerce_float(raw.get("salaryMin")),
        "salary_max": _coerce_float(raw.get("salaryMax")),
        "apply_url": apply_url,
        "posted_at": _coerce_timestamp(
            raw.get("publishedDate")
            or raw.get("updatedAt")
            or raw.get("postedAt")
        ),
        "source_updated_at": _coerce_timestamp(raw.get("updatedAt")),
        "work_mode": _infer_work_mode(location),
        "salary_currency": _text_or_none(raw.get("salaryCurrency")),
        "salary_interval": _text_or_none(raw.get("salaryInterval")),
        "employment_type": _text_or_none(raw.get("employmentType")),
        "seniority": _text_or_none(raw.get("seniority")),
        "source_job_id": _text_or_none(raw.get("id") or raw.get("jobId")),
        "job_url": apply_url,
    }


def _extract_workable(raw: Mapping[str, Any]) -> dict[str, Any]:
    location = None
    if isinstance(raw.get("location"), Mapping):
        location = _text_or_none(
            raw["location"].get("location_str")
            or raw["location"].get("city")
            or raw["location"].get("country")
        )
    location = location or _text_or_none(raw.get("location"))
    apply_url = _text_or_none(raw.get("url") or raw.get("apply_url"))
    return {
        "id": _text_or_none(raw.get("shortcode") or raw.get("id")),
        "title": _text_or_none(raw.get("title")),
        "company": _text_or_none(raw.get("account") or raw.get("company")),
        "location": location,
        "remote": _infer_remote(location),
        "description_text": _text_or_none(raw.get("description") or raw.get("description_plain")),
        "description_html": _text_or_none(raw.get("description")),
        "skills": (),
        "salary_min": _coerce_float(raw.get("salary_min")),
        "salary_max": _coerce_float(raw.get("salary_max")),
        "apply_url": apply_url,
        "posted_at": _coerce_timestamp(raw.get("published") or raw.get("updated_at")),
        "source_updated_at": _coerce_timestamp(raw.get("updated_at")),
        "work_mode": _infer_work_mode(location),
        "salary_currency": _text_or_none(raw.get("salary_currency_code")),
        "salary_interval": _text_or_none(raw.get("salary_interval")),
        "employment_type": _text_or_none(raw.get("employment_type")),
        "seniority": _text_or_none(raw.get("experience_level")),
        "source_job_id": _text_or_none(raw.get("shortcode") or raw.get("id")),
        "job_url": apply_url,
    }


def _extract_generic(raw: Mapping[str, Any]) -> dict[str, Any]:
    apply_url = _text_or_none(raw.get("apply_url") or raw.get("url"))
    return {
        "id": _text_or_none(raw.get("id")),
        "title": _text_or_none(raw.get("title")),
        "company": _text_or_none(raw.get("company")),
        "location": _text_or_none(raw.get("location")),
        "remote": _infer_remote(raw.get("location")),
        "description_text": _text_or_none(raw.get("description_text") or raw.get("description")),
        "description_html": _text_or_none(raw.get("description_html") or raw.get("description")),
        "skills": (),
        "salary_min": _coerce_float(raw.get("salary_min")),
        "salary_max": _coerce_float(raw.get("salary_max")),
        "apply_url": apply_url,
        "posted_at": _coerce_timestamp(raw.get("posted_at")),
        "source_updated_at": _coerce_timestamp(raw.get("updated_at")),
        "work_mode": _infer_work_mode(raw.get("location")),
        "salary_currency": _text_or_none(raw.get("salary_currency")),
        "salary_interval": _text_or_none(raw.get("salary_interval")),
        "employment_type": _text_or_none(raw.get("employment_type")),
        "seniority": _text_or_none(raw.get("seniority")),
        "source_job_id": _text_or_none(raw.get("id")),
        "job_url": apply_url,
    }


def _ensure_metadata_columns(frame: pl.DataFrame) -> pl.DataFrame:
    missing = [
        name
        for name in (*INGEST_METADATA_FIELDS, *INGEST_ADDITIONAL_FIELDS)
        if name not in frame.columns
    ]
    if missing:
        frame = frame.with_columns(
            [pl.lit(None, dtype=pl.String).alias(name) for name in missing]
        )
    return frame.with_columns(
        pl.col("source").cast(pl.String, strict=False),
        pl.col("source_ref").cast(pl.String, strict=False),
        pl.col("source_job_id").cast(pl.String, strict=False),
        pl.col("job_url").cast(pl.String, strict=False),
        pl.col("ingested_at_utc").cast(pl.String, strict=False),
        pl.col("source_payload_hash").cast(pl.String, strict=False),
        pl.col("source_updated_at").cast(pl.String, strict=False),
        pl.col("work_mode").cast(pl.String, strict=False),
        pl.col("salary_currency").cast(pl.String, strict=False),
        pl.col("salary_interval").cast(pl.String, strict=False),
        pl.col("employment_type").cast(pl.String, strict=False),
        pl.col("seniority").cast(pl.String, strict=False),
    )


def _empty_normalized_frame() -> pl.DataFrame:
    columns: dict[str, pl.Series] = {}
    for name, spec in CANONICAL_JOB_SCHEMA.items():
        if spec.logical_type == "bool":
            dtype: pl.DataType = pl.Boolean
        elif spec.logical_type == "float":
            dtype = pl.Float64
        elif spec.logical_type == "list_string":
            dtype = pl.List(pl.String)
        else:
            dtype = pl.String
        columns[name] = pl.Series(name=name, values=[], dtype=dtype)
    for name in INGEST_METADATA_FIELDS:
        columns[name] = pl.Series(name=name, values=[], dtype=pl.String)
    for name in INGEST_ADDITIONAL_FIELDS:
        columns[name] = pl.Series(name=name, values=[], dtype=pl.String)
    return pl.DataFrame(columns)


def _infer_remote(value: object) -> bool | None:
    text = _text_or_none(value)
    if text is None:
        return None
    lowered = text.lower()
    if "remote" in lowered:
        return True
    if "hybrid" in lowered or "onsite" in lowered or "on-site" in lowered:
        return False
    return None


def _infer_work_mode(value: object) -> str:
    text = _text_or_none(value)
    if text is None:
        return "unknown"
    lowered = text.lower()
    if "remote" in lowered:
        return "remote"
    if "hybrid" in lowered:
        return "hybrid"
    if "onsite" in lowered or "on-site" in lowered:
        return "onsite"
    return "unknown"


def _normalize_work_mode(value: object, location: object) -> str:
    text = _text_or_none(value)
    if text is not None:
        mode = _infer_work_mode(text)
        if mode != "unknown":
            return mode
    return _infer_work_mode(location)


def _text_or_none(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _coerce_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = _text_or_none(value)
    if text is None:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _coerce_timestamp(value: object) -> str | None:
    text = _text_or_none(value)
    if text is None:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return _text_or_none(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    else:
        parsed = parsed.astimezone(UTC)
    return parsed.isoformat()


def _payload_hash(raw: Mapping[str, Any]) -> str:
    digest = hashlib.sha256()
    digest.update(json.dumps(dict(raw), sort_keys=True, default=str).encode("utf-8"))
    return digest.hexdigest()
