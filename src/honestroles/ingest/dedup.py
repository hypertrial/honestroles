from __future__ import annotations

import hashlib
from typing import Any
from urllib.parse import urlparse, urlunparse


def deduplicate_records(
    records: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], int]:
    seen: set[str] = set()
    kept: list[dict[str, Any]] = []
    dropped = 0
    for record in records:
        key = dedup_key(record)
        if key in seen:
            dropped += 1
            continue
        seen.add(key)
        kept.append(record)
    return kept, dropped


def dedup_key(record: dict[str, Any]) -> str:
    apply_url = _normalize_url(record.get("apply_url"))
    if apply_url:
        return f"url:{apply_url}"
    job_url = _normalize_url(record.get("job_url"))
    if job_url:
        return f"url:{job_url}"

    source = _norm_text(record.get("source"))
    source_job_id = _norm_text(record.get("source_job_id"))
    if source and source_job_id:
        return f"source-id:{source}:{source_job_id}"

    signature = "|".join(
        [
            _norm_text(record.get("title")),
            _norm_text(record.get("company")),
            _norm_text(record.get("location")),
            _norm_text(record.get("posted_at")),
        ]
    )
    digest = hashlib.sha256(signature.encode("utf-8")).hexdigest()
    return f"fallback:{digest}"


def _normalize_url(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    parsed = urlparse(text)
    if not parsed.scheme or not parsed.netloc:
        return text.lower()
    cleaned = parsed._replace(query="", fragment="")
    normalized = urlunparse(cleaned)
    return normalized.rstrip("/").lower()


def _norm_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip().lower()
