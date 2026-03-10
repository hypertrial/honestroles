from __future__ import annotations

import hashlib
import json
from typing import Any, Callable
from urllib.parse import quote

from honestroles.errors import ConfigValidationError

BASE_URL = "https://boards-api.greenhouse.io"
ENDPOINT_TEMPLATE = "/v1/boards/{source_ref}/jobs?content=true&page={page}"
ALLOWED_METHODS: tuple[str, ...] = ("GET",)
DEFAULT_RATE_LIMIT_RPS = 3


def fetch_greenhouse_jobs(
    source_ref: str,
    *,
    max_pages: int,
    max_jobs: int,
    http_get_json: Callable[[str], Any],
) -> tuple[list[dict[str, Any]], int, tuple[str, ...]]:
    if not source_ref.strip():
        raise ConfigValidationError("source-ref must be non-empty")
    jobs: list[dict[str, Any]] = []
    request_count = 0
    warning_codes: set[str] = set()
    seen_page_fingerprints: set[str] = set()
    safe_ref = quote(source_ref, safe="")
    for page in range(max_pages):
        url = BASE_URL + ENDPOINT_TEMPLATE.format(source_ref=safe_ref, page=page)
        payload = http_get_json(url)
        request_count += 1
        items = payload.get("jobs") if isinstance(payload, dict) else None
        if items is None:
            raise ConfigValidationError("greenhouse response must include 'jobs' array")
        if not isinstance(items, list):
            raise ConfigValidationError("greenhouse response 'jobs' must be an array")
        if not items:
            break
        fingerprint = _page_fingerprint(items)
        if fingerprint in seen_page_fingerprints:
            warning_codes.add("INGEST_PAGE_REPEAT_DETECTED")
            break
        seen_page_fingerprints.add(fingerprint)
        jobs.extend(item for item in items if isinstance(item, dict))
        if len(jobs) >= max_jobs:
            break
    return jobs[:max_jobs], request_count, tuple(sorted(warning_codes))


def _page_fingerprint(items: list[Any]) -> str:
    key_parts: list[str] = []
    for item in items:
        if not isinstance(item, dict):
            key_parts.append(json.dumps(item, sort_keys=True, default=str))
            continue
        item_id = item.get("id")
        if item_id not in (None, ""):
            key_parts.append(f"id:{item_id}")
            continue
        absolute_url = item.get("absolute_url")
        if absolute_url not in (None, ""):
            key_parts.append(f"url:{absolute_url}")
            continue
        key_parts.append(json.dumps(item, sort_keys=True, default=str))
    digest = hashlib.sha256("|".join(key_parts).encode("utf-8")).hexdigest()
    return digest
