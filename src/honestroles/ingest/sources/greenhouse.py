from __future__ import annotations

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
) -> tuple[list[dict[str, Any]], int]:
    if not source_ref.strip():
        raise ConfigValidationError("source-ref must be non-empty")
    jobs: list[dict[str, Any]] = []
    request_count = 0
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
        jobs.extend(item for item in items if isinstance(item, dict))
        if len(jobs) >= max_jobs:
            break
    return jobs[:max_jobs], request_count
