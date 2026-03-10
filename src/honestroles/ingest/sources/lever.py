from __future__ import annotations

from typing import Any, Callable
from urllib.parse import quote

from honestroles.errors import ConfigValidationError

BASE_URL = "https://api.lever.co"
ENDPOINT_TEMPLATE = "/v0/postings/{source_ref}?mode=json&skip={skip}&limit={limit}"
ALLOWED_METHODS: tuple[str, ...] = ("GET",)
DEFAULT_RATE_LIMIT_RPS = 3
_PAGE_SIZE = 100


def fetch_lever_jobs(
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
    safe_ref = quote(source_ref, safe="")
    for page in range(max_pages):
        skip = page * _PAGE_SIZE
        url = BASE_URL + ENDPOINT_TEMPLATE.format(
            source_ref=safe_ref,
            skip=skip,
            limit=_PAGE_SIZE,
        )
        payload = http_get_json(url)
        request_count += 1
        if isinstance(payload, list):
            items = payload
        elif isinstance(payload, dict) and isinstance(payload.get("data"), list):
            items = payload["data"]
        else:
            raise ConfigValidationError(
                "lever response must be an array or object with 'data' array"
            )
        if not items:
            break
        jobs.extend(item for item in items if isinstance(item, dict))
        if len(jobs) >= max_jobs:
            break
    return jobs[:max_jobs], request_count, ()
