from __future__ import annotations

from typing import Any, Callable
from urllib.parse import quote

from honestroles.errors import ConfigValidationError

BASE_URL = "https://api.ashbyhq.com"
ENDPOINT_TEMPLATE = "/posting-api/job-board/{source_ref}?includeCompensation=true{cursor_query}"
ALLOWED_METHODS: tuple[str, ...] = ("GET",)
DEFAULT_RATE_LIMIT_RPS = 3


def fetch_ashby_jobs(
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
    cursor: str | None = None
    seen_cursors: set[str] = set()
    safe_ref = quote(source_ref, safe="")
    for _ in range(max_pages):
        cursor_query = f"&cursor={quote(cursor, safe='')}" if cursor else ""
        url = BASE_URL + ENDPOINT_TEMPLATE.format(
            source_ref=safe_ref,
            cursor_query=cursor_query,
        )
        payload = http_get_json(url)
        request_count += 1
        if not isinstance(payload, dict):
            raise ConfigValidationError("ashby response must be a JSON object")
        items = _extract_items(payload)
        next_cursor = _extract_next_cursor(payload)
        if not items:
            break
        jobs.extend(item for item in items if isinstance(item, dict))
        if len(jobs) >= max_jobs:
            break
        if not next_cursor:
            break
        if next_cursor in seen_cursors:
            break
        seen_cursors.add(next_cursor)
        cursor = next_cursor
    return jobs[:max_jobs], request_count


def _extract_items(payload: dict[str, Any]) -> list[Any]:
    for key in ("jobs", "postings", "results"):
        value = payload.get(key)
        if isinstance(value, list):
            return value
    raise ConfigValidationError("ashby response must include jobs/postings/results array")


def _extract_next_cursor(payload: dict[str, Any]) -> str | None:
    for key in ("nextCursor", "next_cursor", "nextPageToken", "next_page_token"):
        value = payload.get(key)
        if value in (None, ""):
            continue
        return str(value)
    return None
