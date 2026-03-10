from __future__ import annotations

from typing import Any, Callable
from urllib.parse import quote

from honestroles.errors import ConfigValidationError

BASE_URL = "https://www.workable.com"
ACCOUNT_ENDPOINT = "/api/accounts/{source_ref}?details=true"
LOCATIONS_ENDPOINT = "/api/accounts/{source_ref}/locations"
DEPARTMENTS_ENDPOINT = "/api/accounts/{source_ref}/departments"
ALLOWED_METHODS: tuple[str, ...] = ("GET",)
DEFAULT_RATE_LIMIT_RPS = 2


def fetch_workable_jobs(
    source_ref: str,
    *,
    max_pages: int,
    max_jobs: int,
    http_get_json: Callable[[str], Any],
) -> tuple[list[dict[str, Any]], int, tuple[str, ...]]:
    if not source_ref.strip():
        raise ConfigValidationError("source-ref must be non-empty")
    if max_pages < 1:
        raise ConfigValidationError("max-pages must be >= 1")

    safe_ref = quote(source_ref, safe="")
    request_count = 0

    # Public careers API endpoints. Locations/departments are fetched for parity
    # and schema stability, even when jobs endpoint already contains location strings.
    account_payload = http_get_json(
        BASE_URL + ACCOUNT_ENDPOINT.format(source_ref=safe_ref)
    )
    request_count += 1
    http_get_json(BASE_URL + LOCATIONS_ENDPOINT.format(source_ref=safe_ref))
    request_count += 1
    http_get_json(BASE_URL + DEPARTMENTS_ENDPOINT.format(source_ref=safe_ref))
    request_count += 1

    if not isinstance(account_payload, dict):
        raise ConfigValidationError("workable account response must be a JSON object")

    jobs = account_payload.get("jobs")
    if jobs is None and isinstance(account_payload.get("results"), list):
        jobs = account_payload["results"]
    if not isinstance(jobs, list):
        raise ConfigValidationError("workable account response must include 'jobs' array")

    # Public endpoint is treated as a single-page feed in v1; max_pages
    # is validated for API symmetry with other connectors.
    out = [item for item in jobs if isinstance(item, dict)]
    return out[:max_jobs], request_count, ()
