from __future__ import annotations

import json
import time
from typing import Any, Mapping
from urllib import error, request

from honestroles.errors import HonestRolesError

_RETRYABLE_HTTP_STATUS = {429, 500, 502, 503, 504}


def fetch_json(
    url: str,
    *,
    timeout_seconds: float = 15.0,
    max_retries: int = 3,
    base_backoff_seconds: float = 0.25,
    headers: Mapping[str, str] | None = None,
) -> Any:
    req_headers = {
        "Accept": "application/json",
        "User-Agent": "honestroles-ingest/1.0",
    }
    if headers is not None:
        req_headers.update(dict(headers))

    attempt = 0
    while True:
        attempt += 1
        req = request.Request(url=url, method="GET", headers=req_headers)
        try:
            with request.urlopen(req, timeout=timeout_seconds) as response:
                body = response.read().decode("utf-8")
            if not body.strip():
                return {}
            return json.loads(body)
        except error.HTTPError as exc:
            if attempt <= max_retries and exc.code in _RETRYABLE_HTTP_STATUS:
                time.sleep(base_backoff_seconds * (2 ** (attempt - 1)))
                continue
            detail = _http_error_detail(exc)
            raise HonestRolesError(
                f"ingestion request failed for '{url}': HTTP {exc.code} {detail}"
            ) from exc
        except error.URLError as exc:
            if attempt <= max_retries:
                time.sleep(base_backoff_seconds * (2 ** (attempt - 1)))
                continue
            raise HonestRolesError(
                f"ingestion request failed for '{url}': {exc.reason}"
            ) from exc
        except json.JSONDecodeError as exc:
            raise HonestRolesError(
                f"ingestion response for '{url}' is not valid JSON: {exc}"
            ) from exc


def _http_error_detail(exc: error.HTTPError) -> str:
    try:
        payload = exc.read().decode("utf-8", errors="ignore").strip()
    except Exception:
        payload = ""
    return payload if payload else "<no-body>"
