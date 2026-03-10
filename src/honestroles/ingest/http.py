from __future__ import annotations

import json
import time
from typing import Any, Callable, Mapping
from urllib import error, request

from honestroles.errors import HonestRolesError

_RETRYABLE_HTTP_STATUS = {429, 500, 502, 503, 504}


def build_http_getter(
    *,
    timeout_seconds: float,
    max_retries: int,
    base_backoff_seconds: float,
    user_agent: str,
    on_request: Callable[[int | None, bool], None] | None = None,
) -> Callable[[str], Any]:
    def _getter(url: str) -> Any:
        return fetch_json(
            url,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            base_backoff_seconds=base_backoff_seconds,
            headers={"User-Agent": user_agent},
            on_request=on_request,
        )

    return _getter


def fetch_json(
    url: str,
    *,
    timeout_seconds: float = 15.0,
    max_retries: int = 3,
    base_backoff_seconds: float = 0.25,
    headers: Mapping[str, str] | None = None,
    on_request: Callable[[int | None, bool], None] | None = None,
) -> Any:
    req_headers = {
        "Accept": "application/json",
        "User-Agent": "honestroles-ingest/2.0",
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
            if on_request is not None:
                on_request(200, attempt > 1)
            if not body.strip():
                return {}
            return json.loads(body)
        except error.HTTPError as exc:
            if on_request is not None:
                on_request(exc.code, attempt > 1)
            if attempt <= max_retries and exc.code in _RETRYABLE_HTTP_STATUS:
                time.sleep(base_backoff_seconds * (2 ** (attempt - 1)))
                continue
            detail = _http_error_detail(exc)
            raise HonestRolesError(
                f"ingestion request failed for '{url}': HTTP {exc.code} {detail}"
            ) from exc
        except error.URLError as exc:
            if on_request is not None:
                on_request(None, attempt > 1)
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
