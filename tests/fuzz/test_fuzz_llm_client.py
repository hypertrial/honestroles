from __future__ import annotations

import requests
import pytest
from hypothesis import given
from hypothesis import strategies as st

from honestroles.llm.client import OllamaClient


class _FakeResponse:
    def __init__(self, *, ok: bool = True, payload: dict[str, object] | None = None, raise_http: bool = False) -> None:
        self.ok = ok
        self._payload = payload or {}
        self._raise_http = raise_http

    def raise_for_status(self) -> None:
        if self._raise_http:
            raise requests.HTTPError("boom")

    def json(self) -> dict[str, object]:
        return self._payload


@pytest.mark.fuzz
@given(available=st.booleans(), should_raise=st.booleans())
def test_fuzz_ollama_is_available(
    available: bool,
    should_raise: bool,
) -> None:
    def fake_get(url: str, timeout: int):
        if should_raise:
            raise requests.RequestException("down")
        return _FakeResponse(ok=available)

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(requests, "get", fake_get)
        client = OllamaClient(base_url="http://test")
        assert client.is_available() is (False if should_raise else available)


@pytest.mark.fuzz
@given(
    response_text=st.text(min_size=0, max_size=80),
    raise_http=st.booleans(),
)
def test_fuzz_ollama_generate_handles_http_and_payload(
    response_text: str,
    raise_http: bool,
) -> None:
    def fake_post(url: str, json: dict[str, object], timeout: int):
        return _FakeResponse(payload={"response": response_text}, raise_http=raise_http)

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(requests, "post", fake_post)
        client = OllamaClient(base_url="http://test")
        if raise_http:
            with pytest.raises(requests.HTTPError):
                client.generate("prompt", model="model")
        else:
            output = client.generate("prompt", model="model")
            assert output == response_text.strip()


@pytest.mark.fuzz
@given(
    content=st.text(min_size=0, max_size=80),
    raise_http=st.booleans(),
)
def test_fuzz_ollama_chat_handles_http_and_payload(
    content: str,
    raise_http: bool,
) -> None:
    def fake_post(url: str, json: dict[str, object], timeout: int):
        return _FakeResponse(payload={"message": {"content": content}}, raise_http=raise_http)

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(requests, "post", fake_post)
        client = OllamaClient(base_url="http://test")
        if raise_http:
            with pytest.raises(requests.HTTPError):
                client.chat([{"role": "user", "content": "hi"}], model="model")
        else:
            output = client.chat([{"role": "user", "content": "hi"}], model="model")
            assert output == content.strip()
