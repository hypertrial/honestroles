import requests

from honestroles.llm.client import OllamaClient


class _Response:
    def __init__(self, status_code: int, payload: dict[str, object]) -> None:
        self.status_code = status_code
        self._payload = payload

    @property
    def ok(self) -> bool:
        return 200 <= self.status_code < 300

    def json(self) -> dict[str, object]:
        return self._payload

    def raise_for_status(self) -> None:
        if not self.ok:
            raise requests.HTTPError("error")


def test_client_strips_trailing_slash() -> None:
    client = OllamaClient(base_url="http://localhost:11434/")
    assert client.base_url == "http://localhost:11434"


def test_is_available_network_error(monkeypatch) -> None:
    def _raise(*args, **kwargs):
        raise requests.RequestException("boom")

    monkeypatch.setattr(requests, "get", _raise)
    client = OllamaClient()
    assert client.is_available() is False


def test_generate_payload_and_response(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _post(url, json, timeout):  # type: ignore[no-untyped-def]
        captured["url"] = url
        captured["payload"] = json
        return _Response(200, {"response": "ok"})

    monkeypatch.setattr(requests, "post", _post)
    client = OllamaClient(base_url="http://localhost:11434")
    response = client.generate("prompt", model="llama3")
    assert response == "ok"
    assert captured["payload"]["model"] == "llama3"
    assert captured["payload"]["options"]["temperature"] == 0.1


def test_generate_includes_max_tokens(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _post(url, json, timeout):  # type: ignore[no-untyped-def]
        captured["payload"] = json
        return _Response(200, {"response": ""})

    monkeypatch.setattr(requests, "post", _post)
    client = OllamaClient()
    client.generate("prompt", model="llama3", max_tokens=12)
    options = captured["payload"]["options"]
    assert options["num_predict"] == 12


def test_chat_payload_and_response(monkeypatch) -> None:
    def _post(url, json, timeout):  # type: ignore[no-untyped-def]
        return _Response(200, {"message": {"content": "hello"}})

    monkeypatch.setattr(requests, "post", _post)
    client = OllamaClient()
    response = client.chat([{"role": "user", "content": "hi"}], model="llama3")
    assert response == "hello"
