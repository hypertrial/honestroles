## LLM

`honestroles.llm` provides a minimal client wrapper for Ollama and prompt
builders for labeling and quality scoring.

### Modules

- `client.py`: `OllamaClient` HTTP wrapper.
- `prompts.py`: prompt templates for label and quality tasks.

### Public API reference

#### `OllamaClient`

```
OllamaClient(base_url: str = "http://localhost:11434", *, timeout: int = 30)
```

- `is_available() -> bool`: checks `/api/tags` for availability.
- `generate(prompt: str, *, model: str, temperature: float = 0.1, max_tokens: int | None = None) -> str`:
  calls `/api/generate` and returns the response text.
- `chat(messages: list[dict[str, str]], *, model: str, temperature: float = 0.1, max_tokens: int | None = None) -> str`:
  calls `/api/chat` and returns the assistant message content.

#### `build_label_prompt(text: str, labels: list[str]) -> str`

Returns a prompt instructing the model to return JSON with a `labels` list.

#### `build_quality_prompt(text: str) -> str`

Returns a prompt instructing the model to return JSON with `score` and `reason`.

### Usage examples

```python
from honestroles.llm import OllamaClient, build_label_prompt

client = OllamaClient(base_url="http://localhost:11434")
if client.is_available():
    prompt = build_label_prompt("We need a senior data engineer...", ["data", "engineering"])
    response = client.generate(prompt, model="llama3")
```

```python
from honestroles.llm import build_quality_prompt

prompt = build_quality_prompt("This is a short job description.")
```

### Design notes

- These helpers are intentionally thin wrappers. They do not retry or batch.
- You must run an Ollama server locally (`ollama serve`) for calls to succeed.
