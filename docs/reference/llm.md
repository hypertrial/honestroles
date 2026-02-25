## LLM

`honestroles.llm` provides a minimal client wrapper for Ollama and prompt
builders for labeling, quality scoring, and structured job-signal extraction.

### Modules

- `client.py`: `OllamaClient` HTTP wrapper.
- `prompts.py`: prompt templates for label, quality, and job-signal tasks.

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

#### `build_job_signal_prompt(*, title: str, description: str) -> str`

Returns a prompt instructing the model to return JSON with structured matching
signals such as required/preferred skills, experience range, visa signal,
friction/clarity scores, confidence, and reason.

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

```python
from honestroles.llm import build_job_signal_prompt

prompt = build_job_signal_prompt(
    title="Data Analyst",
    description="We require SQL and Python. 1-2 years experience preferred.",
)
```

### Design notes

- These helpers are intentionally thin wrappers. They do not retry or batch.
- You must run an Ollama server locally (`ollama serve`) for calls to succeed.
