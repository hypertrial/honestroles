## Rate

`honestroles.rate` computes completeness and quality scores and combines them
into a composite rating.

### Modules

- `__init__.py`: `rate_jobs` orchestrator and re-exports.
- `completeness.py`: completeness scoring.
- `quality.py`: heuristic quality scoring with optional LLM refinement.
- `composite.py`: weighted composite rating.

### Public API reference

#### `rate_jobs(df: pd.DataFrame, *, use_llm: bool = False, model: str = "llama3", ollama_url: str = "http://localhost:11434") -> pd.DataFrame`

Runs `rate_completeness` -> `rate_quality` -> `rate_composite`.
Passes `use_llm`, `model`, and `ollama_url` to `rate_quality`.

#### `rate_completeness(...) -> pd.DataFrame`

```
rate_completeness(
    df: pd.DataFrame,
    *,
    required_fields: list[str] | None = None,
    output_column: str = "completeness_score",
) -> pd.DataFrame
```

Computes a 0-1 score based on presence of required fields. Defaults to a
curated field set (company, title, location, apply URL, description, salary,
skills, benefits).

#### `rate_quality(...) -> pd.DataFrame`

```
rate_quality(
    df: pd.DataFrame,
    *,
    column: str = DESCRIPTION_TEXT,
    output_column: str = "quality_score",
    use_llm: bool = False,
    model: str = "llama3",
    ollama_url: str = "http://localhost:11434",
) -> pd.DataFrame
```

Heuristic quality score: length-based plus a small bullet-list bonus.
If `use_llm=True`, adds `quality_score_llm` and `quality_reason_llm`.

#### `rate_composite(...) -> pd.DataFrame`

```
rate_composite(
    df: pd.DataFrame,
    *,
    completeness_column: str = "completeness_score",
    quality_column: str = "quality_score",
    output_column: str = "rating",
    weights: dict[str, float] | None = None,
) -> pd.DataFrame
```

Combines scores using weights (default 0.5 / 0.5). Missing score columns are
ignored.

### Usage examples

```python
import honestroles as hr

df = hr.read_parquet("jobs_current.parquet")
df = hr.clean_jobs(df)
df = hr.rate_jobs(df, use_llm=False)
```

```python
from honestroles.rate import rate_completeness, rate_quality, rate_composite

df = rate_completeness(df)
df = rate_quality(df, use_llm=True, model="llama3", ollama_url="http://localhost:11434")
df = rate_composite(df, weights={"completeness_score": 0.3, "quality_score": 0.7})
```

### Design notes

- Completeness and quality scores are normalized to the 0-1 range.
- `rate_quality` uses a heuristic by default to avoid LLM dependencies in tests.
