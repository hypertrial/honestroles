## Label

`honestroles.label` adds derived labels to job postings using heuristic rules
and optional LLM classification.

### Modules

- `__init__.py`: `label_jobs` orchestrator and re-exports.
- `heuristic.py`: rule-based seniority, role category, and tech stack labeling.
- `llm.py`: LLM-based label classification using Ollama.

### Public API reference

#### `label_jobs(df: pd.DataFrame, *, use_llm: bool = False, model: str = "llama3", labels: list[str] | None = None, column: str = DESCRIPTION_TEXT, ollama_url: str = "http://localhost:11434", batch_size: int = 8, plugin_labelers: list[str] | None = None, plugin_labeler_kwargs: dict[str, dict[str, object]] | None = None) -> pd.DataFrame`

Runs heuristic labeling (`label_seniority`, `label_role_category`,
`label_tech_stack`). If `use_llm=True`, appends `label_with_llm` using the
provided LLM args (`model`, `labels`, `column`, `ollama_url`, `batch_size`).
If `plugin_labelers` are provided, registered label
plugins are applied after built-in labeling.

#### `label_seniority(df: pd.DataFrame, *, title_column: str = TITLE) -> pd.DataFrame`

Adds a `seniority` column based on job title patterns
(`intern`, `junior`, `mid`, `senior`, `staff`, `principal`, `lead`, `director`,
`vp`, `c_level`). Existing non-empty `seniority` values are preserved.

#### `label_role_category(df: pd.DataFrame, *, title_column: str = TITLE, description_column: str = DESCRIPTION_TEXT) -> pd.DataFrame`

Adds a `role_category` column by scanning title and description keywords
(`engineering`, `data`, `design`, `product`, `marketing`, `sales`, `operations`,
`finance`, `hr`, `legal`, `support`) using boundary-aware regexes. Matching is
title-first, then description fallback for unresolved rows.

#### `label_tech_stack(df: pd.DataFrame, *, skills_column: str = SKILLS, description_column: str = DESCRIPTION_TEXT) -> pd.DataFrame`

Adds a `tech_stack` column containing a sorted list of detected terms from
alias-normalized `skills` values plus boundary-aware description extraction.

#### `label_with_llm(...) -> pd.DataFrame`

```
label_with_llm(
    df: pd.DataFrame,
    *,
    model: str = "llama3",
    labels: list[str] | None = None,
    column: str = DESCRIPTION_TEXT,
    ollama_url: str = "http://localhost:11434",
    batch_size: int = 8,
) -> pd.DataFrame
```

Uses `OllamaClient` to classify descriptions and writes `llm_labels` as a list.
If the column is missing or Ollama is unavailable, returns the input DataFrame.

### Usage examples

```python
import honestroles as hr

df = hr.read_parquet("jobs_current.parquet")
df = hr.clean_jobs(df)
df = hr.label_jobs(df, use_llm=False)
```

```python
from honestroles.label import label_with_llm

df = label_with_llm(
    df,
    model="llama3",
    labels=["engineering", "data", "product"],
    ollama_url="http://localhost:11434",
)
```

### Design notes

- Heuristic labeling uses predefined regex patterns and keyword sets; tune them
  in `heuristic.py` if your dataset has domain-specific terms.
- `DEFAULT_LABELS` in `llm.py` is used when no label list is provided.
