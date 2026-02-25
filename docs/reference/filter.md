## Filter

`honestroles.filter` provides composable predicates and a simple chaining
utility for filtering job DataFrames.

### Modules

- `__init__.py`: `filter_jobs` orchestrator and re-exports.
- `chain.py`: `FilterChain` for AND/OR predicate composition.
- `predicates.py`: `by_*` predicate helpers (location, salary, skills, keywords, recency).

### Public API reference

#### `filter_jobs(...) -> pd.DataFrame`

```
filter_jobs(
    df: pd.DataFrame,
    *,
    cities: list[str] | None = None,
    regions: list[str] | None = None,
    countries: list[str] | None = None,
    remote_only: bool = False,
    min_salary: float | None = None,
    max_salary: float | None = None,
    currency: str | None = None,
    required_skills: list[str] | None = None,
    excluded_skills: list[str] | None = None,
    include_keywords: list[str] | None = None,
    exclude_keywords: list[str] | None = None,
    keyword_columns: list[str] | None = None,
    required_fields: list[str] | None = None,
    posted_within_days: int | None = None,
    seen_within_days: int | None = None,
    as_of: str | pd.Timestamp | None = None,
    plugin_filters: list[str] | None = None,
    plugin_filter_kwargs: dict[str, dict[str, object]] | None = None,
    plugin_filter_mode: str = "and",
) -> pd.DataFrame
```

Builds a `FilterChain` in AND mode with `by_location`, `by_salary`,
`by_skills`, `by_keywords`, `by_completeness`, and optional `by_recency`.
Predicate families are skipped entirely when their inputs are inactive. If
`plugin_filters` are provided, registered filter plugins are applied after the
built-in chain.

#### `FilterChain`

- `FilterChain(mode: str = "and")`: Create a chain in "and" or "or" mode.
- `add(predicate: Predicate, **kwargs: object) -> FilterChain`: Add a predicate.
- `apply(df: pd.DataFrame) -> pd.DataFrame`: Apply all steps and return filtered rows.

`Predicate` is any callable that returns a `pd.Series` mask for a DataFrame.

#### Predicate helpers

- `by_location(...) -> pd.Series`: Filters by `city`, `region`, `country`, and `remote_flag`.
- `by_salary(...) -> pd.Series`: Filters by `salary_min`/`salary_max` and currency.
- `by_skills(...) -> pd.Series`: Filters by required/excluded skills across the union of
  `skills`, `tech_stack`, and `required_skills_extracted`.
- `by_keywords(...) -> pd.Series`: Filters by include/exclude terms in columns, with
  short-token precision guards for single include terms.
- `by_recency(...) -> pd.Series`: Filters by `posted_at`/`last_seen` recency windows with
  fallback to `ingested_at`.
- `by_completeness(...) -> pd.Series`: Filters by required field presence.

### Usage examples

```python
import honestroles as hr

df = hr.read_parquet("jobs_current.parquet")
df = hr.clean_jobs(df)
df = hr.filter_jobs(
    df,
    countries=["US"],
    regions=["California"],
    remote_only=True,
    min_salary=120_000,
    include_keywords=["python", "data"],
)
```

```python
from honestroles.filter import FilterChain, by_location, by_keywords

chain = FilterChain(mode="and")
chain.add(by_location, countries=["CA"])
chain.add(by_keywords, include=["machine learning"])

filtered = chain.apply(df)
```

### Design notes

- `FilterChain` always resets index after filtering.
- Predicates are defensive: if required columns are missing, they default to
  allowing all rows rather than raising.
