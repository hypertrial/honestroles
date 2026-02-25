# HonestRoles

HonestRoles, developed by [Hypertrial](https://www.hypertrial.ai), is a Python package designed to transform raw job posting data into structured, scored, and searchable datasets.

## Features

- **🧹 Clean**: HTML stripping, location normalization (city/region/country), salary parsing, and record deduplication.
- **🕰️ Historical Mode**: Opt-in cleaning and compaction for historical snapshots (`clean_historical_jobs`).
- **🔍 Filter**: High-performance `FilterChain` with predicates for location, salary, skills, and keyword matching.
- **🏷️ Label**: Automated seniority detection, role categorization, and tech stack extraction.
- **⭐️ Rate**: Comprehensive job description scoring for completeness and quality.
- **🎯 Match**: Candidate-profile-based ranking with explainable fit breakdowns and next actions.
- **🤖 LLM Integration**: seamless integration with local Ollama models (e.g., Llama 3) for deep semantic analysis.

## Installation

```bash
pip install honestroles
```

For development:

```bash
git clone https://github.com/hypertrial/honestroles.git
cd honestroles
pip install -e ".[dev]"
```

## Choose Your Entry Point

- Library API (recommended for pipelines and notebooks): import `honestroles` and compose stages in Python.
- CLI commands (recommended for quick checks and plugin scaffolding):
  - `honestroles-report-quality`
  - `honestroles-scaffold-plugin`

## Quickstart

```python
import honestroles as hr
from honestroles import schema

# 1. Read source data without strict validation
df = hr.read_parquet("jobs_current.parquet", validate=False)

# 2. Normalize and validate source-data contract
df = hr.normalize_source_data_contract(df)
df = hr.validate_source_data_contract(df)

# 3. Clean and normalize derived fields
df = hr.clean_jobs(df)

# 4. Filter rows
df = hr.filter_jobs(
    df,
    remote_only=True,
    min_salary=120_000,
    required_skills=["Python"],
)

# 5. Label and rate
df = hr.label_jobs(df, use_llm=False)
df = hr.rate_jobs(df, use_llm=False)

# 6. Rank and plan next actions
profile = hr.CandidateProfile.mds_new_grad()
ranked = hr.rank_jobs(df, profile=profile, use_llm_signals=False, top_n=100)
plan = hr.build_application_plan(ranked, profile=profile, top_n=20)

# Access data using schema constants
print(df[[schema.TITLE, schema.CITY, schema.COUNTRY]].head())

# Save structured results
hr.write_parquet(df, "jobs_scored.parquet")
```

For historical snapshots, use the opt-in historical workflow:

```python
df = hr.read_parquet("jobs_historical.parquet", validate=False)
df = hr.clean_historical_jobs(df)
df = hr.filter_jobs(df, remote_only=False)
df = hr.label_jobs(df, use_llm=False)
df = hr.rate_jobs(df, use_llm=False)
```

Generate a quality report:

```bash
honestroles-report-quality jobs_historical.parquet --stream --format json
```

See `/docs/start/quickstart.md` and `/docs/reference/source_data_contract_v1.md`.

Documentation index: `/docs/index.md`.
Docs stack: `/docs/maintainers/docs_stack.md`.

Build docs locally:
```bash
pip install -e ".[docs]"
mkdocs serve
```

Deploy docs on GitHub Pages:
1. Ensure repository **Settings -> Pages -> Build and deployment -> Source** is set to **GitHub Actions**.
2. Push to `main` to trigger `.github/workflows/docs-pages.yml`.

## Core Modules

### Schema Constants
Always use `honestroles.schema` for consistent column referencing:
```python
from honestroles import schema

# Available constants:
# schema.TITLE, schema.DESCRIPTION_TEXT, schema.COMPANY
# schema.CITY, schema.REGION, schema.COUNTRY
# schema.SALARY_MIN, schema.SALARY_MAX, etc.
```

### Filtering with `FilterChain`
The `FilterChain` allows you to compose multiple filtering rules efficiently:
```python
import honestroles as hr
from honestroles import schema

# Functional approach:
df = hr.filter_jobs(df, remote_only=True, min_salary=100_000)

# Composable approach:
chain = hr.FilterChain()
chain.add(hr.filter.by_keywords, include=["Engineer"], exclude=["Manager"])
chain.add(hr.filter.by_completeness, required_fields=[schema.DESCRIPTION_TEXT, schema.APPLY_URL])
filtered_df = chain.apply(df)
```

### Local LLM Usage (Ollama)
LLM integration uses built-in runtime dependencies (`requests`) plus a local Ollama server.
No separate package extra is required.

Ensure [Ollama](https://ollama.com/) is running locally:
```bash
ollama serve
ollama pull llama3
```
Then enable LLM-based labeling or quality rating:
```python
df = hr.label_jobs(df, use_llm=True, model="llama3")
df = hr.rate_jobs(df, use_llm=True, model="llama3")
ranked = hr.rank_jobs(df, profile=hr.CandidateProfile.mds_new_grad(), use_llm_signals=True, model="llama3")
```

## Package Layout

```text
src/honestroles/
├── clean/        # HTML stripping, normalization, and dedup
├── filter/       # Composed FilterChain and predicates
├── io/           # Parquet and DuckDB I/O with validation
├── label/        # Seniority, Category, and Tech Stack labeling
├── llm/          # Ollama client and prompt templates
├── match/        # Candidate profile matching, ranking, and action plans
├── rate/         # Completeness, Quality, and Composite ratings
└── schema.py     # Centralized column name constants
```

## Testing

Run the default test suite (includes `tests/` and `plugin_template/tests/`) with `pytest`:
```bash
pytest
```

Run historical smoke checks explicitly (non-default marker):
```bash
pytest -o addopts="" -m "historical_smoke"
```

Run the repository coverage gate (100% required):
```bash
pytest -m "not performance" --cov=src --cov=plugin_template/src --cov-report=term-missing --cov-fail-under=100 -q
```

Run all CI-equivalent quality checks automatically before each local commit:
```bash
pip install -e ".[dev]"
pre-commit install
pre-commit run --all-files
```
This installs a Git `pre-commit` hook that runs `ruff`, `mypy`, and the 100% coverage gate command above.

## Stability

- Changelog: `/CHANGELOG.md`
- Performance guardrails: `/docs/maintainers/performance.md`
- Docs index: `/docs/index.md`
