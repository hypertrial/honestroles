# HonestRoles

HonestRoles, developed by [Hypertrial](https://www.hypertrial.ai), is a Python package designed to transform raw job posting data into structured, scored, and searchable datasets.

## Features

- **🧹 Clean**: HTML stripping, location normalization (city/region/country), salary parsing, and record deduplication.
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

## Quickstart

```python
import honestroles as hr
from honestroles import schema

# Load raw job data (Parquet or DuckDB)
df = hr.read_parquet("jobs_current.parquet")

# 1. Clean and normalize data
df = hr.clean_jobs(df)

# 2. Apply complex filtering
chain = hr.FilterChain()
chain.add(hr.filter.by_location, regions=["California", "New York"])
chain.add(hr.filter.by_salary, min_salary=120_000, currency="USD")
chain.add(hr.filter.by_skills, required=["Python", "React"])
df = chain.apply(df)

# 3. Label roles (Heuristics + LLM)
df = hr.label_jobs(df, use_llm=True, model="llama3")

# 4. Rate job quality
df = hr.rate_jobs(df)

# 5. Rank for a candidate profile
profile = hr.CandidateProfile.mds_new_grad()
ranked = hr.rank_jobs(df, profile=profile, use_llm_signals=False, top_n=100)
plan = hr.build_application_plan(ranked, profile=profile, top_n=20)

# Access data using schema constants
print(df[[schema.TITLE, schema.CITY, schema.COUNTRY]].head())

# Save structured results
hr.write_parquet(df, "jobs_scored.parquet")
```

## Contract-First Flow

For source data, use contract normalization + validation before processing:

```python
import honestroles as hr

df = hr.read_parquet("jobs_current.parquet", validate=False)
df = hr.normalize_source_data_contract(df)
df = hr.validate_source_data_contract(df)

df = hr.clean_jobs(df)
df = hr.filter_jobs(df, remote_only=True)
df = hr.label_jobs(df, use_llm=False)
df = hr.rate_jobs(df, use_llm=False)
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
from honestroles import FilterChain, filter_jobs

# Functional approach:
df = filter_jobs(df, remote_only=True, min_salary=100_000)

# Composable approach:
chain = FilterChain()
chain.add(hr.filter.by_keywords, include=["Engineer"], exclude=["Manager"])
chain.add(hr.filter.by_completeness, required_fields=[schema.DESCRIPTION_TEXT, schema.APPLY_URL])
filtered_df = chain.apply(df)
```

### Local LLM Usage (Ollama)
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
