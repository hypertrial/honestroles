# HonestRoles

Clean, filter, label, and rate job description data using heuristics and local LLMs. 

HonestRoles is a Python package designed to transform raw job posting data into structured, scored, and searchable datasets. It provides a modular pipeline for normalization, high-performance filtering, and automated labeling using both traditional heuristics and local LLMs (Ollama).

## Features

- **🧹 Clean**: HTML stripping, location normalization (city/region/country), salary parsing, and record deduplication.
- **🔍 Filter**: High-performance `FilterChain` with predicates for location, salary, skills, and keyword matching.
- **🏷️ Label**: Automated seniority detection, role categorization, and tech stack extraction.
- **⭐️ Rate**: Comprehensive job description scoring for completeness and quality.
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

See `/docs/quickstart_contract.md` and `/docs/source_data_contract_v1.md`.

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
```

## Package Layout

```text
src/honestroles/
├── clean/        # HTML stripping, normalization, and dedup
├── filter/       # Composed FilterChain and predicates
├── io/           # Parquet and DuckDB I/O with validation
├── label/        # Seniority, Category, and Tech Stack labeling
├── llm/          # Ollama client and prompt templates
├── rate/         # Completeness, Quality, and Composite ratings
└── schema.py     # Centralized column name constants
```

## Testing

Run the test suite with `pytest`:
```bash
pytest
```

## Stability

- Changelog: `/CHANGELOG.md`
- Deprecation policy: `/docs/deprecation.md`
- Performance guardrails: `/docs/performance.md`
