# honestroles

Clean, filter, label, and rate job description data using heuristics and local LLMs.

## Package layout

```
honestroles/
├── analytics/                 # standalone EDA script(s)
├── src/honestroles/           # package source
│   ├── __init__.py            # public API
│   ├── schema.py              # column constants + TypedDict
│   ├── io/                    # parquet + DuckDB I/O
│   ├── clean/                 # HTML stripping + normalization + dedup
│   ├── filter/                # predicate filters + FilterChain
│   ├── label/                 # heuristic + Ollama labeling
│   ├── rate/                  # completeness + quality + composite ratings
│   └── llm/                   # Ollama client + prompts
└── tests/                     # pytest suites for each module
```

## Installation

```bash
pip install honestroles
```

For development:

```bash
pip install -e ".[dev]"
```

## Quickstart

```python
import honestroles as hr

df = hr.read_parquet("jobs_current.parquet")
df = hr.clean_jobs(df)
df = hr.filter_jobs(df, remote_only=True, min_salary=100_000)
df = hr.label_jobs(df, use_llm=False)
df = hr.rate_jobs(df)
```

## Data I/O

Parquet:

```python
df = hr.read_parquet("jobs_current.parquet")
hr.write_parquet(df, "jobs_scored.parquet")
```

DuckDB:

```python
import duckdb

conn = duckdb.connect()
df = hr.read_duckdb(conn, "jobs_current")
hr.write_duckdb(df, conn, "jobs_scored")
```

## Schema constants

Use `honestroles.schema` for consistent column names:

```python
from honestroles import schema

schema.TITLE
schema.DESCRIPTION_TEXT
```

## Modules

- `honestroles.io`: read/write parquet and DuckDB, DataFrame validation
- `honestroles.clean`: HTML stripping, normalization, and deduplication
- `honestroles.filter`: composable filter predicates and chains
- `honestroles.label`: heuristic labeling, optional Ollama-based labeling
- `honestroles.rate`: completeness and quality scoring with composite ratings
- `honestroles.llm`: Ollama client and prompt templates

## Ollama usage

Ensure Ollama is running locally:

```bash
ollama serve
```

Then:

```python
df = hr.label_jobs(df, use_llm=True, model="llama3")
```