# Contract-First Quickstart

## When to use this

Use this page to run the canonical `honestroles` flow from raw source data to ranked opportunities.

<div class="hr-callout">
  <strong>At a glance:</strong> `read(validate=False) -> normalize -> validate -> clean/filter/label/rate -> rank/plan`.
</div>

## Prerequisites

- Installed package and working Python environment
- Input parquet or duckdb dataset aligned to source-data contract intent
- Optional: local Ollama service for LLM branches

## Happy path

### Baseline deterministic branch

```python
import honestroles as hr

# Read source data without strict validation at ingest boundary
jobs = hr.read_parquet("jobs_current.parquet", validate=False)

# Normalize then validate contract
jobs = hr.normalize_source_data_contract(jobs)
jobs = hr.validate_source_data_contract(jobs)

# Core processing
jobs = hr.clean_jobs(jobs)
jobs = hr.filter_jobs(jobs, remote_only=True, min_salary=100_000)
jobs = hr.label_jobs(jobs, use_llm=False)
jobs = hr.rate_jobs(jobs, use_llm=False)

profile = hr.CandidateProfile.mds_new_grad()
ranked = hr.rank_jobs(jobs, profile=profile, use_llm_signals=False, top_n=100)
plan = hr.build_application_plan(ranked, profile=profile, top_n=20)
```

### Choose this path if...

- Use deterministic path (`use_llm=False`) if you need reproducibility, CI consistency, or baseline benchmarking.
- Use LLM path if you need richer semantic enrichment and accept external service dependency.

Optional LLM branch:

```python
jobs = hr.label_jobs(jobs, use_llm=True, model="llama3")
jobs = hr.rate_jobs(jobs, use_llm=True, model="llama3")
ranked = hr.rank_jobs(jobs, profile=profile, use_llm_signals=True, model="llama3")
```

Expected output (success):

- processed DataFrame with derived label/rating columns
- ranked DataFrame with `fit_score`, `fit_breakdown`, `why_match`
- plan DataFrame with `next_actions`

## Failure modes

- Validation fails after read:
  - fix: run `normalize_source_data_contract` before validation
- Historical data behaves unexpectedly in standard clean path:
  - fix: use `clean_historical_jobs` for snapshot workflows
- LLM columns missing:
  - fix: verify Ollama availability and model pull

Failure example:

```text
ValueError: required columns are missing: ...
```

## Related pages

- [Installation](installation.md)
- [End-to-End Pipeline](../guides/end_to_end_pipeline.md)
- [Output Columns by Stage](../guides/output_columns.md)
- [Troubleshooting](../guides/troubleshooting.md)

<div class="hr-next-steps">
  <h2>Next actions</h2>
  <ul>
    <li>Run an expanded scenario in <a href="../guides/end_to_end_pipeline.md">End-to-End Pipeline</a>.</li>
    <li>Audit schema changes with <a href="../guides/output_columns.md">Output Columns by Stage</a>.</li>
  </ul>
</div>
