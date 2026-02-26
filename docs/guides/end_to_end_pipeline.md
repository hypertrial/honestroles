# End-to-End Pipeline

## When to use this

Use this guide when you need a realistic production-like workflow from raw source records to ranked job targets.

<div class="hr-callout">
  <strong>At a glance:</strong> run contract-first ingest, deterministic enrichment, optional LLM augmentation, then ranking and action planning.
</div>

## Prerequisites

- Installed package and accessible input dataset
- Familiarity with source-data contract fields
- Optional Ollama runtime for LLM branches

## Happy path

```python
import honestroles as hr

# Ingest boundary
jobs = hr.read_parquet("jobs_current.parquet", validate=False)
jobs = hr.normalize_source_data_contract(jobs)
jobs = hr.validate_source_data_contract(jobs)

# Core processing
jobs = hr.clean_jobs(jobs)
jobs = hr.filter_jobs(
    jobs,
    remote_only=True,
    min_salary=100_000,
    employment_types=["full_time", "contract"],
)

# Deterministic baseline
jobs = hr.label_jobs(jobs, use_llm=False)
jobs = hr.rate_jobs(jobs, use_llm=False)

# Optional LLM branch (enable when semantic enrichment is needed)
# jobs = hr.label_jobs(jobs, use_llm=True, model="llama3")
# jobs = hr.rate_jobs(jobs, use_llm=True, model="llama3")

profile = hr.CandidateProfile.mds_new_grad()
ranked = hr.rank_jobs(jobs, profile=profile, use_llm_signals=False, top_n=100)
plan = hr.build_application_plan(ranked, profile=profile, top_n=20)

hr.write_parquet(jobs, "out/jobs_scored.parquet")
hr.write_parquet(ranked, "out/jobs_ranked.parquet")
hr.write_parquet(plan, "out/jobs_application_plan.parquet")
```

Validate outputs with CLI:

```bash
honestroles-report-quality out/jobs_scored.parquet --format text
```

Expected output (success):

- scored dataset persisted to `out/jobs_scored.parquet`
- ranked dataset with fit fields and reasons
- plan dataset with `next_actions`

Choose this path if...

- You need reproducible outputs: keep all LLM flags disabled.
- You need semantic enrichment depth: enable LLM at label/rate/rank stages and monitor quality deltas.

## Failure modes

- Validation errors after read:
  - fix with explicit normalize -> validate sequence
- Unexpected historical behavior:
  - use `clean_historical_jobs` for snapshot datasets
- Weak ranking quality:
  - inspect `fit_breakdown`, `missing_requirements`, and extracted signal columns before retuning filters

Failure example:

```text
ValueError: required columns are missing: ...
```

## Related pages

- [Contract-First Quickstart](../start/quickstart.md)
- [Output Columns by Stage](output_columns.md)
- [Match Reference](../reference/match.md)
- [Troubleshooting](troubleshooting.md)

<div class="hr-next-steps">
  <h2>Next actions</h2>
  <ul>
    <li>Audit stage-level schema changes in <a href="output_columns.md">Output Columns by Stage</a>.</li>
    <li>If failures appear, jump to <a href="troubleshooting.md">Troubleshooting</a>.</li>
  </ul>
</div>
