# End-to-End Pipeline

## Purpose

This guide shows a full, realistic `honestroles` pipeline from raw source records to ranked opportunities and next actions.

## Public API / Interface

Canonical sequence:

1. `read_*` with `validate=False`
2. `normalize_source_data_contract`
3. `validate_source_data_contract`
4. `clean_jobs`
5. `filter_jobs`
6. `label_jobs`
7. `rate_jobs`
8. `rank_jobs` + `build_application_plan`

Outputs:

- cleaned/scored parquet dataset
- ranked dataframe with match fields
- application plan dataframe
- quality report from CLI

## Usage Example

```python
import honestroles as hr

# Read and normalize raw source data
jobs = hr.read_parquet("jobs_current.parquet", validate=False)
jobs = hr.normalize_source_data_contract(jobs)
jobs = hr.validate_source_data_contract(jobs)

# Process core stages
jobs = hr.clean_jobs(jobs)
jobs = hr.filter_jobs(
    jobs,
    remote_only=True,
    min_salary=100_000,
    employment_types=["full_time", "contract"],
)

# Non-LLM deterministic branch
jobs = hr.label_jobs(jobs, use_llm=False)
jobs = hr.rate_jobs(jobs, use_llm=False)

# Optional LLM branch (requires local Ollama)
# jobs = hr.label_jobs(jobs, use_llm=True, model="llama3")
# jobs = hr.rate_jobs(jobs, use_llm=True, model="llama3")

profile = hr.CandidateProfile.mds_new_grad()
ranked = hr.rank_jobs(jobs, profile=profile, use_llm_signals=False, top_n=100)
plan = hr.build_application_plan(ranked, profile=profile, top_n=20)

hr.write_parquet(jobs, "out/jobs_scored.parquet")
hr.write_parquet(ranked, "out/jobs_ranked.parquet")
hr.write_parquet(plan, "out/jobs_application_plan.parquet")
```

Inspect quality report output:

```bash
honestroles-report-quality out/jobs_scored.parquet --format text
```

## Edge Cases and Errors

- Always normalize before strict validation to avoid avoidable format failures.
- Use `clean_historical_jobs` for historical snapshot workflows.
- If ranking appears weak, inspect `fit_breakdown`, `missing_requirements`, and extracted signal columns before changing filters.
- Keep `use_llm=False` for reproducible baseline runs in CI and benchmarking.

## Related Pages

- [Quickstart](../start/quickstart.md)
- [Output Columns](output_columns.md)
- [Match Reference](../reference/match.md)
- [Source Data Contract](../reference/source_data_contract_v1.md)
