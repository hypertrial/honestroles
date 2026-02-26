# Output Columns by Stage

## Purpose

This page defines how columns evolve across processing stages so pipelines can depend on stable contracts.

## Public API / Interface

Required vs additive behavior:

- Required source fields are defined by `schema.REQUIRED_COLUMNS`.
- Stages should preserve existing fields and add derived columns.
- Filtering changes rows, not schema guarantees.

Stage-by-stage contract summary:

| Stage | Row Effect | Column Effect |
|---|---|---|
| Normalize/Validate | No intentional row drop | Normalize formats and enforce required source fields |
| Clean | May drop obvious duplicates | Normalizes text/location/salary and canonical fields |
| Filter | Reduces row count | Usually no required column changes |
| Label | Keeps filtered rows | Adds `seniority`, `role_category`, `tech_stack`, optional `llm_labels` |
| Rate | Keeps labeled rows | Adds `completeness_score`, `quality_score`, optional `quality_score_llm`, `quality_reason_llm`, and `rating` |
| Match | Produces ranked subset when `top_n` applied | Adds `fit_score`, `fit_breakdown`, `missing_requirements`, `why_match`, `next_actions`, and signal columns |

Common match/signal columns:

- `required_skills_extracted`, `preferred_skills_extracted`
- `experience_years_min`, `experience_years_max`, `entry_level_likely`
- `visa_sponsorship_signal`, `application_friction_score`, `role_clarity_score`
- `signal_confidence`, `signal_source`, `signal_reason`

## Usage Example

```python
from honestroles import schema
import honestroles as hr

df = hr.read_parquet("jobs_current.parquet", validate=False)
df = hr.normalize_source_data_contract(df)
df = hr.validate_source_data_contract(df)

# Required fields should still be present after processing
required = sorted(schema.REQUIRED_COLUMNS)

processed = hr.clean_jobs(df)
processed = hr.label_jobs(processed, use_llm=False)
processed = hr.rate_jobs(processed, use_llm=False)
```

## Edge Cases and Errors

- Plugin transforms may add columns, but should not remove required core columns.
- Optional LLM fields only appear when LLM-enabled branches run.
- `top_n` in ranking limits row count; preserve full scored dataset separately when needed.

## Related Pages

- [Schema Reference](../reference/schema.md)
- [Source Data Contract](../reference/source_data_contract_v1.md)
- [Label Reference](../reference/label.md)
- [Rate Reference](../reference/rate.md)
- [Match Reference](../reference/match.md)
