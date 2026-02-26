# Architecture

## Purpose

This page defines module boundaries and invariants for the `honestroles` data-processing architecture.

## Public API / Interface

Boundary map:

`io -> clean -> filter -> label -> rate -> match`

Boundary responsibilities:

- `io`: ingest/read/write, normalization, contract validation
- `clean`: canonical text/location/salary normalization and dedup foundations
- `filter`: deterministic row selection and policy filters
- `label`: derived semantic labels and optional LLM labels
- `rate`: completeness/quality/composite scoring
- `match`: profile-based ranking and action planning

Architecture invariants:

- contract validation happens at explicit boundaries
- schema constants are centralized in `honestroles.schema`
- defaults remain deterministic unless LLM features are explicitly enabled
- stages are composable and side-effect free on input objects

Extension points:

- plugin registries for filter/label/rate
- match component override hooks for custom scoring components

## Usage Example

```python
import honestroles as hr

df = hr.read_parquet("jobs_current.parquet", validate=False)
df = hr.normalize_source_data_contract(df)
df = hr.validate_source_data_contract(df)
df = hr.clean_jobs(df)
df = hr.filter_jobs(df, remote_only=True)
df = hr.label_jobs(df, use_llm=False)
df = hr.rate_jobs(df, use_llm=False)
ranked = hr.rank_jobs(df, profile=hr.CandidateProfile.mds_new_grad(), use_llm_signals=False)
```

## Edge Cases and Errors

- Skipping normalization before validation increases format/type failures.
- Mixing plugin side effects with core stage assumptions can make outputs non-reproducible.
- LLM-enabled branches should be treated as opt-in enrichment paths, not baseline behavior.

## Related Pages

- [Framework](framework.md)
- [Compatibility and Versioning](compatibility_and_versioning.md)
- [Plugin Overview](../reference/plugins.md)
- [Quickstart](../start/quickstart.md)
