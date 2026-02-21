# Contract-First Quickstart

This quickstart shows the recommended flow for processing source data with `honestroles`.

## 1) Read source data

```python
import honestroles as hr

df = hr.read_parquet("jobs_current.parquet", validate=False)
```

## 2) Normalize contract shape

Use this step to normalize common source-data issues such as timestamp formats and list-like fields encoded as strings.

```python
df = hr.normalize_source_data_contract(df)
```

## 3) Validate contract

```python
df = hr.validate_source_data_contract(df)
```

## 4) Process data

```python
df = hr.clean_jobs(df)
df = hr.filter_jobs(df, remote_only=True, min_salary=100_000)
df = hr.label_jobs(df, use_llm=False)
df = hr.rate_jobs(df, use_llm=False)
```

## 5) Write output

```python
hr.write_parquet(df, "jobs_processed.parquet")
```

## 6) Rank for a candidate profile

```python
profile = hr.CandidateProfile.mds_new_grad()
ranked = hr.rank_jobs(df, profile=profile, use_llm_signals=False, top_n=100)
plan = hr.build_application_plan(ranked, profile=profile, top_n=20)
```
