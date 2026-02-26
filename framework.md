# HonestRoles Framework

## Purpose

`honestroles` exists to turn raw, inconsistent job-posting data into a reliable decision dataset for job seekers and downstream tools.

In practice, this package should make it easy to:

1. ingest messy source data safely,
2. standardize it into a known contract,
3. enrich and score jobs with transparent logic,
4. rank opportunities for a specific candidate profile,
5. preserve reproducibility across runs.

## Problem We Are Solving

Job data from boards, ATS exports, and scrapers is usually noisy:

- inconsistent field names and formats,
- missing or low-quality descriptions,
- duplicate postings,
- weak location/salary normalization,
- no clear way to compare opportunities.

`honestroles` is the canonical package that addresses this end-to-end in one composable pipeline.

## Core Workflow (Canonical Path)

The intended happy path is:

1. `read_*` with `validate=False` at ingest boundaries,
2. `normalize_source_data_contract`,
3. `validate_source_data_contract`,
4. `clean_jobs` (or `clean_historical_jobs`),
5. `filter_jobs` / `FilterChain`,
6. `label_jobs`,
7. `rate_jobs`,
8. `rank_jobs` + `build_application_plan`.

This keeps validation explicit, transformation stages modular, and ranking grounded in normalized data.

## Product Principles

1. Contract-first data handling: source-data contract defines required input shape.
2. Deterministic defaults: same input should produce same output unless LLM features are explicitly enabled.
3. Explainable scoring: labels/ratings/ranking should be inspectable, not opaque.
4. Composable APIs: users can run full pipeline or individual stages.
5. Schema centralization: use `honestroles.schema` constants, not hard-coded column names.
6. Backward compatibility: avoid breaking APIs in minor/patch releases.
7. Package-first ergonomics: installed package behavior must match source-tree behavior.

## Primary Interfaces

### Library API

Main interface for notebooks, ETL jobs, and custom pipelines.

### CLI

- `honestroles-report-quality`: quick data quality checks for parquet/duckdb inputs.
- `honestroles-scaffold-plugin`: generate plugin packages from built-in templates.

### Plugin System

External packages can register filter/label/rate plugins using the public plugin contract and entry points.

## What We Intentionally Do Not Optimize For

- Being a data collection/scraping framework.
- Replacing full ATS or applicant tracking workflows.
- Hidden magic defaults that reduce transparency.
- Breaking API churn in core clean/filter/label/rate/match operations.

## Quality Bar

To keep this package trustworthy and distribution-ready:

1. tests must cover behavior and public interfaces,
2. docs/examples must reflect real callable APIs,
3. wheel/sdist builds must include required runtime assets,
4. installed console scripts must work outside the repo root.

## Definition of Success

A user can install `honestroles`, run the documented pipeline on a raw dataset, and get:

- validated structured data,
- actionable quality diagnostics,
- ranked opportunities aligned with their profile,
- reproducible outputs they can trust.
