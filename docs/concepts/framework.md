# HonestRoles Framework

## Purpose

`honestroles` exists to convert raw, inconsistent job-posting data into a reliable decision dataset for job seekers and downstream tooling.

The package should make it straightforward to:

1. ingest noisy data safely,
2. normalize it into a known contract,
3. enrich and score jobs with transparent logic,
4. rank opportunities for a candidate profile,
5. produce reproducible outputs.

## Problem We Solve

Real-world job datasets are usually inconsistent:

- mixed field formats and naming conventions,
- missing or low-quality descriptions,
- duplicate records,
- weak location/salary normalization,
- no consistent basis for comparing opportunities.

`honestroles` is the canonical package that solves this end-to-end through a composable processing pipeline.

## Canonical Workflow

The expected happy path is:

1. read data with `validate=False` at ingest boundaries,
2. `normalize_source_data_contract`,
3. `validate_source_data_contract`,
4. `clean_jobs` (or `clean_historical_jobs` for snapshots),
5. `filter_jobs` / `FilterChain`,
6. `label_jobs`,
7. `rate_jobs`,
8. `rank_jobs` + `build_application_plan`.

This sequence keeps validation explicit, stage boundaries clear, and ranking based on normalized data.

## Product Principles

1. Contract-first data handling.
   The [Source Data Contract](../reference/source_data_contract_v1.md) defines required shape and semantics.
2. Deterministic defaults.
   Given the same input, outputs should be stable unless LLM features are explicitly enabled.
3. Explainable scoring and ranking.
   Labels, ratings, and ranking signals should be inspectable and testable.
4. Composable public APIs.
   Users should be able to run full workflows or isolated stages without hidden side effects.
5. Centralized schema references.
   Use `honestroles.schema` constants instead of hard-coded column strings.
6. Backward compatibility by default.
   Minor/patch releases should not break existing Python APIs or CLI behavior.
7. Pass-through unknown fields.
   Extra upstream columns are retained unless there is a documented reason to drop them.

## Primary Interfaces

### Library API

Primary interface for notebooks, pipelines, and ETL jobs.

### CLI

- `honestroles-report-quality`: quick quality diagnostics for parquet/duckdb inputs.
- `honestroles-scaffold-plugin`: scaffold plugin packages from built-in templates.

### Plugin System

External packages can extend filter/label/rate behavior through the plugin contract and entry points.
See [Plugin API Contract](../reference/plugins/api_contract.md) and [Plugin Author Guide](../reference/plugins/author_guide.md).

## Non-goals

- Building or operating data collection/scraping infrastructure.
- Replacing full ATS/application tracking products.
- Introducing hidden heuristics that reduce transparency.
- Frequent breaking changes in clean/filter/label/rate/match interfaces.

## Quality Bar

Every meaningful behavior change should maintain:

1. test coverage for modified behavior and public interfaces,
2. docs and examples that match callable APIs,
3. packaging correctness (wheel/sdist include required runtime assets),
4. installed CLI behavior that works outside repository paths.

## Definition of Success

A user can install `honestroles`, run the documented pipeline, and obtain:

- validated structured job data,
- actionable quality diagnostics,
- ranked opportunities aligned to a candidate profile,
- reproducible results they can trust.

## Related Documents

- [Quickstart](../start/quickstart.md)
- [API Reference](../reference/api/reference.md)
- [Source Data Contract](../reference/source_data_contract_v1.md)
