# Objects

Yes. In `honestroles`, job description and role data should be the fundamental object.

## Core Object

1. `JobRoleRecord` (fundamental object)
   A single normalized job-role record representing one role posting at one point in time.
2. Required identity fields
   `job_key`, `company`, `source`, `job_id`, `title`, `location_raw`, `apply_url`, `ingested_at`, `content_hash`.
3. Enrichment and analysis fields
   Optional attributes such as location components, salary fields, extracted skills/languages/benefits, labels, and scores.

## Why This Should Be Fundamental

1. It matches the library mission.
   `honestroles` exists to process job description data end to end.
2. It keeps pipeline stages composable.
   Cleaning, filtering, labeling, and rating all transform the same base record shape.
3. It makes validation and compatibility explicit.
   A stable core object enables predictable contract checks and safer versioning.
4. It supports extensibility.
   New columns can be added without replacing the core record model.

## Supporting Objects Around the Core

1. `Dataset`
   A collection of `JobRoleRecord` items (typically a DataFrame/table).
2. `FilterSpec`
   A declarative set of filter rules applied to a dataset.
3. `LabelOutput`
   Derived semantic outputs (for example: seniority, role category, tech stack).
4. `RatingOutput`
   Derived quality/completeness/composite scores.
5. `ContractValidationResult`
   Pass/fail status for required-field checks at I/O boundaries.

## Framework Rule

1. Every public processing API should accept and return the `JobRoleRecord` dataset shape (plus additive derived columns), rather than introducing alternate primary objects.
