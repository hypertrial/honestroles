# HonestRoles Library Framework

`honestroles` is a Python PyPI package under the MIT license and is the canonical place for all job-description data processing logic.

1. Own the data-processing domain.
   The library is responsible for ingestion-adjacent validation, cleaning, normalization, filtering, labeling, and rating of job-description data.
2. Treat the source-data contract as input truth.
   All incoming datasets must follow the source-data contract, including required core fields and tolerant handling of extra fields.
3. Keep schema references centralized.
   All code should use `honestroles.schema` constants instead of hard-coded column strings to avoid drift and support safe refactors.
4. Design for composable pipelines.
   Public APIs should remain modular (`clean`, `filter`, `label`, `rate`, `io`) so users can run full pipelines or individual stages without side effects.
5. Validate early, fail clearly.
   I/O boundaries must validate required columns and raise explicit, actionable errors when contract expectations are not met.
6. Preserve backward compatibility in minor releases.
   Existing public functions, expected column names, and default behaviors should not break without a major version change and migration notes.
7. Prioritize deterministic outputs.
   Transformations should be reproducible given the same input, with stable behavior across environments and runs.
8. Keep unknown fields as pass-through data.
   Extra columns from upstream sources should be retained unless there is a documented reason to drop them.
9. Enforce quality gates.
   Every behavior change should include tests, and docs should be updated when schema, API, or pipeline expectations change.
10. Ship as a reusable package.
    Repository changes should maintain PyPI package readiness: clean module boundaries, documented public API, semantic versioning, and MIT licensing continuity.
