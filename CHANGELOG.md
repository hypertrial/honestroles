# Changelog

## Unreleased

- Added pipeline-configurable input aliasing via `[input.aliases]` with diagnostics in `input_aliasing`.
- Added runtime quality profiles via `[runtime.quality]` and optional per-field weight overrides.
- Changed `report-quality`/`build_data_quality_report` semantics: `score_percent` is now weighted profile-based (not equal-weight mean-null).
- Added profile metadata in quality output: `profile`, `effective_weights`, `weighted_null_percent`.

## 0.1.0

- Hard architectural rewrite to explicit runtime + manifest-driven plugin registry.
- Removed process-global plugin registration API.
- Migrated runtime data model to Polars-only.
- Added config-driven CLI (`honestroles`) with run/validate/report flows.
- Rebuilt deterministic and fuzz test suites around new runtime contracts.
