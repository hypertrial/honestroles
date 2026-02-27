# Changelog

## Unreleased

- Added `honestroles eda generate` for deterministic file-first EDA artifacts (`summary.json`, `report.md`, parquet tables, PNG figures).
- Added optional `honestroles eda dashboard` Streamlit wrapper that reads generated artifacts only (view layer, no profiling logic).
- Added pipeline-configurable input aliasing via `[input.aliases]` with diagnostics in `input_aliasing`.
- Added runtime quality profiles via `[runtime.quality]` and optional per-field weight overrides.
- Changed `report-quality`/`build_data_quality_report` semantics: `score_percent` is now weighted profile-based (not equal-weight mean-null).
- Added profile metadata in quality output: `profile`, `effective_weights`, `weighted_null_percent`.
- Removed legacy docs redirect pages and legacy script wrappers in favor of canonical docs IA and CLI commands.
- Raised minimum supported Python version to 3.11 and removed TOML loader compatibility fallback.

## 0.1.0

- Hard architectural rewrite to explicit runtime + manifest-driven plugin registry.
- Removed process-global plugin registration API.
- Migrated runtime data model to Polars-only.
- Added config-driven CLI (`honestroles`) with run/validate/report flows.
- Rebuilt deterministic and fuzz test suites around new runtime contracts.
