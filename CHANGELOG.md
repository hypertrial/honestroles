# Changelog

## Unreleased

- Added `honestroles init` to scaffold `pipeline.toml` and `plugins.toml` from a sample parquet, including adapter inference when available.
- Added `honestroles doctor` to validate environment, config, schema readiness, required content signals, and output path setup with actionable fixes.
- Added `honestroles runs list/show` for local run lineage exploration.
- Added unified CLI output formatting with `--format {json,table}` for structured-output commands, while keeping JSON as the default.
- Added run lineage persistence in `.honestroles/runs/<run_id>/run.json` for runtime, adapter, and EDA execution commands.

## 0.1.3

- Added fast pre-commit hygiene gates (trailing whitespace, EOF, docs refs/style/links) across all GitHub Actions workflows.
- Added shared `scripts/run_precommit_fast.sh` to centralize workflow pre-commit execution.
- Hardened CI/docs/fuzz/release workflows with fail-fast pre-commit checks before heavier jobs.

## 0.1.2

- Fixed `honestroles adapter infer` date parsing inference to avoid Polars timezone schema conflicts while scoring parseability.
- Fixed `honestroles eda dashboard` table rendering to avoid requiring `pyarrow` in test/runtime environments.
- Updated release workflow to publish with PyPI API token secrets (`PYPI_API_KEY` or `PYPI_API_TOKEN`) instead of Trusted Publishing OIDC.

## 0.1.1

- Added EDA v2 commands: `honestroles eda diff` and `honestroles eda gate`.
- Added EDA rules file support (`--rules-file`) with deterministic gate/drift thresholds.
- Added source-attributed EDA outputs: `quality.by_source`, `consistency.by_source`, and `findings_by_source`.
- Added diff artifacts (`artifact_kind=\"diff\"`) with drift metrics (numeric PSI, categorical JSD) and gate evaluation payload.
- Added `honestroles eda generate` for deterministic file-first EDA artifacts (`summary.json`, `report.md`, parquet tables, PNG figures).
- Added optional `honestroles eda dashboard` Streamlit wrapper that reads generated artifacts only (view layer, no profiling logic).
- Added pipeline-configurable input aliasing via `[input.aliases]` with diagnostics in `input_aliasing`.
- Added declarative source adapter layer via `[input.adapter]` with typed coercion, conflict tracking, and diagnostics in `input_adapter`.
- Added `honestroles adapter infer` to generate draft adapter TOML fragments and inference reports from parquet inputs.
- Replaced dataframe-first runtime/plugin contracts with typed domain objects: `PipelineSpec`, `JobDataset`, `PipelineRun`, `RuntimeDiagnostics`, `ApplicationPlanEntry`, and `PluginDefinition`.
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
