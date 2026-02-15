# Contributing Plugins

This guide defines the preferred process for contributing plugin-related changes.

## Scope

Plugin contributions include:

- external plugin packages
- plugin API changes
- plugin loading and compatibility logic
- plugin docs and templates

## Contribution Types

## 1. New Plugin Submission

1. Build from `plugin_template/`.
2. Add tests and README in your plugin repo.
3. Open a PR updating `plugins-index/plugins.toml` with metadata.

## 2. Core Plugin Runtime Change

1. Open issue first if behavior change is non-trivial.
2. Include compatibility impact notes.
3. Add/adjust contract tests.
4. Update plugin docs.

## 3. Plugin API Change

1. Mark PR with `plugin-api`.
2. Add migration notes.
3. Preserve backward behavior unless major release.
4. Add compatibility matrix tests.

## Review Checklist

PRs should demonstrate:

1. Correct output types for plugin kind.
2. Deterministic behavior for fixed fixtures.
3. Explicit API compatibility declaration.
4. Clear failure messages for invalid inputs.
5. Docs updates where relevant.

## Quality Bar

- No breaking plugin API changes without a major version plan.
- No silent fallback when compatibility check fails.
- New plugin loader behavior must be covered by tests.
