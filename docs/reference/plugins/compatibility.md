# Plugin Compatibility Policy

## Versioning

`honestroles` plugin compatibility is governed by plugin API versions, not by plugin package versions.

- Core exposes `SUPPORTED_PLUGIN_API_VERSION` (currently `1.0`)
- Plugins declare `PluginSpec.api_version`

## Compatibility Rules

Registration is allowed only when:

1. Plugin major version equals core major version.
2. Plugin minor version is less than or equal to core minor version.

Examples with core `1.0`:

- `1.0`: compatible
- `1`: compatible (normalized to `1.0`)
- `1.1`: incompatible (plugin requires newer API)
- `2.0`: incompatible (breaking major mismatch)

## Deprecation

When introducing behavior changes:

1. Add warnings in one minor release.
2. Keep previous behavior working through the deprecation window.
3. Remove only in next major version.

## Plugin Maintainer Checklist

Before releasing a plugin:

1. Pin and test against supported `honestroles` versions.
2. Declare `PluginSpec` explicitly.
3. Run plugin contract tests with representative fixtures.
4. Avoid mutating input DataFrames in-place unless explicitly documented.

## Core Maintainer Checklist

Before releasing `honestroles`:

1. Run plugin compatibility tests.
2. Run the non-performance repository coverage gate at 100%.
3. Review any API changes impacting plugin signatures.
4. Update `docs/reference/plugins/api_contract.md` and migration notes.
5. Bump major version for plugin API breaking changes.
