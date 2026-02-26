# Compatibility and Versioning

## Purpose

This page defines compatibility expectations for `honestroles` releases and how breaking changes are handled.

## Public API / Interface

Default compatibility policy:

- patch/minor releases should remain non-breaking for documented Python APIs and CLI commands
- major releases may include intentional breaking changes with migration notes

Changes considered breaking:

- Python API signature removals/renames or incompatible behavior changes
- CLI flag removals/renames or incompatible output contract changes
- required source-schema/contract changes without migration guidance

Deprecation expectations:

- announce deprecations before removal
- provide migration instructions in docs/changelog
- avoid silent behavior shifts for core pipeline defaults

Versioning relation:

- package version is defined in `src/honestroles/__about__.py`
- release tags must match `v<major>.<minor>.<patch>`
- changelog should capture user-visible behavior and interface changes

## Usage Example

Before introducing an API change:

1. evaluate whether it is additive vs breaking,
2. add tests for old and new behavior where applicable,
3. update docs and changelog entries,
4. defer removals to an appropriate major release.

## Edge Cases and Errors

- Changing required source contract fields is breaking unless migration support is documented.
- Reinterpreting CLI defaults can be breaking if users depend on previous behavior.
- Plugin API contract changes require compatibility notes for external plugin packages.

## Related Pages

- [Framework](framework.md)
- [Source Data Contract](../reference/source_data_contract_v1.md)
- [Plugin Compatibility](../reference/plugins/compatibility.md)
- Changelog: `CHANGELOG.md`
