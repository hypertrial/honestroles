# HonestRoles Plugin Template

This folder provides a starter template for external `honestroles` plugins.

## Contents

- `pyproject.toml`: package metadata and entrypoint groups
- `src/honestroles_plugin_example/`: plugin implementation
- `tests/`: basic plugin contract tests

## Quick Start

1. Copy this folder into a new repository.
2. Rename `honestroles_plugin_example` package and plugin names.
3. Implement your plugin logic in `plugins.py`.
4. Run `pytest`.
5. Publish and register in `plugins-index/plugins.toml`.

## Loading

You can load this plugin with either:

- explicit call to `register_plugins()`
- `load_plugins_from_entrypoints()` if installed and entrypoints are configured
