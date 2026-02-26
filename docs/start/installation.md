# Installation

## Purpose

This page explains how to install `honestroles` for normal use, development, and docs work.

## Public API / Interface

Supported Python versions:

- Minimum: `>=3.10`
- Classifier targets: `3.10`, `3.11`, `3.12`

Install modes:

- Runtime install: `pip install honestroles`
- Development install: `pip install -e ".[dev]"`
- Docs install: `pip install -e ".[docs]"`

Sanity-check commands:

- `honestroles-scaffold-plugin --help`
- `honestroles-report-quality --help`

## Usage Example

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install honestroles

honestroles-scaffold-plugin --help
honestroles-report-quality --help
```

Development and docs setup:

```bash
pip install -e ".[dev]"
pip install -e ".[docs]"
```

## Edge Cases and Errors

- If `python -m build --no-isolation` fails with backend import errors, ensure build backend tooling is installed in the active environment (covered by `.[dev]`).
- If CLI commands are not found after install, confirm the environment is activated and run `python -m pip show honestroles`.
- If you are installing from source and commands are unavailable, reinstall with `pip install -e .`.

## Related Pages

- [Entry Points](entry_points.md)
- [Quickstart](quickstart.md)
- [CLI Guide](../guides/cli.md)
- [Maintainer Packaging](../maintainers/packaging.md)
