# Installation

## When to use this

Use this page when setting up `honestroles` for the first time or validating environment setup in CI/dev workflows.

<div class="hr-callout">
  <strong>At a glance:</strong> install package, run CLI sanity checks, then choose runtime-only or contributor setup.
</div>

## Prerequisites

- Python `>=3.10` (classifier targets: `3.10`, `3.11`, `3.12`)
- A virtual environment is strongly recommended.

## Happy path

### Runtime install

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install honestroles

honestroles-scaffold-plugin --help
honestroles-report-quality --help
```

Expected output (success):

```text
usage: honestroles-scaffold-plugin ...
usage: honestroles-report-quality ...
```

### Contributor/dev install

```bash
pip install -e ".[dev]"
pip install -e ".[docs]"
```

## Failure modes

- CLI command not found:
  - cause: wrong environment or PATH
  - fix: activate venv and verify with `python -m pip show honestroles`
- Backend import error during no-isolation build (`hatchling.build` not available):
  - cause: build backend not installed in active environment
  - fix: `pip install -e ".[dev]"` or `pip install build hatchling`
- Editable install not exposing expected command updates:
  - fix: reinstall with `pip install -e .`

Failure example:

```text
ERROR Backend 'hatchling.build' is not available.
```

## Related pages

- [Choose Your Entry Point](entry_points.md)
- [Contract-First Quickstart](quickstart.md)
- [CLI Guide](../guides/cli.md)
- [Maintainer Packaging](../maintainers/packaging.md)

<div class="hr-next-steps">
  <h2>Next actions</h2>
  <ul>
    <li>Pick usage mode in <a href="entry_points.md">Choose Your Entry Point</a>.</li>
    <li>Run the baseline pipeline in <a href="quickstart.md">Contract-First Quickstart</a>.</li>
  </ul>
</div>
