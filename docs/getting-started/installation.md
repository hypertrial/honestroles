# Installation

Install HonestRoles in a virtual environment and verify the CLI is available.

If you only need the hosted product experience, use the app directly at [https://honestroles.com](https://honestroles.com).

## When to use

Use this page before running any pipeline.

## Prerequisites

- Python 3.10+
- A shell with `python` and `pip`

## Steps

```bash
$ python -m venv .venv
$ . .venv/bin/activate
$ python -m pip install --upgrade pip
$ pip install honestroles
$ honestroles --help
```

For local development:

```bash
$ pip install -e ".[dev,docs]"
```

## Expected result

You should see `honestroles` commands including `run`, `plugins`, `config`, `report-quality`, and `scaffold-plugin`.

## Next steps

Go to [Quickstart (First Run)](quickstart-first-run.md).
