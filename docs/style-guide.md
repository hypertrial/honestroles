# Documentation Style Guide

Use this guide when adding or changing user-facing docs.

## Page Contract

Each task-oriented page should follow this order:

1. `When to use`
2. `Prerequisites`
3. `Steps`
4. `Expected result`
5. `Next steps`

Reference pages may use schema-first sections instead.

## Terminology

Use these terms consistently:

- `pipeline config`
- `plugin manifest`
- `runtime`
- `stage`
- `diagnostics`

Avoid aliases like "config file" or "plugin list" when the exact contract matters.

## Snippet Rules

- Every fenced block must include a language.
- CLI commands use `bash` and start with `$`.
- TOML snippets should be complete and minimal.
- Python API snippets include imports and result inspection.

## UX Writing Rules

- Open with a short lead paragraph.
- Use short sections and scannable headings.
- Add explicit failure guidance with `!!! warning` blocks where relevant.
- Cross-link to one reference page and one troubleshooting page from every guide.

## Quality Gates

Docs changes must pass:

- `bash scripts/check_docs_refs.sh`
- `python scripts/check_markdown_style.py`
- `python scripts/check_markdown_links.py`
- `pytest tests/docs -q`
