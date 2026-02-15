# Docs Stack

This repository’s documentation stack is:

- Static site generator: `MkDocs`
- Theme/UI: `Material for MkDocs`
- API docs generation: `mkdocstrings[python]`
- Markdown extensions: `pymdown-extensions` (tabs, superfences, emoji/icons, etc.)
- Custom styling: repository CSS at `stylesheets/extra.css`
- Quality checks (CI): `pymarkdownlnt`, `codespell`, `lychee`, plus `mkdocs build --strict`
- Hosting/deploy: GitHub Pages via GitHub Actions artifact deploy (`configure-pages` + `upload-pages-artifact` + `deploy-pages`), branchless mode
- Config entrypoint: `mkdocs.yml`
- Docs checks workflow: `.github/workflows/docs-check.yml`
- Docs publishing workflow: `.github/workflows/docs-pages.yml`
