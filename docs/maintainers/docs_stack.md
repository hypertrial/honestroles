# Docs Stack

This repository’s documentation stack is:

- Static site generator: `MkDocs`
- Theme/UI: `Material for MkDocs`
- API docs generation: `mkdocstrings[python]`
- Markdown extensions: `pymdown-extensions` (tabs, superfences, emoji/icons, etc.)
- Custom styling: repository CSS at `stylesheets/extra.css`
- Hosting/deploy: GitHub Pages via GitHub Actions artifact deploy (`configure-pages` + `upload-pages-artifact` + `deploy-pages`), branchless mode
- Config entrypoint: `mkdocs.yml`
- Docs publishing workflow: `.github/workflows/docs-pages.yml`
