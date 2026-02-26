from __future__ import annotations

from pathlib import Path
import re


REPO_ROOT = Path(__file__).resolve().parents[1]


def _read(path: str) -> str:
    return (REPO_ROOT / path).read_text(encoding="utf-8")


def test_homepage_uses_hero_and_task_cards() -> None:
    docs_index = _read("docs/index.md")

    assert "hr-hero" in docs_index
    assert "hr-card-grid" in docs_index
    assert "Install HonestRoles" in docs_index
    assert "Workflows" in docs_index


def test_homepage_is_not_raw_backticked_path_list() -> None:
    docs_index = _read("docs/index.md")

    assert "`start/installation.md`" not in docs_index
    assert "`guides/cli.md`" not in docs_index
    assert "`reference/api/reference.md`" not in docs_index


def test_guides_landing_exists_and_links_core_flows() -> None:
    guides_index = _read("docs/guides/index.md")

    assert "CLI Operations" in guides_index
    assert "End-to-End Processing" in guides_index
    assert "Troubleshooting" in guides_index
    assert "LLM Operations" in guides_index
    assert "href=\"cli.md\"" in guides_index
    assert "href=\"end_to_end_pipeline.md\"" in guides_index
    assert "href=\"troubleshooting.md\"" in guides_index
    assert "href=\"llm_operations.md\"" in guides_index


def test_custom_404_supports_md_path_redirects() -> None:
    not_found_doc = _read("docs/404.md")

    assert "<script>" in not_found_doc
    assert "path.endsWith(\".md\")" in not_found_doc
    assert "replace(/\\/index\\.md$/, \"/\")" in not_found_doc
    assert "replace(/\\.md$/, \"/\")" in not_found_doc
    assert "path.endsWith(\"/index\")" in not_found_doc
    assert "window.location.search" in not_found_doc
    assert "window.location.hash" in not_found_doc
    assert "[Installation](start/installation.md)" in not_found_doc


def test_custom_404_redirect_examples() -> None:
    def normalize(path: str) -> str:
        if path.endswith(".md"):
            path = re.sub(r"/index\.md$", "/", path)
            return re.sub(r"\.md$", "/", path)
        if path.endswith("/index"):
            return f"{path}/"
        return path

    assert normalize("/honestroles/start/installation.md") == "/honestroles/start/installation/"
    assert normalize("/honestroles/guides/index.md") == "/honestroles/guides/"
    assert normalize("/honestroles/reference/api/index") == "/honestroles/reference/api/index/"
    assert normalize("/honestroles/start/quickstart/") == "/honestroles/start/quickstart/"
