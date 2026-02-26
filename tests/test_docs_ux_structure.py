from __future__ import annotations

from pathlib import Path


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
