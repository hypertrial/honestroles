from __future__ import annotations

import re
from pathlib import Path


NEW_DOC_PATHS = [
    "start/installation.md",
    "start/entry_points.md",
    "guides/index.md",
    "guides/cli.md",
    "guides/troubleshooting.md",
    "guides/end_to_end_pipeline.md",
    "guides/output_columns.md",
    "guides/llm_operations.md",
    "concepts/architecture.md",
    "concepts/compatibility_and_versioning.md",
    "reference/faq.md",
    "maintainers/packaging.md",
    "maintainers/release_process.md",
    "maintainers/fuzzing.md",
]


def _nav_doc_paths() -> set[str]:
    root = Path(__file__).resolve().parents[1]
    mkdocs_text = (root / "mkdocs.yml").read_text(encoding="utf-8")
    _, nav_text = mkdocs_text.split("nav:", maxsplit=1)
    return set(re.findall(r"([a-zA-Z0-9_./-]+\.md)", nav_text))


def _top_nav_labels() -> list[str]:
    root = Path(__file__).resolve().parents[1]
    mkdocs_text = (root / "mkdocs.yml").read_text(encoding="utf-8")
    _, nav_text = mkdocs_text.split("nav:", maxsplit=1)
    return re.findall(r"^  - ([^:\n]+):\s*$", nav_text, flags=re.MULTILINE)


def test_new_docs_exist_and_are_in_mkdocs_nav() -> None:
    root = Path(__file__).resolve().parents[1]
    nav_paths = _nav_doc_paths()

    for rel in NEW_DOC_PATHS:
        assert (root / "docs" / rel).exists(), f"Missing docs file: docs/{rel}"
        assert rel in nav_paths, f"Missing docs nav entry: {rel}"


def test_docs_index_mentions_new_docs() -> None:
    root = Path(__file__).resolve().parents[1]
    index_text = (root / "docs" / "index.md").read_text(encoding="utf-8")

    for rel in NEW_DOC_PATHS:
        assert rel in index_text, f"Missing docs index entry: {rel}"


def test_top_nav_labels_match_task_first_structure() -> None:
    labels = _top_nav_labels()
    assert labels[:5] == [
        "Get Started",
        "Workflows",
        "Concepts",
        "Reference",
        "Maintainers",
    ]
