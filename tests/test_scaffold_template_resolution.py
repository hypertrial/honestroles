from __future__ import annotations

from pathlib import Path

import pytest

from honestroles.cli import scaffold_plugin as scaffold_cli


def test_template_root_context_prefers_packaged_template(monkeypatch) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    packaged_template = repo_root / "plugin_template"

    monkeypatch.setattr(scaffold_cli, "_packaged_template_root", lambda: packaged_template)
    monkeypatch.setattr(scaffold_cli, "_filesystem_template_candidates", lambda: [])

    with scaffold_cli._template_root_context() as resolved:
        assert resolved.resolve() == packaged_template.resolve()


def test_template_root_context_falls_back_to_filesystem(monkeypatch) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    expected = (repo_root / "plugin_template").resolve()

    monkeypatch.setattr(scaffold_cli, "_packaged_template_root", lambda: None)

    with scaffold_cli._template_root_context() as resolved:
        assert resolved.resolve() == expected


def test_template_root_context_error_lists_searched_paths(monkeypatch, tmp_path) -> None:
    missing_a = (tmp_path / "missing_a").resolve()
    missing_b = (tmp_path / "missing_b").resolve()

    monkeypatch.setattr(scaffold_cli, "_packaged_template_root", lambda: None)
    monkeypatch.setattr(scaffold_cli, "_filesystem_template_candidates", lambda: [missing_a, missing_b])

    with pytest.raises(FileNotFoundError) as exc_info:
        with scaffold_cli._template_root_context():
            pass

    message = str(exc_info.value)
    assert "Searched:" in message
    assert "package:honestroles/_templates/plugin_template" in message
    assert str(missing_a) in message
    assert str(missing_b) in message


def test_template_root_context_ignores_non_directory_candidates(monkeypatch, tmp_path) -> None:
    not_a_directory = tmp_path / "plugin_template"
    not_a_directory.write_text("not a directory", encoding="utf-8")

    monkeypatch.setattr(scaffold_cli, "_packaged_template_root", lambda: None)
    monkeypatch.setattr(scaffold_cli, "_filesystem_template_candidates", lambda: [not_a_directory])

    with pytest.raises(FileNotFoundError) as exc_info:
        with scaffold_cli._template_root_context():
            pass

    message = str(exc_info.value)
    assert str(not_a_directory.resolve()) in message
