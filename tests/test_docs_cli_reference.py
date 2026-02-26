from __future__ import annotations

import re
from pathlib import Path

from honestroles.cli.report_data_quality import build_parser as build_report_parser
from honestroles.cli.scaffold_plugin import build_parser as build_scaffold_parser


def _long_option_flags(parser) -> set[str]:
    flags: set[str] = set()
    for action in parser._actions:
        for option in action.option_strings:
            if option.startswith("--"):
                flags.add(option)
    return flags


def test_cli_docs_cover_parser_flags_and_no_unknown_flags() -> None:
    root = Path(__file__).resolve().parents[1]
    docs_text = (root / "docs" / "guides" / "cli.md").read_text(encoding="utf-8")

    scaffold_flags = _long_option_flags(build_scaffold_parser())
    report_flags = _long_option_flags(build_report_parser())
    parser_flags = scaffold_flags | report_flags

    documented_flags = set(re.findall(r"`(--[a-z0-9-]+)`", docs_text))

    assert parser_flags <= documented_flags
    assert documented_flags <= parser_flags


def test_cli_docs_cover_report_input_positional() -> None:
    root = Path(__file__).resolve().parents[1]
    docs_text = (root / "docs" / "guides" / "cli.md").read_text(encoding="utf-8")

    assert "`input`" in docs_text
