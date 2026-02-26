#!/usr/bin/env python3
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

REQUIRED_TASK_SECTIONS = [
    "## When to use",
    "## Prerequisites",
    "## Steps",
    "## Expected result",
    "## Next steps",
]


def iter_markdown_files() -> list[Path]:
    extra_files = [
        "README.md",
        "CHANGELOG.md",
        "CONTRIBUTING_PLUGIN.md",
        "examples/README.md",
        "plugin_template/README.md",
    ]
    files: list[Path] = []
    for rel in extra_files:
        path = ROOT / rel
        if path.exists():
            files.append(path)
    files.extend(sorted((ROOT / "docs").rglob("*.md")))
    return files


def check_file(path: Path) -> list[str]:
    errors: list[str] = []
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines()

    in_fence = False
    for idx, line in enumerate(lines, start=1):
        if line.endswith(" "):
            errors.append(f"{path}: trailing whitespace at line {idx}")
        if "\t" in line:
            errors.append(f"{path}: tab character at line {idx}")

        fence_match = re.match(r"^```(.*)$", line)
        if fence_match:
            fence_tail = fence_match.group(1).strip()
            if in_fence:
                in_fence = False
            else:
                if not fence_tail:
                    errors.append(f"{path}: code fence missing language at line {idx}")
                in_fence = True

    if in_fence:
        errors.append(f"{path}: unclosed code fence")

    heading_levels: list[tuple[int, int]] = []
    for idx, line in enumerate(lines, start=1):
        match = re.match(r"^(#{1,6})\s+.+$", line)
        if match:
            heading_levels.append((len(match.group(1)), idx))

    for (prev_level, prev_line), (level, line) in zip(heading_levels, heading_levels[1:]):
        if level > prev_level + 1:
            errors.append(
                f"{path}: heading level jumps from H{prev_level} to H{level} at line {line}"
            )

    rel = path.relative_to(ROOT).as_posix()
    if rel.startswith("docs/getting-started/") or rel.startswith("docs/guides/"):
        positions: list[int] = []
        for section in REQUIRED_TASK_SECTIONS:
            try:
                pos = lines.index(section)
            except ValueError:
                errors.append(f"{path}: missing required section '{section}'")
                continue
            positions.append(pos)
        if len(positions) == len(REQUIRED_TASK_SECTIONS) and positions != sorted(positions):
            errors.append(f"{path}: required sections are not in the mandated order")

    return errors


def main() -> int:
    files = iter_markdown_files()
    errors: list[str] = []
    for path in files:
        errors.extend(check_file(path))

    if errors:
        print("Markdown style check failed:")
        for err in errors:
            print(f" - {err}")
        return 1

    print("Markdown style check passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
