#!/usr/bin/env python3
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


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


def slugify(heading: str) -> str:
    text = heading.strip().lower()
    text = re.sub(r"[^a-z0-9 _-]", "", text)
    text = re.sub(r"\s+", "-", text)
    text = re.sub(r"-+", "-", text)
    return text.strip("-")


def collect_anchors(path: Path) -> set[str]:
    anchors: set[str] = set()
    counts: dict[str, int] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        match = re.match(r"^#{1,6}\s+(.+)$", line)
        if not match:
            continue
        base = slugify(match.group(1))
        if not base:
            continue
        seen = counts.get(base, 0)
        counts[base] = seen + 1
        anchor = base if seen == 0 else f"{base}-{seen}"
        anchors.add(anchor)
    return anchors


def resolve_link(source: Path, target: str) -> tuple[Path, str | None]:
    if "#" in target:
        file_part, anchor = target.split("#", 1)
        anchor = anchor.strip()
    else:
        file_part, anchor = target, None

    file_part = file_part.strip()
    if not file_part:
        return source, anchor

    if file_part.startswith("/"):
        resolved = ROOT / file_part.lstrip("/")
    else:
        resolved = (source.parent / file_part).resolve()
    return resolved, anchor


def main() -> int:
    link_re = re.compile(r"(?<!!)\[[^\]]+\]\(([^)]+)\)")
    errors: list[str] = []
    anchor_cache: dict[Path, set[str]] = {}

    for source in iter_markdown_files():
        text = source.read_text(encoding="utf-8")
        for raw_target in link_re.findall(text):
            target = raw_target.strip()
            if not target:
                continue
            if target.startswith(("http://", "https://", "mailto:", "tel:")):
                continue

            resolved, anchor = resolve_link(source, target)
            if not resolved.exists():
                errors.append(f"{source}: broken link target '{target}'")
                continue

            if anchor:
                if resolved.suffix.lower() != ".md":
                    continue
                anchors = anchor_cache.setdefault(resolved, collect_anchors(resolved))
                if anchor not in anchors:
                    errors.append(
                        f"{source}: missing anchor '#{anchor}' in '{resolved.relative_to(ROOT)}'"
                    )

    if errors:
        print("Markdown link check failed:")
        for err in errors:
            print(f" - {err}")
        return 1

    print("Markdown link check passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
