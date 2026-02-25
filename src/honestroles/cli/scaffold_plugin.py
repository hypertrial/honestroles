from __future__ import annotations

import argparse
from collections.abc import Iterator
from contextlib import contextmanager
from importlib.resources import as_file, files
from importlib.abc import Traversable
import re
import shutil
import sys
from pathlib import Path


def _sanitize_distribution_name(name: str) -> str:
    value = name.strip().lower()
    value = re.sub(r"[^a-z0-9-]+", "-", value)
    value = re.sub(r"-{2,}", "-", value).strip("-")
    if not value:
        raise ValueError("Distribution name must contain letters or numbers.")
    return value


def _sanitize_package_name(name: str) -> str:
    value = name.strip().lower()
    value = re.sub(r"[^a-z0-9_]+", "_", value)
    value = re.sub(r"_{2,}", "_", value).strip("_")
    if not value:
        raise ValueError("Package name must contain letters or numbers.")
    if value[0].isdigit():
        value = f"plugin_{value}"
    return value


def _default_package_name(distribution_name: str) -> str:
    base = distribution_name.replace("-", "_")
    if base.startswith("honestroles_plugin_"):
        return base
    if base.startswith("honestroles_"):
        return f"honestroles_plugin_{base[len('honestroles_'):]}"
    return f"honestroles_plugin_{base}"


def _plugin_prefix(distribution_name: str) -> str:
    trimmed = distribution_name
    if trimmed.startswith("honestroles-plugin-"):
        trimmed = trimmed[len("honestroles-plugin-") :]
    elif trimmed.startswith("honestroles_plugin_"):
        trimmed = trimmed[len("honestroles_plugin_") :]
    prefix = _sanitize_package_name(trimmed or "custom")
    return prefix


def _text_file(path: Path) -> bool:
    return path.suffix in {".md", ".py", ".toml", ".txt", ".yml", ".yaml"}


def _packaged_template_root() -> Traversable | None:
    candidate = files("honestroles").joinpath("_templates").joinpath("plugin_template")
    return candidate if candidate.is_dir() else None


def _filesystem_template_candidates(
    *,
    this_file: Path | None = None,
    cwd: Path | None = None,
) -> list[Path]:
    resolved_file = this_file or Path(__file__).resolve()
    resolved_cwd = cwd or Path.cwd()
    candidates = [resolved_file.parents[3] / "plugin_template", resolved_cwd / "plugin_template"]
    deduped: list[Path] = []
    seen: set[Path] = set()
    for candidate in candidates:
        resolved = candidate.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        deduped.append(resolved)
    return deduped


@contextmanager
def _template_root_context() -> Iterator[Path]:
    packaged_search_target = "package:honestroles/_templates/plugin_template"
    searched: list[str] = [packaged_search_target]
    packaged = _packaged_template_root()
    if packaged is not None:
        with as_file(packaged) as path:
            yield path
            return

    for candidate in _filesystem_template_candidates():
        searched.append(str(candidate))
        if candidate.is_dir():
            yield candidate
            return

    searched_msg = ", ".join(searched) if searched else "<none>"
    raise FileNotFoundError(
        "Template directory not found. Expected 'plugin_template' in package data or repository root. "
        f"Searched: {searched_msg}"
    )


def scaffold_plugin(
    *,
    distribution_name: str,
    package_name: str | None,
    output_dir: Path,
    force: bool,
) -> Path:
    with _template_root_context() as template_root:
        dist = _sanitize_distribution_name(distribution_name)
        package = _sanitize_package_name(package_name or _default_package_name(dist))
        prefix = _plugin_prefix(dist)

        destination = output_dir / dist
        if destination.exists():
            if not force:
                raise FileExistsError(
                    f"Destination already exists: {destination}. Use --force to overwrite."
                )
            shutil.rmtree(destination)

        shutil.copytree(template_root, destination)

        src_old = destination / "src" / "honestroles_plugin_example"
        src_new = destination / "src" / package
        if src_old.exists() and src_old != src_new:
            src_old.rename(src_new)

        replacements = {
            "honestroles-plugin-example": dist,
            "honestroles_plugin_example": package,
            "example_filter": f"{prefix}_filter",
            "example_label": f"{prefix}_label",
            "example_rate": f"{prefix}_rate",
            "only_remote": f"{prefix}_only_remote",
            "add_source_group": f"{prefix}_add_source_group",
            "add_priority_rating": f"{prefix}_add_priority_rating",
        }

        for path in destination.rglob("*"):
            if not path.is_file() or not _text_file(path):
                continue
            text = path.read_text(encoding="utf-8")
            updated = text
            for old, new in replacements.items():
                updated = updated.replace(old, new)
            if updated != text:
                path.write_text(updated, encoding="utf-8")

        return destination


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Scaffold a new HonestRoles plugin package from plugin_template."
    )
    parser.add_argument(
        "--name",
        required=True,
        help="Plugin distribution name (for example honestroles-plugin-myorg).",
    )
    parser.add_argument(
        "--package",
        default=None,
        help="Optional Python package name. Defaults to derived honestroles_plugin_<name>.",
    )
    parser.add_argument(
        "--output-dir",
        default=".",
        help="Directory to place the generated plugin package.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite destination if it already exists.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        destination = scaffold_plugin(
            distribution_name=args.name,
            package_name=args.package,
            output_dir=Path(args.output_dir).resolve(),
            force=args.force,
        )
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    print(f"Scaffold created at: {destination}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
