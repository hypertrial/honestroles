#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

try:
    from honestroles.cli.main import main as cli_main
except ModuleNotFoundError as exc:
    if exc.name != "honestroles":
        raise
    repo_src = Path(__file__).resolve().parents[1] / "src"
    sys.path.insert(0, str(repo_src))
    from honestroles.cli.main import main as cli_main


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Deprecated wrapper around 'honestroles eda generate' for profiling "
            "jobs_historical parquet inputs."
        )
    )
    parser.add_argument("--input-parquet", default="jobs_historical.parquet")
    parser.add_argument(
        "--output-json",
        default="dist/jobs_historical_profile.json",
        help="Path to copied summary.json output.",
    )
    parser.add_argument(
        "--quality-profile",
        default="core_fields_weighted",
        choices=["core_fields_weighted", "equal_weight_all", "strict_recruiting"],
    )
    parser.add_argument("--quality-weight", action="append", default=[])
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--max-rows", type=int, default=None)
    parser.add_argument(
        "--manual-map-debug",
        action="store_true",
        help="Deprecated no-op; aliasing is handled by the runtime automatically.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()

    output_json = Path(args.output_json).expanduser().resolve()
    artifacts_dir = output_json.parent / f"{output_json.stem}_artifacts"

    print(
        "[DEPRECATED] scripts/profile_jobs_historical.py now delegates to "
        "'honestroles eda generate'.",
        file=sys.stderr,
    )
    if args.manual_map_debug:
        print(
            "[DEPRECATED] --manual-map-debug is ignored; aliasing diagnostics are always "
            "included in generated artifacts.",
            file=sys.stderr,
        )

    cli_args = [
        "eda",
        "generate",
        "--input-parquet",
        args.input_parquet,
        "--output-dir",
        str(artifacts_dir),
        "--quality-profile",
        args.quality_profile,
        "--top-k",
        str(args.top_k),
    ]

    if args.max_rows is not None:
        cli_args.extend(["--max-rows", str(args.max_rows)])
    for weight in args.quality_weight:
        cli_args.extend(["--quality-weight", weight])

    code = cli_main(cli_args)
    if code != 0:
        return code

    summary_path = artifacts_dir / "summary.json"
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(summary_path.read_text(encoding="utf-8"), encoding="utf-8")

    payload = json.loads(output_json.read_text(encoding="utf-8"))
    print(f"Wrote profile summary to {output_json}")
    print(
        json.dumps(
            {
                "rows": payload["shape"]["runtime"]["rows"],
                "quality_score_percent": payload["quality"]["score_percent"],
                "quality_profile": payload["quality"]["profile"],
                "findings": payload["findings"][:3],
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
