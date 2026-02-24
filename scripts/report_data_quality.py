from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

import duckdb


def _bootstrap_repo_pythonpath() -> None:
    script_dir = Path(__file__).resolve().parent
    repo_src = script_dir.parent / "src"
    if repo_src.exists():
        repo_src_str = str(repo_src)
        if repo_src_str not in sys.path:
            sys.path.insert(0, repo_src_str)


def _load_io_api() -> tuple[object, object, object, object, object, object]:
    _bootstrap_repo_pythonpath()
    from honestroles.io import (  # noqa: PLC0415
        DataQualityAccumulator,
        build_data_quality_report,
        iter_parquet_row_groups,
        read_duckdb_query,
        read_duckdb_table,
        read_parquet,
    )

    return (
        DataQualityAccumulator,
        build_data_quality_report,
        iter_parquet_row_groups,
        read_duckdb_query,
        read_duckdb_table,
        read_parquet,
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build a data quality report for honestroles inputs.")
    parser.add_argument("input", type=Path, help="Path to parquet or duckdb file")
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format",
    )
    parser.add_argument(
        "--dataset-name",
        default=None,
        help="Optional dataset label in report output",
    )
    parser.add_argument(
        "--stream",
        action="store_true",
        help="Stream parquet row groups instead of loading full file",
    )
    parser.add_argument(
        "--table",
        default=None,
        help="DuckDB table name (required for duckdb input unless --query is provided)",
    )
    parser.add_argument(
        "--query",
        default=None,
        help="DuckDB read-only query to analyze",
    )
    parser.add_argument(
        "--top-n-duplicates",
        type=int,
        default=10,
        help="Top N duplicate values to include",
    )
    return parser


def _report_to_text(report: dict[str, object]) -> str:
    lines: list[str] = []
    lines.append(f"Dataset: {report.get('dataset_name') or '<unspecified>'}")
    lines.append(f"Rows: {report['row_count']}")
    lines.append(f"Columns: {report['column_count']}")

    lines.append("\nRequired Field Null Counts:")
    for key, value in sorted(report["required_field_null_counts"].items()):  # type: ignore[union-attr]
        lines.append(f"- {key}: {value}")

    lines.append("\nRequired Field Empty Counts:")
    for key, value in sorted(report["required_field_empty_counts"].items()):  # type: ignore[union-attr]
        lines.append(f"- {key}: {value}")

    lines.append("\nTop Duplicate job_key values:")
    top_job_keys = report["top_duplicate_job_keys"]  # type: ignore[assignment]
    if top_job_keys:
        for item in top_job_keys:
            lines.append(f"- {item['value']}: {item['count']}")  # type: ignore[index]
    else:
        lines.append("- <none>")

    lines.append("\nTop Duplicate content_hash values:")
    top_hashes = report["top_duplicate_content_hashes"]  # type: ignore[assignment]
    if top_hashes:
        for item in top_hashes:
            lines.append(f"- {item['value']}: {item['count']}")  # type: ignore[index]
    else:
        lines.append("- <none>")

    lines.append("\nListing Pages:")
    lines.append(f"- rows: {report['listing_page_rows']}")
    lines.append(f"- ratio_pct: {report['listing_page_ratio']}")

    lines.append("\nSource Row Counts:")
    for key, value in sorted(report["source_row_counts"].items()):  # type: ignore[union-attr]
        lines.append(f"- {key}: {value}")

    lines.append("\nSource Quality:")
    source_quality = report["source_quality"]  # type: ignore[assignment]
    for source_name in sorted(source_quality):
        metrics = source_quality[source_name]
        lines.append(
            (
                f"- {source_name}: unknown_location_pct={metrics['unknown_location_pct']}, "
                f"missing_description_pct={metrics['missing_description_pct']}, "
                f"remote_true_pct={metrics['remote_true_pct']}"
            )
        )

    lines.append("\nEnrichment Sparsity (% null/empty):")
    for key, value in sorted(report["enrichment_sparsity_pct"].items()):  # type: ignore[union-attr]
        lines.append(f"- {key}: {value}")

    lines.append("\nAdditional Metrics:")
    lines.append(f"- invalid_apply_url_count: {report['invalid_apply_url_count']}")
    lines.append(f"- unknown_location_count: {report['unknown_location_count']}")
    return "\n".join(lines)


def _build_report(args: argparse.Namespace) -> dict[str, object]:
    (
        DataQualityAccumulator,
        build_data_quality_report,
        iter_parquet_row_groups,
        read_duckdb_query,
        read_duckdb_table,
        read_parquet,
    ) = _load_io_api()
    input_path: Path = args.input
    dataset_name = args.dataset_name or input_path.name
    suffix = input_path.suffix.lower()
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    if suffix == ".parquet":
        if args.stream:
            accumulator = DataQualityAccumulator(
                dataset_name=dataset_name,
                top_n_duplicates=args.top_n_duplicates,
            )
            for chunk in iter_parquet_row_groups(input_path, validate=False):
                accumulator.update(chunk)
            return accumulator.finalize().to_dict()
        df = read_parquet(input_path, validate=False)
        report = build_data_quality_report(
            df,
            dataset_name=dataset_name,
            top_n_duplicates=args.top_n_duplicates,
        )
        return report.to_dict()

    if suffix in {".duckdb", ".db"}:
        with duckdb.connect(str(input_path)) as conn:
            if args.query:
                df = read_duckdb_query(conn, args.query, validate=False)
            else:
                if not args.table:
                    raise ValueError("--table is required for duckdb input when --query is not provided")
                df = read_duckdb_table(conn, args.table, validate=False)
        report = build_data_quality_report(
            df,
            dataset_name=dataset_name,
            top_n_duplicates=args.top_n_duplicates,
        )
        return report.to_dict()

    raise ValueError("Unsupported input type. Use parquet or duckdb/db files.")


def main() -> None:
    args = _build_parser().parse_args()
    report = _build_report(args)
    if args.format == "json":
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print(_report_to_text(report))


if __name__ == "__main__":
    main()
