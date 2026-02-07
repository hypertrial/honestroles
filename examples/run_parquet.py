from __future__ import annotations

import argparse
from pathlib import Path

import honestroles as hr


def run(input_path: Path, output_path: Path) -> None:
    print(f"Loading parquet from {input_path}...")
    df = hr.read_parquet(input_path)
    print(f"Loaded {len(df)} rows. Cleaning data...")
    df = hr.clean_jobs(df)
    print("Cleaning complete. Applying filters...")
    df = hr.filter_jobs(df, remote_only=False)
    print(f"Filtering complete. {len(df)} rows remain. Adding heuristic labels...")
    df = hr.label_jobs(df, use_llm=False)
    print(f"Labeling complete. Writing output to {output_path}...")
    hr.write_parquet(df, output_path)
    print("Done. Output parquet contains cleaned, filtered, labeled job data.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run honestroles pipeline on parquet data."
    )
    parser.add_argument("input", type=Path, help="Path to input parquet file")
    parser.add_argument("output", type=Path, help="Path to output parquet file")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(args.input, args.output)
