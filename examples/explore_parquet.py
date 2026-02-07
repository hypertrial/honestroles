from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

import honestroles as hr


DEFAULT_COLUMNS = [
    "job_key",
    "company",
    "title",
    "location_raw",
    "remote_flag",
    "salary_min",
    "salary_max",
    "salary_currency",
    "seniority",
    "role_category",
    "tech_stack",
]


def _truncate(value: object, limit: int = 120) -> str:
    if value is None:
        return ""
    text = str(value).replace("\n", " ").strip()
    if len(text) <= limit:
        return text
    return f"{text[:limit]}..."


def _pick_columns(columns: Iterable[str], available: Iterable[str]) -> list[str]:
    available_set = set(available)
    selected = [col for col in columns if col in available_set]
    return selected or list(available)


def run(input_path: Path, rows: int) -> None:
    print(f"Loading parquet from {input_path}...")
    df = hr.read_parquet(input_path, validate=False)
    print(f"Loaded {len(df)} rows with {len(df.columns)} columns.")

    print("\nColumns:")
    print(", ".join(df.columns))

    print("\nMissing values (top 10):")
    missing = df.isna().sum().sort_values(ascending=False).head(10)
    for column, count in missing.items():
        print(f"- {column}: {count}")

    selected_columns = _pick_columns(DEFAULT_COLUMNS, df.columns)
    sample = df[selected_columns].head(rows).copy()
    for column in sample.columns:
        sample[column] = sample[column].apply(_truncate)
    print(f"\nSample rows (first {rows}) with key columns:")
    print(sample.to_string(index=False))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Explore a parquet file produced by honestroles."
    )
    parser.add_argument("input", type=Path, help="Path to input parquet file")
    parser.add_argument("--rows", type=int, default=5, help="Number of rows to show")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(args.input, args.rows)
