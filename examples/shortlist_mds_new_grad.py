from __future__ import annotations

import argparse
from pathlib import Path

import honestroles as hr


def run(input_path: Path, output_path: Path, top_n: int) -> None:
    print(f"Loading parquet from {input_path}...")
    df = hr.read_parquet(input_path, validate=False)
    print(f"Loaded {len(df)} rows.")

    df = hr.clean_jobs(df)
    df = hr.label_jobs(df, use_llm=False)
    df = hr.rate_jobs(df, use_llm=False)

    profile = hr.CandidateProfile.mds_new_grad()
    ranked = hr.rank_jobs(df, profile=profile, use_llm_signals=False, top_n=top_n)
    planned = hr.build_application_plan(ranked, profile=profile, top_n=top_n)

    hr.write_parquet(planned, output_path)
    print(f"Wrote ranked shortlist with action plan to {output_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create an MDS new-grad shortlist with explainable fit scores."
    )
    parser.add_argument("input", type=Path, help="Path to input parquet file")
    parser.add_argument("output", type=Path, help="Path to output parquet file")
    parser.add_argument("--top-n", type=int, default=50, help="Number of top jobs to keep")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(args.input, args.output, args.top_n)
