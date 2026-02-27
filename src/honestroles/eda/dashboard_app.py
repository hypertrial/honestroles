from __future__ import annotations

import argparse
from pathlib import Path

import polars as pl

from honestroles.eda.artifacts import load_eda_artifacts


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="HonestRoles EDA dashboard")
    parser.add_argument("--artifacts-dir", required=True)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    import streamlit as st

    bundle = load_eda_artifacts(Path(args.artifacts_dir))
    summary = bundle.summary

    st.set_page_config(page_title="HonestRoles EDA", layout="wide")
    st.title("HonestRoles EDA Dashboard")
    st.caption(f"Artifacts: {bundle.artifacts_dir}")

    shape = summary["shape"]
    quality = summary["quality"]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Raw Rows", shape["raw"]["rows"])
    c2.metric("Runtime Rows", shape["runtime"]["rows"])
    c3.metric("Quality Score", quality["score_percent"])
    c4.metric("Weighted Null %", quality["weighted_null_percent"])

    st.subheader("Findings")
    findings = summary.get("findings", [])
    if not findings:
        st.write("No prioritized findings.")
    else:
        for finding in findings:
            st.markdown(
                f"- **{finding['severity']}** `{finding['title']}`: {finding['detail']}"
            )
            st.caption(f"Recommendation: {finding['recommendation']}")

    st.subheader("Figures")
    figure_keys = [
        "nulls_by_column",
        "completeness_by_source",
        "remote_by_source",
        "posted_at_timeseries",
        "top_locations",
    ]
    for key in figure_keys:
        relative = bundle.manifest.files.get(key)
        if not relative:
            continue
        image_path = bundle.artifacts_dir / relative
        if image_path.exists():
            st.image(str(image_path), caption=key.replace("_", " ").title(), use_container_width=True)

    st.subheader("Tables")
    table_keys = [
        "null_percentages",
        "column_profile",
        "source_profile",
        "top_values_source",
        "top_values_company",
        "top_values_title",
        "top_values_location",
    ]
    for key in table_keys:
        relative = bundle.manifest.files.get(key)
        if not relative:
            continue
        table_path = bundle.artifacts_dir / relative
        if not table_path.exists():
            continue
        st.markdown(f"#### {key.replace('_', ' ').title()}")
        st.dataframe(pl.read_parquet(table_path).to_pandas(), use_container_width=True)


if __name__ == "__main__":
    main()
