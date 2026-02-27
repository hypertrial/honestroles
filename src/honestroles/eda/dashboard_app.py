from __future__ import annotations

import argparse
from pathlib import Path

import polars as pl

from honestroles.eda.artifacts import load_eda_artifacts


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="HonestRoles EDA dashboard")
    parser.add_argument("--artifacts-dir", required=True)
    parser.add_argument("--diff-dir", default=None)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    import streamlit as st

    bundle = load_eda_artifacts(Path(args.artifacts_dir))
    if bundle.summary is None:
        raise ValueError("dashboard --artifacts-dir must be profile artifacts")
    summary = bundle.summary

    diff_bundle = None
    diff_payload = None
    if args.diff_dir is not None:
        diff_bundle = load_eda_artifacts(Path(args.diff_dir))
        diff_payload = diff_bundle.diff

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

    if diff_payload is not None and diff_bundle is not None:
        st.subheader("Diff Overview")
        gate = diff_payload.get("gate_evaluation", {})
        d1, d2, d3 = st.columns(3)
        d1.metric("Gate Status", str(gate.get("status", "unknown")).upper())
        d2.metric("Gate Failures", len(gate.get("failures", [])))
        d3.metric("Gate Warnings", len(gate.get("warnings", [])))

        st.markdown("#### Shape Diff")
        st.json(diff_payload.get("shape_diff", {}))

        st.markdown("#### Quality Diff")
        st.json(diff_payload.get("quality_diff", {}))

        st.markdown("#### Drift Metrics")
        drift_path_rel = diff_bundle.manifest.files.get("drift_metrics")
        if drift_path_rel:
            drift_path = diff_bundle.artifacts_dir / drift_path_rel
            if drift_path.exists():
                st.dataframe(pl.read_parquet(drift_path).to_pandas(), use_container_width=True)

        st.markdown("#### Findings Delta")
        findings_delta_rel = diff_bundle.manifest.files.get("findings_delta")
        if findings_delta_rel:
            findings_delta_path = diff_bundle.artifacts_dir / findings_delta_rel
            if findings_delta_path.exists():
                st.dataframe(
                    pl.read_parquet(findings_delta_path).to_pandas(),
                    use_container_width=True,
                )


if __name__ == "__main__":  # pragma: no cover
    main()
