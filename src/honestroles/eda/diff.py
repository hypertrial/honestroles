from __future__ import annotations

import math
from typing import Any

import polars as pl

from honestroles.eda.artifacts import load_eda_artifacts
from honestroles.eda.gate import evaluate_eda_gate
from honestroles.eda.rules import EDARules
from honestroles.errors import ConfigValidationError


def build_eda_diff(
    *,
    baseline_dir,
    candidate_dir,
    rules: EDARules,
) -> tuple[dict[str, Any], dict[str, pl.DataFrame], dict[str, Any], dict[str, Any]]:
    baseline_bundle = load_eda_artifacts(baseline_dir)
    candidate_bundle = load_eda_artifacts(candidate_dir)

    if baseline_bundle.summary is None or candidate_bundle.summary is None:
        raise ConfigValidationError("EDA diff requires profile artifacts as inputs")

    baseline_summary = baseline_bundle.summary
    candidate_summary = candidate_bundle.summary

    null_diff_df = _build_null_diff_table(baseline_bundle, candidate_bundle)
    source_diff_df = _build_source_profile_diff_table(baseline_bundle, candidate_bundle)
    drift_df = _build_drift_metrics_table(
        baseline_bundle=baseline_bundle,
        candidate_bundle=candidate_bundle,
        rules=rules,
    )

    findings_delta_df = _build_findings_delta_table(
        baseline_summary=baseline_summary,
        candidate_summary=candidate_summary,
    )

    diff_payload: dict[str, Any] = {
        "shape_diff": _shape_diff(baseline_summary, candidate_summary),
        "quality_diff": _quality_diff(baseline_summary, candidate_summary),
        "null_diff_top": _jsonable(null_diff_df.head(20).to_dicts()),
        "distribution_diff": {
            "source_profile": _jsonable(source_diff_df.to_dicts()),
        },
        "consistency_diff": _consistency_diff(baseline_summary, candidate_summary),
        "drift": {
            "metrics": _jsonable(drift_df.to_dicts()),
        },
        "findings_delta": _jsonable(findings_delta_df.to_dicts()),
        "gate_evaluation": {},
    }

    gate_payload = evaluate_eda_gate(
        candidate_summary=candidate_summary,
        rules=rules,
        diff_payload=diff_payload,
    )
    diff_payload["gate_evaluation"] = gate_payload

    tables = {
        "diff_null_percentages": null_diff_df,
        "diff_source_profile": source_diff_df,
        "drift_metrics": drift_df,
        "findings_delta": findings_delta_df,
    }

    return diff_payload, tables, baseline_summary, candidate_summary


def _shape_diff(
    baseline_summary: dict[str, Any], candidate_summary: dict[str, Any]
) -> dict[str, Any]:
    base_rows = int(baseline_summary["shape"]["runtime"]["rows"])
    cand_rows = int(candidate_summary["shape"]["runtime"]["rows"])
    base_cols = int(baseline_summary["shape"]["runtime"]["columns"])
    cand_cols = int(candidate_summary["shape"]["runtime"]["columns"])

    row_delta = cand_rows - base_rows
    col_delta = cand_cols - base_cols
    return {
        "baseline_rows": base_rows,
        "candidate_rows": cand_rows,
        "delta_rows": row_delta,
        "delta_rows_pct": _pct_delta(base_rows, cand_rows),
        "baseline_columns": base_cols,
        "candidate_columns": cand_cols,
        "delta_columns": col_delta,
        "delta_columns_pct": _pct_delta(base_cols, cand_cols),
    }


def _quality_diff(
    baseline_summary: dict[str, Any], candidate_summary: dict[str, Any]
) -> dict[str, Any]:
    base_score = float(baseline_summary["quality"]["score_percent"])
    cand_score = float(candidate_summary["quality"]["score_percent"])
    base_null = float(baseline_summary["quality"]["weighted_null_percent"])
    cand_null = float(candidate_summary["quality"]["weighted_null_percent"])

    return {
        "baseline_score_percent": _round4(base_score),
        "candidate_score_percent": _round4(cand_score),
        "delta_score_percent": _round4(cand_score - base_score),
        "baseline_weighted_null_percent": _round4(base_null),
        "candidate_weighted_null_percent": _round4(cand_null),
        "delta_weighted_null_percent": _round4(cand_null - base_null),
    }


def _consistency_diff(
    baseline_summary: dict[str, Any], candidate_summary: dict[str, Any]
) -> dict[str, Any]:
    keys = ["salary_min_gt_salary_max", "title_equals_company"]
    out: dict[str, Any] = {}
    for key in keys:
        base = baseline_summary.get("consistency", {}).get(key, {})
        cand = candidate_summary.get("consistency", {}).get(key, {})
        out[key] = {
            "baseline_count": int(base.get("count", 0)),
            "candidate_count": int(cand.get("count", 0)),
            "delta_count": int(cand.get("count", 0)) - int(base.get("count", 0)),
            "baseline_pct": _round4(float(base.get("pct", 0.0))),
            "candidate_pct": _round4(float(cand.get("pct", 0.0))),
            "delta_pct": _round4(float(cand.get("pct", 0.0)) - float(base.get("pct", 0.0))),
        }
    return out


def _build_null_diff_table(baseline_bundle, candidate_bundle) -> pl.DataFrame:
    base = _load_table(baseline_bundle, "null_percentages")
    cand = _load_table(candidate_bundle, "null_percentages")

    return (
        base.rename({"null_percent": "baseline_null_percent"})
        .join(
            cand.rename({"null_percent": "candidate_null_percent"}),
            on="column",
            how="full",
        )
        .with_columns(
            pl.coalesce([pl.col("column"), pl.col("column_right")]).alias("column"),
            pl.col("baseline_null_percent").fill_null(0.0),
            pl.col("candidate_null_percent").fill_null(0.0),
        )
        .drop("column_right")
        .with_columns(
            (pl.col("candidate_null_percent") - pl.col("baseline_null_percent")).alias(
                "delta_null_percent"
            ),
            (pl.col("candidate_null_percent") - pl.col("baseline_null_percent"))
            .abs()
            .alias("abs_delta_null_percent"),
        )
        .sort("abs_delta_null_percent", descending=True)
    )


def _build_source_profile_diff_table(baseline_bundle, candidate_bundle) -> pl.DataFrame:
    base = _load_table(baseline_bundle, "source_profile")
    cand = _load_table(candidate_bundle, "source_profile")

    return (
        base.rename(
            {
                "rows_raw": "baseline_rows_raw",
                "rows_runtime": "baseline_rows_runtime",
                "posted_at_non_null_pct_raw": "baseline_posted_at_non_null_pct_raw",
                "remote_true_pct_runtime": "baseline_remote_true_pct_runtime",
            }
        )
        .join(
            cand.rename(
                {
                    "rows_raw": "candidate_rows_raw",
                    "rows_runtime": "candidate_rows_runtime",
                    "posted_at_non_null_pct_raw": "candidate_posted_at_non_null_pct_raw",
                    "remote_true_pct_runtime": "candidate_remote_true_pct_runtime",
                }
            ),
            on="source",
            how="full",
        )
        .with_columns(
            pl.coalesce([pl.col("source"), pl.col("source_right")]).alias("source"),
            pl.col("baseline_rows_raw").fill_null(0).cast(pl.Int64),
            pl.col("candidate_rows_raw").fill_null(0).cast(pl.Int64),
            pl.col("baseline_rows_runtime").fill_null(0).cast(pl.Int64),
            pl.col("candidate_rows_runtime").fill_null(0).cast(pl.Int64),
            pl.col("baseline_posted_at_non_null_pct_raw").fill_null(0.0),
            pl.col("candidate_posted_at_non_null_pct_raw").fill_null(0.0),
            pl.col("baseline_remote_true_pct_runtime").fill_null(0.0),
            pl.col("candidate_remote_true_pct_runtime").fill_null(0.0),
        )
        .drop("source_right")
        .with_columns(
            (pl.col("candidate_rows_runtime") - pl.col("baseline_rows_runtime")).alias(
                "delta_rows_runtime"
            ),
            (
                pl.col("candidate_posted_at_non_null_pct_raw")
                - pl.col("baseline_posted_at_non_null_pct_raw")
            ).alias("delta_posted_at_non_null_pct_raw"),
            (
                pl.col("candidate_remote_true_pct_runtime")
                - pl.col("baseline_remote_true_pct_runtime")
            ).alias("delta_remote_true_pct_runtime"),
        )
        .sort("source")
    )


def _build_drift_metrics_table(
    *,
    baseline_bundle,
    candidate_bundle,
    rules: EDARules,
) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []

    baseline_quantiles = _load_table(baseline_bundle, "numeric_quantiles")
    candidate_quantiles = _load_table(candidate_bundle, "numeric_quantiles")
    for column in rules.drift.columns_numeric:
        psi_value = _numeric_psi_from_quantiles(
            baseline_quantiles=baseline_quantiles,
            candidate_quantiles=candidate_quantiles,
            column=column,
        )
        if psi_value is None:
            continue
        status = _classify_threshold(
            psi_value,
            warn=rules.drift.numeric_warn_psi,
            fail=rules.drift.numeric_fail_psi,
        )
        rows.append(
            {
                "column": column,
                "kind": "numeric",
                "metric": "psi",
                "value": _round4(psi_value),
                "warn_threshold": rules.drift.numeric_warn_psi,
                "fail_threshold": rules.drift.numeric_fail_psi,
                "status": status,
            }
        )

    baseline_cat = _load_table(baseline_bundle, "categorical_distribution")
    candidate_cat = _load_table(candidate_bundle, "categorical_distribution")
    for column in rules.drift.columns_categorical:
        jsd_value = _categorical_jsd(
            baseline_distribution=baseline_cat,
            candidate_distribution=candidate_cat,
            column=column,
        )
        if jsd_value is None:
            continue
        status = _classify_threshold(
            jsd_value,
            warn=rules.drift.categorical_warn_jsd,
            fail=rules.drift.categorical_fail_jsd,
        )
        rows.append(
            {
                "column": column,
                "kind": "categorical",
                "metric": "jsd",
                "value": _round4(jsd_value),
                "warn_threshold": rules.drift.categorical_warn_jsd,
                "fail_threshold": rules.drift.categorical_fail_jsd,
                "status": status,
            }
        )

    if not rows:
        return pl.DataFrame(
            schema={
                "column": pl.String,
                "kind": pl.String,
                "metric": pl.String,
                "value": pl.Float64,
                "warn_threshold": pl.Float64,
                "fail_threshold": pl.Float64,
                "status": pl.String,
            }
        )
    return pl.DataFrame(rows).sort(["kind", "column"])


def _numeric_psi_from_quantiles(
    *,
    baseline_quantiles: pl.DataFrame,
    candidate_quantiles: pl.DataFrame,
    column: str,
) -> float | None:
    baseline_col = (
        baseline_quantiles.filter(pl.col("column") == column)
        .sort("quantile")
        .to_dicts()
    )
    candidate_col = (
        candidate_quantiles.filter(pl.col("column") == column)
        .sort("quantile")
        .to_dicts()
    )
    if len(baseline_col) < 2 or len(candidate_col) < 2:
        return None

    baseline_edges: list[tuple[float, float]] = []
    for row in baseline_col:
        q = float(row["quantile"])
        value = row.get("value")
        if value is None:
            return None
        baseline_edges.append((q, float(value)))

    candidate_curve: list[tuple[float, float]] = []
    for row in candidate_col:
        q = float(row["quantile"])
        value = row.get("value")
        if value is None:
            return None
        candidate_curve.append((q, float(value)))

    epsilon = 1e-9
    psi = 0.0
    for idx in range(1, len(baseline_edges)):
        _, edge_lo = baseline_edges[idx - 1]
        _, edge_hi = baseline_edges[idx]
        baseline_prob = _cdf_from_quantile_curve(baseline_edges, edge_hi) - _cdf_from_quantile_curve(
            baseline_edges, edge_lo
        )
        candidate_prob = _cdf_from_quantile_curve(candidate_curve, edge_hi) - _cdf_from_quantile_curve(
            candidate_curve, edge_lo
        )
        if baseline_prob <= epsilon and candidate_prob <= epsilon:
            continue
        baseline_prob = max(epsilon, baseline_prob)
        candidate_prob = max(epsilon, candidate_prob)
        psi += (baseline_prob - candidate_prob) * math.log(baseline_prob / candidate_prob)

    return float(psi)


def _cdf_from_quantile_curve(curve: list[tuple[float, float]], value: float) -> float:
    if not curve:
        return 0.0
    sorted_curve = sorted(curve, key=lambda item: item[0])
    q_prev, v_prev = sorted_curve[0]
    if value < v_prev:
        return float(q_prev)

    for q_curr, v_curr in sorted_curve[1:]:
        if value < v_curr:
            if v_curr == v_prev:
                return float(q_curr)
            ratio = (value - v_prev) / (v_curr - v_prev)
            return float(q_prev + ratio * (q_curr - q_prev))
        q_prev, v_prev = q_curr, v_curr
    return float(sorted_curve[-1][0])


def _categorical_jsd(
    *,
    baseline_distribution: pl.DataFrame,
    candidate_distribution: pl.DataFrame,
    column: str,
) -> float | None:
    base = baseline_distribution.filter(pl.col("column") == column).to_dicts()
    cand = candidate_distribution.filter(pl.col("column") == column).to_dicts()
    if not base or not cand:
        return None

    base_map = {str(item["value"]): max(float(item["pct"]) / 100.0, 0.0) for item in base}
    cand_map = {str(item["value"]): max(float(item["pct"]) / 100.0, 0.0) for item in cand}

    keys = sorted(set(base_map) | set(cand_map))
    p_raw = [base_map.get(key, 0.0) for key in keys]
    q_raw = [cand_map.get(key, 0.0) for key in keys]

    p = _normalize_distribution(p_raw)
    q = _normalize_distribution(q_raw)
    m = [(a + b) / 2.0 for a, b in zip(p, q)]

    return float(0.5 * _kl_divergence(p, m) + 0.5 * _kl_divergence(q, m))


def _normalize_distribution(values: list[float]) -> list[float]:
    total = sum(values)
    if total <= 0:
        return [0.0 for _ in values]
    return [value / total for value in values]


def _kl_divergence(p: list[float], q: list[float]) -> float:
    epsilon = 1e-12
    total = 0.0
    for p_i, q_i in zip(p, q):
        if p_i <= 0:
            continue
        total += p_i * math.log((p_i + epsilon) / (q_i + epsilon))
    return total


def _build_findings_delta_table(
    *,
    baseline_summary: dict[str, Any],
    candidate_summary: dict[str, Any],
) -> pl.DataFrame:
    baseline_counts = _count_findings(baseline_summary)
    candidate_counts = _count_findings(candidate_summary)

    rows = []
    for severity in ["P0", "P1", "P2"]:
        base = baseline_counts.get(severity, 0)
        cand = candidate_counts.get(severity, 0)
        rows.append(
            {
                "severity": severity,
                "baseline_count": base,
                "candidate_count": cand,
                "delta_count": cand - base,
            }
        )
    return pl.DataFrame(rows)


def _count_findings(summary: dict[str, Any]) -> dict[str, int]:
    counts = {"P0": 0, "P1": 0, "P2": 0}
    for finding in summary.get("findings", []):
        severity = str(finding.get("severity", "")).upper()
        if severity in counts:
            counts[severity] += 1
    for finding in summary.get("findings_by_source", []):
        severity = str(finding.get("severity", "")).upper()
        if severity in counts:
            counts[severity] += 1
    return counts


def _classify_threshold(value: float, warn: float, fail: float) -> str:
    if value >= fail:
        return "fail"
    if value >= warn:
        return "warn"
    return "pass"


def _load_table(bundle, key: str) -> pl.DataFrame:
    relative = bundle.manifest.files.get(key)
    if relative is None:
        return pl.DataFrame()
    return pl.read_parquet(bundle.artifacts_dir / relative)


def _pct_delta(baseline: int, candidate: int) -> float:
    if baseline == 0:
        return 0.0 if candidate == 0 else 100.0
    return _round4(((candidate - baseline) / baseline) * 100.0)


def _round4(value: float) -> float:
    return round(float(value), 4)


def _jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_jsonable(v) for v in value]
    if isinstance(value, tuple):
        return [_jsonable(v) for v in value]
    return value
