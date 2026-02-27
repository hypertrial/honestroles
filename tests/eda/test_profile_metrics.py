from __future__ import annotations

import polars as pl

from honestroles.eda.profile_metrics import (
    build_categorical_distribution_table,
    build_column_profile,
    build_consistency,
    build_consistency_by_source,
    build_numeric_quantiles_table,
    build_quality_by_source,
    build_source_profile,
    build_temporal,
    distribution,
    high_sentinel_columns,
    key_field_completeness,
    non_empty_distribution,
)


def test_distribution_helpers_handle_missing_or_empty() -> None:
    df = pl.DataFrame({"a": [1]})
    assert distribution(df, "missing", top_k=5) == []
    assert non_empty_distribution(df, "missing", top_k=5) == []
    assert distribution(pl.DataFrame(), "a", top_k=5) == []


def test_build_column_profile_and_high_sentinel_columns() -> None:
    df = pl.DataFrame(
        {
            "s": ["unknown", "", "ok", None],
            "n": [1, 2, None, 4],
        }
    )
    profile = build_column_profile(df)
    assert set(profile.columns) >= {"column", "null_percent", "sentinel_percent"}
    sentinels = high_sentinel_columns(profile, limit=5)
    assert sentinels

    empty_profile = pl.DataFrame(
        schema={
            "column": pl.String,
            "dtype": pl.String,
            "null_count": pl.Int64,
            "null_percent": pl.Float64,
            "empty_count": pl.Int64,
            "empty_percent": pl.Float64,
            "sentinel_count": pl.Int64,
            "sentinel_percent": pl.Float64,
            "cardinality_estimate": pl.Int64,
        }
    )
    assert high_sentinel_columns(empty_profile, limit=5) == []


def test_build_source_profile_branches() -> None:
    raw_missing_source = pl.DataFrame({"title": ["x"]})
    runtime = pl.DataFrame({"source": ["a"], "remote": [True]})
    out = build_source_profile(raw_missing_source, runtime)
    assert out.height == 0

    raw = pl.DataFrame({"source": ["a", "a"], "posted_at": [None, "2026-01-01"]})
    runtime_no_source = pl.DataFrame({"remote": [True, False]})
    out2 = build_source_profile(raw, runtime_no_source)
    assert out2["rows_runtime"].to_list() == [0]

    raw_no_posted = pl.DataFrame({"source": ["a", "b"]})
    runtime_with_source_no_remote = pl.DataFrame({"source": ["a", "b"]})
    out3 = build_source_profile(raw_no_posted, runtime_with_source_no_remote)
    assert out3["posted_at_non_null_pct_raw"].to_list() == [0.0, 0.0]
    assert out3["remote_true_pct_runtime"].to_list() == [0.0, 0.0]


def test_build_quality_and_consistency_by_source_branches() -> None:
    assert build_quality_by_source(pl.DataFrame(), effective_weights={}) == []

    runtime = pl.DataFrame({"source": ["a", "a"], "x": [None, 1]})
    rows = build_quality_by_source(runtime, effective_weights={"missing": 1.0, "x": 1.0})
    assert rows[0]["source"] == "a"
    assert rows[0]["key_nulls"]
    zero_rows = build_quality_by_source(runtime, effective_weights={"x": 0.0})
    assert zero_rows[0]["score_proxy"] == 100.0

    assert build_consistency_by_source(pl.DataFrame({"a": [1]}), pl.DataFrame({"b": [2]})) == []
    assert build_consistency_by_source(pl.DataFrame({"source": ["a"]}), pl.DataFrame({"x": [1]}))[0]["rows_runtime"] == 0
    only_runtime_source = build_consistency_by_source(
        pl.DataFrame({"x": [1]}),
        pl.DataFrame({"source": ["a"]}),
    )
    assert only_runtime_source[0]["rows_raw"] == 0

    raw = pl.DataFrame({"source": ["a"], "title": ["X"], "company": ["X"]})
    rt = pl.DataFrame({"source": ["a"], "salary_min": [2], "salary_max": [1]})
    consistency_rows = build_consistency_by_source(raw, rt)
    assert consistency_rows[0]["title_equals_company_count"] == 1
    assert consistency_rows[0]["salary_min_gt_salary_max_count"] == 1

    raw_no_title_company = pl.DataFrame({"source": ["b"]})
    rt_no_salary = pl.DataFrame({"source": ["b"]})
    consistency_rows2 = build_consistency_by_source(raw_no_title_company, rt_no_salary)
    assert consistency_rows2[0]["title_equals_company_count"] == 0
    assert consistency_rows2[0]["salary_min_gt_salary_max_count"] == 0


def test_numeric_quantiles_and_categorical_distribution_branches() -> None:
    non_numeric = pl.DataFrame({"s": ["x"]})
    q_empty = build_numeric_quantiles_table(non_numeric)
    assert q_empty.height == 0

    numeric_all_null = pl.DataFrame({"n": [None, None]}, schema={"n": pl.Float64})
    q_null = build_numeric_quantiles_table(numeric_all_null)
    assert q_null.filter(pl.col("column") == "n").height == 11
    assert q_null["value"].null_count() == 11

    cat_empty = build_categorical_distribution_table(pl.DataFrame(), columns=("source",))
    assert cat_empty.height == 0

    cat = build_categorical_distribution_table(
        pl.DataFrame({"source": ["a", "b", "c", "d"]}),
        columns=("source",),
        top_k=1,
    )
    assert "__other__" in cat["value"].to_list()

    cat_no_cols = build_categorical_distribution_table(
        pl.DataFrame({"x": [1]}),
        columns=("source",),
    )
    assert cat_no_cols.height == 0


def test_key_field_completeness_consistency_temporal() -> None:
    df = pl.DataFrame({"a": [1, None], "posted_at": ["2026-01-01", "bad"]})
    completeness = key_field_completeness(df, ["a", "missing"])
    assert completeness["missing"]["non_null_pct"] == 0.0

    consistency = build_consistency(
        pl.DataFrame({"title": ["x"], "company": ["x"]}),
        pl.DataFrame({"salary_min": [2], "salary_max": [1]}),
    )
    assert consistency["title_equals_company"]["count"] == 1
    assert consistency["salary_min_gt_salary_max"]["count"] == 1

    temporal_missing = build_temporal(pl.DataFrame({"x": [1]}))
    assert temporal_missing["monthly_counts"] == []

    temporal = build_temporal(df)
    assert "posted_at_range" in temporal
    consistency_missing_cols = build_consistency(pl.DataFrame({"x": [1]}), pl.DataFrame({"y": [1]}))
    assert consistency_missing_cols["title_equals_company"]["count"] == 0
    assert consistency_missing_cols["salary_min_gt_salary_max"]["count"] == 0
