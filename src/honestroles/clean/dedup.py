from __future__ import annotations

import pandas as pd

from honestroles.schema import CONTENT_HASH


def deduplicate(
    df: pd.DataFrame, *, subset: list[str] | None = None, keep: str = "first"
) -> pd.DataFrame:
    dedup_subset = subset or ([CONTENT_HASH] if CONTENT_HASH in df.columns else None)
    if not dedup_subset:
        return df
    return df.drop_duplicates(subset=dedup_subset, keep=keep).reset_index(drop=True)


def compact_snapshots(
    df: pd.DataFrame,
    *,
    key_columns: tuple[str, ...] = ("job_key", CONTENT_HASH),
    timestamp_column: str = "ingested_at",
    first_seen_column: str = "first_seen",
    last_seen_column: str = "last_seen",
    snapshot_count_column: str = "snapshot_count",
) -> pd.DataFrame:
    if not key_columns:
        result = df.copy()
        result[snapshot_count_column] = 1
        result[first_seen_column] = None
        result[last_seen_column] = None
        return result

    missing_keys = [column for column in key_columns if column not in df.columns]
    if missing_keys:
        result = df.copy()
        result[snapshot_count_column] = 1
        result[first_seen_column] = None
        result[last_seen_column] = None
        return result
    present_keys = list(key_columns)
    if df.empty:
        result = df.copy()
        result[snapshot_count_column] = pd.Series([], dtype="int64")
        result[first_seen_column] = pd.Series([], dtype="object")
        result[last_seen_column] = pd.Series([], dtype="object")
        return result

    result = df.copy().reset_index(drop=True)
    result["__row_order"] = range(len(result))
    parsed_ts = None
    if timestamp_column in result.columns:
        parsed_ts = pd.to_datetime(result[timestamp_column], errors="coerce", utc=True)
        result["__parsed_ts"] = parsed_ts
        result = result.sort_values(
            by=["__parsed_ts", "__row_order"],
            kind="mergesort",
            na_position="last",
        )
    else:
        result = result.sort_values(by=["__row_order"], kind="mergesort")

    representative = result.drop_duplicates(subset=present_keys, keep="first").copy()
    counts = (
        result.groupby(present_keys, dropna=False)
        .size()
        .rename(snapshot_count_column)
        .reset_index()
    )

    if parsed_ts is not None:
        first_seen = (
            result.groupby(present_keys, dropna=False)["__parsed_ts"]
            .min()
            .rename(first_seen_column)
            .reset_index()
        )
        last_seen = (
            result.groupby(present_keys, dropna=False)["__parsed_ts"]
            .max()
            .rename(last_seen_column)
            .reset_index()
        )
        metadata = counts.merge(first_seen, on=present_keys, how="left")
        metadata = metadata.merge(last_seen, on=present_keys, how="left")
        for col in (first_seen_column, last_seen_column):
            metadata[col] = (
                metadata[col]
                .dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                .where(metadata[col].notna(), None)
            )
    else:
        metadata = counts.copy()
        metadata[first_seen_column] = None
        metadata[last_seen_column] = None

    representative = representative.merge(metadata, on=present_keys, how="left")
    representative = representative.drop(columns=["__row_order"], errors="ignore")
    representative = representative.drop(columns=["__parsed_ts"], errors="ignore")

    original_columns = list(df.columns)
    ordered_columns = original_columns + [
        snapshot_count_column,
        first_seen_column,
        last_seen_column,
    ]
    representative = representative[ordered_columns]
    return representative.reset_index(drop=True)
