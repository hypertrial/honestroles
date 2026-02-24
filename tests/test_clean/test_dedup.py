import pandas as pd

from honestroles.clean.dedup import compact_snapshots, deduplicate


def test_deduplicate_by_content_hash(sample_df: pd.DataFrame) -> None:
    duplicated = pd.concat([sample_df, sample_df.iloc[[0]]], ignore_index=True)
    result = deduplicate(duplicated)
    assert len(result) == len(sample_df)


def test_deduplicate_empty_dataframe(empty_df: pd.DataFrame) -> None:
    result = deduplicate(empty_df)
    assert result.empty


def test_deduplicate_custom_subset(sample_df: pd.DataFrame) -> None:
    df = sample_df.copy()
    df["company"] = ["Acme", "Acme"]
    df["job_id"] = ["1", "1"]
    result = deduplicate(df, subset=["company", "job_id"])
    assert len(result) == 1


def test_deduplicate_missing_content_hash_column(sample_df: pd.DataFrame) -> None:
    df = sample_df.drop(columns=["content_hash"])
    result = deduplicate(df)
    assert result.equals(df)


def test_deduplicate_keep_last(sample_df: pd.DataFrame) -> None:
    duplicated = pd.concat([sample_df, sample_df.iloc[[0]]], ignore_index=True)
    result = deduplicate(duplicated, keep="last")
    assert result.loc[0, "job_key"] == sample_df.loc[1, "job_key"]


def test_deduplicate_all_rows_identical() -> None:
    df = pd.DataFrame(
        [
            {"content_hash": "same", "title": "Engineer"},
            {"content_hash": "same", "title": "Engineer"},
        ]
    )
    result = deduplicate(df)
    assert len(result) == 1


def test_deduplicate_with_nan_hashes() -> None:
    df = pd.DataFrame(
        [
            {"content_hash": None, "title": "Engineer"},
            {"content_hash": None, "title": "Engineer"},
            {"content_hash": "abc", "title": "Engineer"},
        ]
    )
    result = deduplicate(df)
    assert len(result) == 2


def test_compact_snapshots_reduces_by_key_and_tracks_metadata() -> None:
    df = pd.DataFrame(
        [
            {
                "job_key": "acme::greenhouse::1",
                "content_hash": "h1",
                "ingested_at": "2025-01-01T00:00:00Z",
                "title": "Engineer",
            },
            {
                "job_key": "acme::greenhouse::1",
                "content_hash": "h1",
                "ingested_at": "2025-01-02T00:00:00Z",
                "title": "Engineer updated",
            },
            {
                "job_key": "acme::greenhouse::2",
                "content_hash": "h2",
                "ingested_at": "2025-01-03T00:00:00Z",
                "title": "PM",
            },
        ]
    )

    compacted = compact_snapshots(df)
    assert len(compacted) == 2
    assert compacted["snapshot_count"].tolist() == [2, 1]
    assert compacted.loc[0, "first_seen"] == "2025-01-01T00:00:00Z"
    assert compacted.loc[0, "last_seen"] == "2025-01-02T00:00:00Z"


def test_compact_snapshots_datetime_output_returns_utc_timestamps() -> None:
    df = pd.DataFrame(
        [
            {
                "job_key": "acme::greenhouse::1",
                "content_hash": "h1",
                "ingested_at": "2025-01-01T00:00:00Z",
            },
            {
                "job_key": "acme::greenhouse::1",
                "content_hash": "h1",
                "ingested_at": "2025-01-02T00:00:00Z",
            },
        ]
    )

    compacted = compact_snapshots(df, timestamp_output="datetime")

    assert isinstance(compacted["first_seen"].dtype, pd.DatetimeTZDtype)
    assert isinstance(compacted["last_seen"].dtype, pd.DatetimeTZDtype)
    assert str(compacted["first_seen"].dt.tz) == "UTC"
    assert str(compacted["last_seen"].dt.tz) == "UTC"
    assert compacted.loc[0, "first_seen"] == pd.Timestamp("2025-01-01T00:00:00Z")
    assert compacted.loc[0, "last_seen"] == pd.Timestamp("2025-01-02T00:00:00Z")


def test_compact_snapshots_handles_missing_compaction_keys() -> None:
    df = pd.DataFrame([{"title": "Engineer"}, {"title": "Engineer"}])
    compacted = compact_snapshots(df, key_columns=("job_key", "content_hash"))
    assert compacted["snapshot_count"].tolist() == [1, 1]
    assert compacted["first_seen"].tolist() == [None, None]
    assert compacted["last_seen"].tolist() == [None, None]


def test_compact_snapshots_handles_empty_dataframe_with_keys() -> None:
    df = pd.DataFrame(columns=["job_key", "content_hash", "ingested_at"])
    compacted = compact_snapshots(df)
    assert compacted.empty
    assert "snapshot_count" in compacted.columns


def test_compact_snapshots_empty_dataframe_datetime_output() -> None:
    df = pd.DataFrame(columns=["job_key", "content_hash", "ingested_at"])
    compacted = compact_snapshots(df, timestamp_output="datetime")
    assert compacted.empty
    assert isinstance(compacted["first_seen"].dtype, pd.DatetimeTZDtype)
    assert isinstance(compacted["last_seen"].dtype, pd.DatetimeTZDtype)


def test_compact_snapshots_without_timestamp_column() -> None:
    df = pd.DataFrame(
        [
            {"job_key": "a::1", "content_hash": "h1", "title": "Engineer"},
            {"job_key": "a::1", "content_hash": "h1", "title": "Engineer v2"},
        ]
    )
    compacted = compact_snapshots(df, timestamp_column="missing_ts")
    assert len(compacted) == 1
    assert compacted.loc[0, "first_seen"] is None
    assert compacted.loc[0, "last_seen"] is None


def test_compact_snapshots_without_timestamp_column_datetime_output() -> None:
    df = pd.DataFrame(
        [
            {"job_key": "a::1", "content_hash": "h1", "title": "Engineer"},
            {"job_key": "a::1", "content_hash": "h1", "title": "Engineer v2"},
        ]
    )
    compacted = compact_snapshots(
        df, timestamp_column="missing_ts", timestamp_output="datetime"
    )
    assert len(compacted) == 1
    assert isinstance(compacted["first_seen"].dtype, pd.DatetimeTZDtype)
    assert isinstance(compacted["last_seen"].dtype, pd.DatetimeTZDtype)
    assert pd.isna(compacted.loc[0, "first_seen"])
    assert pd.isna(compacted.loc[0, "last_seen"])


def test_compact_snapshots_requires_all_compaction_keys() -> None:
    df = pd.DataFrame(
        [
            {"job_key": "a::1", "ingested_at": "2025-01-01T00:00:00Z"},
            {"job_key": "a::1", "ingested_at": "2025-01-02T00:00:00Z"},
        ]
    )
    compacted = compact_snapshots(df, key_columns=("job_key", "content_hash"))
    assert len(compacted) == 2
    assert compacted["snapshot_count"].tolist() == [1, 1]


def test_compact_snapshots_without_key_columns_no_compaction() -> None:
    df = pd.DataFrame(
        [
            {"job_key": "a::1", "content_hash": "h1"},
            {"job_key": "a::1", "content_hash": "h1"},
        ]
    )
    compacted = compact_snapshots(df, key_columns=())
    assert len(compacted) == 2
    assert compacted["snapshot_count"].tolist() == [1, 1]


def test_compact_snapshots_rejects_invalid_timestamp_output() -> None:
    df = pd.DataFrame([{"job_key": "a::1", "content_hash": "h1"}])
    try:
        compact_snapshots(df, timestamp_output="bad")  # type: ignore[arg-type]
    except ValueError as exc:
        assert "timestamp_output" in str(exc)
    else:
        raise AssertionError("Expected ValueError for invalid timestamp_output")
