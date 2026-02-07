import pandas as pd

from honestroles.clean.dedup import deduplicate


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
