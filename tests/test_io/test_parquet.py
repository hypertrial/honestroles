import pandas as pd

from honestroles.io import (
    iter_parquet_row_groups,
    read_parquet,
    validate_source_data_contract,
    write_parquet,
)


def test_parquet_roundtrip(tmp_path, sample_df: pd.DataFrame) -> None:
    path = tmp_path / "jobs.parquet"
    write_parquet(sample_df, path)
    loaded = read_parquet(path)
    assert len(loaded) == len(sample_df)
    assert loaded["job_key"].tolist() == sample_df["job_key"].tolist()


def test_read_parquet_missing_file(tmp_path) -> None:
    path = tmp_path / "missing.parquet"
    try:
        read_parquet(path)
    except FileNotFoundError as exc:
        assert "Parquet file not found" in str(exc)
    else:
        raise AssertionError("Expected FileNotFoundError")


def test_read_parquet_validate_false(tmp_path, sample_df: pd.DataFrame) -> None:
    path = tmp_path / "jobs.parquet"
    write_parquet(sample_df, path)
    loaded = read_parquet(path, validate=False)
    assert len(loaded) == len(sample_df)


def test_write_parquet_creates_parent(tmp_path, sample_df: pd.DataFrame) -> None:
    path = tmp_path / "nested" / "jobs.parquet"
    write_parquet(sample_df, path)
    assert path.exists()


def test_parquet_contract_validation_with_extra_columns_and_arrays(
    tmp_path, minimal_df: pd.DataFrame
) -> None:
    df = minimal_df.copy()
    df["skills"] = [["Python", "SQL"]]
    df["source_data_debug"] = ["trace-1"]
    path = tmp_path / "jobs_contract.parquet"

    write_parquet(df, path)
    loaded = read_parquet(path, validate=False)
    validate_source_data_contract(loaded)

    assert loaded["source_data_debug"].tolist() == ["trace-1"]
    assert list(loaded["skills"].iloc[0]) == ["Python", "SQL"]


def test_iter_parquet_row_groups_roundtrip(tmp_path, sample_df: pd.DataFrame) -> None:
    path = tmp_path / "chunked.parquet"
    sample_df.to_parquet(path, index=False, row_group_size=1)

    chunks = list(iter_parquet_row_groups(path, validate=False))
    assert len(chunks) == len(sample_df)
    loaded = pd.concat(chunks, ignore_index=True)
    assert loaded["job_key"].tolist() == sample_df["job_key"].tolist()


def test_iter_parquet_row_groups_validate_true_raises_for_missing_required(tmp_path) -> None:
    path = tmp_path / "bad.parquet"
    pd.DataFrame({"only": [1, 2, 3]}).to_parquet(path, index=False, row_group_size=2)

    iterator = iter_parquet_row_groups(path, validate=True)
    try:
        next(iterator)
    except ValueError as exc:
        assert "Missing required columns" in str(exc)
    else:
        raise AssertionError("Expected ValueError for invalid row-group schema")


def test_iter_parquet_row_groups_missing_file(tmp_path) -> None:
    path = tmp_path / "missing.parquet"
    try:
        list(iter_parquet_row_groups(path))
    except FileNotFoundError as exc:
        assert "Parquet file not found" in str(exc)
    else:
        raise AssertionError("Expected FileNotFoundError")
