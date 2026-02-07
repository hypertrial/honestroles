import pandas as pd

from honestroles.filter import FilterChain
from honestroles.filter.predicates import by_keywords, by_location


def test_filter_chain_and(sample_df: pd.DataFrame) -> None:
    chain = FilterChain()
    chain.add(by_location, cities=["New York"])
    chain.add(by_keywords, include=["systems"])
    result = chain.apply(sample_df)
    assert len(result) == 1
    assert result.iloc[0]["job_id"] == "1"


def test_filter_chain_or(sample_df: pd.DataFrame) -> None:
    chain = FilterChain(mode="or")
    chain.add(by_location, cities=["New York"])
    chain.add(by_keywords, include=["roadmap"])
    result = chain.apply(sample_df)
    assert len(result) == 2


def test_filter_chain_invalid_mode_raises() -> None:
    try:
        FilterChain(mode="xor")
    except ValueError as exc:
        assert "mode must be" in str(exc)
    else:
        raise AssertionError("Expected ValueError for invalid mode")


def test_filter_chain_empty_steps_returns_input(sample_df: pd.DataFrame) -> None:
    chain = FilterChain()
    result = chain.apply(sample_df)
    assert result.equals(sample_df)


def test_filter_chain_single_predicate(sample_df: pd.DataFrame) -> None:
    chain = FilterChain()
    chain.add(by_location, cities=["New York"])
    result = chain.apply(sample_df)
    assert result["job_id"].tolist() == ["1"]


def test_filter_chain_no_matches(sample_df: pd.DataFrame) -> None:
    chain = FilterChain()
    chain.add(by_keywords, include=["nonexistent"])
    result = chain.apply(sample_df)
    assert result.empty
