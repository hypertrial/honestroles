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
