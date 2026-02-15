import pandas as pd
import pytest

from honestroles.plugins import register_rate_plugin
from honestroles.rate import rate_jobs


def test_rate_jobs_integration(sample_df: pd.DataFrame) -> None:
    rated = rate_jobs(sample_df, use_llm=False)
    assert "completeness_score" in rated.columns
    assert "quality_score" in rated.columns
    assert "rating" in rated.columns


def test_rate_jobs_empty_dataframe(empty_df: pd.DataFrame) -> None:
    rated = rate_jobs(empty_df, use_llm=False)
    assert rated.empty


def test_rate_jobs_with_unknown_plugin_rater_raises(sample_df: pd.DataFrame) -> None:
    with pytest.raises(KeyError, match="Unknown rate plugin"):
        rate_jobs(sample_df, use_llm=False, plugin_raters=["missing_plugin"])


def test_rate_jobs_with_bad_plugin_rater_type_raises(sample_df: pd.DataFrame) -> None:
    register_rate_plugin("bad_plugin", lambda df: ["wrong"])  # type: ignore[return-value]
    with pytest.raises(TypeError, match="must return a pandas DataFrame"):
        rate_jobs(sample_df, use_llm=False, plugin_raters=["bad_plugin"])
