import pandas as pd

from honestroles import schema
from honestroles.clean import clean_jobs
from honestroles.filter import filter_jobs
from honestroles.label import label_jobs
from honestroles.rate import rate_jobs


def _run_pipeline(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = clean_jobs(df)
    filtered = filter_jobs(cleaned)
    labeled = label_jobs(filtered, use_llm=False)
    return rate_jobs(labeled, use_llm=False)


def test_pipeline_preserves_unknown_columns(sample_df: pd.DataFrame) -> None:
    df = sample_df.copy()
    df["source_data_debug"] = ["trace-1", "trace-2"]

    processed = _run_pipeline(df)

    assert "source_data_debug" in processed.columns
    assert processed["source_data_debug"].tolist() == ["trace-1", "trace-2"]


def test_pipeline_preserves_required_core_columns(sample_df: pd.DataFrame) -> None:
    processed = _run_pipeline(sample_df)

    for column in sorted(schema.REQUIRED_COLUMNS):
        assert column in processed.columns
        assert int(processed[column].isna().sum()) == 0
