import pandas as pd
import pytest

from honestroles.clean import strip_html
from honestroles.clean.html import _strip_boilerplate


def test_strip_html(sample_df: pd.DataFrame) -> None:
    cleaned = strip_html(sample_df)
    assert "description_text" in cleaned.columns
    assert cleaned.loc[0, "description_text"].startswith("Build")


def test_strip_html_missing_column(sample_df: pd.DataFrame, caplog: pytest.LogCaptureFixture) -> None:
    df = sample_df.drop(columns=["description_html"])
    cleaned = strip_html(df)
    assert cleaned.equals(df)
    assert any("HTML column" in record.message for record in caplog.records)


def test_strip_html_none_or_empty_values() -> None:
    df = pd.DataFrame({"description_html": [None, "", "<p></p>"]})
    cleaned = strip_html(df)
    assert cleaned["description_text"].tolist() == [None, None, None]


def test_strip_html_explicit_none_object_value() -> None:
    df = pd.DataFrame({"description_html": pd.Series([None], dtype="object")})
    cleaned = strip_html(df)
    assert cleaned["description_text"].tolist() == [None]


def test_strip_html_nan_and_non_string_values() -> None:
    df = pd.DataFrame({"description_html": [float("nan"), 123]})
    cleaned = strip_html(df)
    assert cleaned["description_text"].tolist() == [None, None]


def test_strip_html_drops_blank_lines_created_from_html() -> None:
    df = pd.DataFrame({"description_html": ["<p>One</p><p></p><p>Two</p>"]})
    cleaned = strip_html(df)
    assert cleaned.loc[0, "description_text"] == "One\nTwo"


def test_strip_boilerplate_skips_blank_lines() -> None:
    text = "First line\n\nSecond line"
    assert _strip_boilerplate(text) == "First line\nSecond line"


def test_strip_html_removes_boilerplate() -> None:
    html = "<p>Great job.</p><p>Equal Employment Opportunity</p>"
    df = pd.DataFrame({"description_html": [html]})
    cleaned = strip_html(df)
    assert cleaned.loc[0, "description_text"] == "Great job."


def test_strip_html_custom_columns() -> None:
    df = pd.DataFrame({"html_body": ["<p>Hello</p>"]})
    cleaned = strip_html(df, html_column="html_body", text_column="text_body")
    assert cleaned.loc[0, "text_body"] == "Hello"


def test_strip_html_preserves_existing_text_when_overwrite_disabled() -> None:
    df = pd.DataFrame(
        {
            "description_html": ["<p>Parsed text</p>"],
            "description_text": ["Already normalized"],
        }
    )
    cleaned = strip_html(df, overwrite_existing=False)
    assert cleaned.loc[0, "description_text"] == "Already normalized"


def test_strip_html_overwrite_disabled_creates_text_column_when_missing() -> None:
    df = pd.DataFrame({"description_html": ["<p>Hello</p>"]})
    cleaned = strip_html(df, overwrite_existing=False)
    assert cleaned.loc[0, "description_text"] == "Hello"
