from __future__ import annotations

import logging
import re

import pandas as pd
from bs4 import BeautifulSoup

from honestroles.schema import DESCRIPTION_HTML, DESCRIPTION_TEXT

LOGGER = logging.getLogger(__name__)

_BOILERPLATE_PATTERNS = [
    re.compile(r"equal employment opportunity", re.IGNORECASE),
    re.compile(r"\bEEO\b", re.IGNORECASE),
    re.compile(r"we are an equal opportunity employer", re.IGNORECASE),
    re.compile(r"reasonable accommodation", re.IGNORECASE),
    re.compile(r"veteran status", re.IGNORECASE),
]


def _strip_boilerplate(text: str) -> str:
    lines = [line.strip() for line in text.splitlines()]
    cleaned_lines: list[str] = []
    for line in lines:
        if not line:
            continue
        if any(pattern.search(line) for pattern in _BOILERPLATE_PATTERNS):
            continue
        cleaned_lines.append(line)
    return "\n".join(cleaned_lines)


def strip_html(
    df: pd.DataFrame,
    *,
    html_column: str = DESCRIPTION_HTML,
    text_column: str = DESCRIPTION_TEXT,
) -> pd.DataFrame:
    if html_column not in df.columns:
        LOGGER.warning("HTML column %s not found; returning input DataFrame.", html_column)
        return df
    result = df.copy()

    def convert(value: str | None) -> str | None:
        if value is None or value == "":
            return None
        soup = BeautifulSoup(value, "html.parser")
        text = soup.get_text(separator="\n")
        text = _strip_boilerplate(text)
        return text.strip() or None

    result[text_column] = result[html_column].apply(convert)
    return result
