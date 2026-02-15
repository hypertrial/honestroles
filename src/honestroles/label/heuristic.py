from __future__ import annotations

import re

import pandas as pd

from honestroles.schema import DESCRIPTION_TEXT, SKILLS, TITLE

_SENIORITY_PATTERNS = [
    ("intern", re.compile(r"\bintern\b", re.IGNORECASE)),
    ("junior", re.compile(r"\b(?:junior|jr)\b\.?", re.IGNORECASE)),
    ("mid", re.compile(r"\bmid\b|\bmid-level\b", re.IGNORECASE)),
    ("senior", re.compile(r"\b(?:senior|sr)\b\.?", re.IGNORECASE)),
    ("staff", re.compile(r"\bstaff\b", re.IGNORECASE)),
    ("principal", re.compile(r"\bprincipal\b", re.IGNORECASE)),
    ("lead", re.compile(r"\blead\b", re.IGNORECASE)),
    ("director", re.compile(r"\bdirector\b", re.IGNORECASE)),
    ("vp", re.compile(r"\bvice president\b|\bvp\b", re.IGNORECASE)),
    ("c_level", re.compile(r"\bchief\b|\bceo\b|\bcto\b|\bcfo\b", re.IGNORECASE)),
]

_ROLE_KEYWORDS = {
    "engineering": ["engineer", "developer", "software", "backend", "frontend", "full stack"],
    "data": ["data scientist", "data engineer", "analytics", "machine learning", "ml"],
    "design": ["designer", "ux", "ui", "product design"],
    "product": ["product manager", "product owner", "pm"],
    "marketing": ["marketing", "growth", "demand gen", "seo"],
    "sales": ["sales", "account executive", "bd", "business development"],
    "operations": ["operations", "ops", "sre", "reliability", "devops"],
    "finance": ["finance", "accounting", "fp&a", "controller"],
    "hr": ["human resources", "people", "recruiter", "talent"],
    "legal": ["legal", "counsel", "attorney", "compliance"],
    "support": ["support", "customer success", "cs", "help desk"],
}

_TECH_TERMS = {
    "python",
    "java",
    "javascript",
    "typescript",
    "react",
    "node",
    "aws",
    "gcp",
    "azure",
    "docker",
    "kubernetes",
    "sql",
    "postgres",
    "mysql",
}


def label_seniority(df: pd.DataFrame, *, title_column: str = TITLE) -> pd.DataFrame:
    if title_column not in df.columns:
        return df
    result = df.copy()

    def match(title: str | None) -> str | None:
        if title is None:
            return None
        if isinstance(title, float) and pd.isna(title):
            return None
        if not isinstance(title, str):
            return None
        if not title:
            return None
        for label, pattern in _SENIORITY_PATTERNS:
            if pattern.search(title):
                return label
        return None

    result["seniority"] = result[title_column].apply(match)
    return result


def label_role_category(
    df: pd.DataFrame,
    *,
    title_column: str = TITLE,
    description_column: str = DESCRIPTION_TEXT,
) -> pd.DataFrame:
    result = df.copy()
    if title_column not in result.columns and description_column not in result.columns:
        return result

    def match(row: pd.Series) -> str | None:
        parts = []
        if title_column in row and pd.notna(row[title_column]):
            parts.append(str(row[title_column]).lower())
        if description_column in row and pd.notna(row[description_column]):
            parts.append(str(row[description_column]).lower())
        text = " ".join(parts)
        if not text:
            return None
        for category, keywords in _ROLE_KEYWORDS.items():
            if any(keyword in text for keyword in keywords):
                return category
        return None

    result["role_category"] = result.apply(match, axis=1)
    return result


def label_tech_stack(
    df: pd.DataFrame,
    *,
    skills_column: str = SKILLS,
    description_column: str = DESCRIPTION_TEXT,
) -> pd.DataFrame:
    result = df.copy()

    def match(row: pd.Series) -> list[str]:
        terms = set()
        if skills_column in row and isinstance(row[skills_column], list):
            terms.update({skill.lower() for skill in row[skills_column]})
        if description_column in row and pd.notna(row[description_column]):
            text = str(row[description_column]).lower()
            for term in _TECH_TERMS:
                if term in text:
                    terms.add(term)
        return sorted(terms.intersection(_TECH_TERMS))

    result["tech_stack"] = result.apply(match, axis=1)
    return result
