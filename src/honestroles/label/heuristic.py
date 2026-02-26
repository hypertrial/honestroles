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
    "engineering": (
        "software engineer",
        "engineer",
        "developer",
        "backend",
        "frontend",
        "full stack",
        "devops",
        "site reliability",
        "sre",
    ),
    "data": (
        "data scientist",
        "data engineer",
        "analytics",
        "machine learning",
        "business intelligence",
    ),
    "design": ("designer", "ux", "ui", "product design"),
    "product": (
        "product manager",
        "product owner",
        "product management",
        "program manager",
    ),
    "marketing": ("marketing", "growth", "demand gen", "seo"),
    "sales": ("sales", "account executive", "business development", "sales development"),
    "operations": ("operations", "ops", "reliability", "program operations"),
    "finance": ("finance", "accounting", "fp&a", "controller"),
    "hr": ("human resources", "people operations", "recruiter", "talent"),
    "legal": ("legal", "counsel", "attorney", "compliance"),
    "support": ("support", "customer success", "help desk", "technical support"),
}

_ROLE_PRIORITY = (
    "engineering",
    "data",
    "design",
    "product",
    "marketing",
    "sales",
    "operations",
    "finance",
    "hr",
    "legal",
    "support",
)

_ROLE_PARSE_TEXT_MAX_CHARS = 600
_TECH_PARSE_TEXT_MAX_CHARS = 600

_TECH_ALIASES = {
    "python": ("python",),
    "sql": ("sql",),
    "javascript": ("javascript", "js"),
    "typescript": ("typescript", "ts"),
    "react": ("react", "reactjs"),
    "node": ("node", "nodejs", "node.js"),
    "aws": ("aws", "amazon web services"),
    "gcp": ("gcp", "google cloud", "google cloud platform"),
    "azure": ("azure", "microsoft azure"),
    "docker": ("docker",),
    "kubernetes": ("kubernetes", "k8s"),
    "postgres": ("postgres", "postgresql"),
    "mysql": ("mysql",),
    "java": ("java",),
    "excel": ("excel", "microsoft excel"),
}


def _compile_keyword_pattern(keyword: str) -> re.Pattern[str]:
    escaped = re.escape(keyword).replace(r"\ ", r"\s+")
    return re.compile(rf"(?<![a-z0-9]){escaped}(?![a-z0-9])", re.IGNORECASE)


def _keyword_fragment(keyword: str) -> str:
    return re.escape(keyword).replace(r"\ ", r"\s+")


_ROLE_PATTERNS = {
    category: re.compile(
        rf"(?<![a-z0-9])(?:{'|'.join(_keyword_fragment(keyword) for keyword in keywords)})(?![a-z0-9])",
        re.IGNORECASE,
    )
    for category, keywords in _ROLE_KEYWORDS.items()
}
_TECH_ALIAS_TO_CANONICAL = {
    alias: canonical
    for canonical, aliases in _TECH_ALIASES.items()
    for alias in aliases
}
_TECH_PATTERNS = [
    (canonical, _compile_keyword_pattern(alias))
    for alias, canonical in sorted(_TECH_ALIAS_TO_CANONICAL.items())
]
_TECH_EXTRACT_RE = re.compile(
    r"(?<![a-z0-9])("
    + "|".join(_keyword_fragment(alias) for alias in sorted(_TECH_ALIAS_TO_CANONICAL, key=len, reverse=True))
    + r")(?![a-z0-9])",
    re.IGNORECASE,
)


def _normalize_tech_token(token: str) -> str | None:
    normalized = token.strip().lower().replace("-", " ")
    return _TECH_ALIAS_TO_CANONICAL.get(normalized)


def _skills_from_value(value: object) -> set[str]:
    if value is None:
        return set()
    if isinstance(value, float) and pd.isna(value):
        return set()
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return set()
        delimiter = ";" if ";" in stripped else ("," if "," in stripped else None)
        parts = [part.strip() for part in stripped.split(delimiter)] if delimiter else [stripped]
    elif isinstance(value, (list, tuple, set)):
        parts = [str(item).strip() for item in value if str(item).strip()]
    else:
        parts = [str(value).strip()]
    normalized = {
        canonical
        for part in parts
        for canonical in [_normalize_tech_token(part)]
        if canonical is not None
    }
    return normalized


def _extract_tech_terms(text: str) -> set[str]:
    if not text:
        return set()
    found: set[str] = set()
    for canonical, pattern in _TECH_PATTERNS:
        if pattern.search(text):
            found.add(canonical)
    return found


def _extract_tech_terms_series(text: pd.Series) -> pd.Series:
    lowered = text.astype("string").fillna("").str.lower().str.slice(0, _TECH_PARSE_TEXT_MAX_CHARS)
    extracted = pd.Series([set() for _ in range(len(lowered))], index=lowered.index, dtype="object")
    if lowered.empty:
        return extracted
    matches = lowered.str.extractall(_TECH_EXTRACT_RE)
    if matches.empty:
        return extracted
    canonical = matches[0].map(_normalize_tech_token).dropna()
    if canonical.empty:
        return extracted
    grouped = canonical.groupby(level=0).agg(lambda values: set(values))
    extracted.loc[grouped.index] = grouped.astype("object")
    return extracted


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

    inferred = result[title_column].apply(match).astype("object")
    if "seniority" in result.columns:
        existing = result["seniority"].astype("object")
        keep_existing = existing.astype("string").fillna("").str.strip().ne("")
        result["seniority"] = existing.where(keep_existing, inferred)
    else:
        result["seniority"] = inferred
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
    title_text = (
        result[title_column].astype("string").fillna("").str.lower()
        if title_column in result.columns
        else pd.Series("", index=result.index, dtype="string")
    )
    description_text = (
        result[description_column]
        .astype("string")
        .fillna("")
        .str.lower()
        .str.slice(0, _ROLE_PARSE_TEXT_MAX_CHARS)
        if description_column in result.columns
        else pd.Series("", index=result.index, dtype="string")
    )
    categories = pd.Series([None] * len(result), index=result.index, dtype="object")
    unresolved = categories.isna()

    for category in _ROLE_PRIORITY:
        if not bool(unresolved.any()):
            break
        pattern = _ROLE_PATTERNS[category]
        unresolved_idx = unresolved.index[unresolved]
        title_match = title_text.loc[unresolved_idx].str.contains(pattern, na=False)
        categories.loc[title_match.index[title_match]] = category
        unresolved = categories.isna()

    if bool(unresolved.any()):
        for category in _ROLE_PRIORITY:
            if not bool(unresolved.any()):
                break
            pattern = _ROLE_PATTERNS[category]
            unresolved_idx = unresolved.index[unresolved]
            description_match = description_text.loc[unresolved_idx].str.contains(pattern, na=False)
            categories.loc[description_match.index[description_match]] = category
            unresolved = categories.isna()

    result["role_category"] = categories
    return result


def label_tech_stack(
    df: pd.DataFrame,
    *,
    skills_column: str = SKILLS,
    description_column: str = DESCRIPTION_TEXT,
) -> pd.DataFrame:
    result = df.copy()
    skills_terms = (
        result[skills_column].map(_skills_from_value)
        if skills_column in result.columns
        else pd.Series([set() for _ in range(len(result))], index=result.index, dtype="object")
    )
    description_terms = (
        _extract_tech_terms_series(result[description_column])
        if description_column in result.columns
        else pd.Series([set() for _ in range(len(result))], index=result.index, dtype="object")
    )
    merged = pd.Series(
        [left.union(right) for left, right in zip(skills_terms.tolist(), description_terms.tolist())],
        index=result.index,
        dtype="object",
    )
    result["tech_stack"] = merged.map(lambda terms: sorted(terms)).astype("object")
    return result
