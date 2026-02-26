from __future__ import annotations

from hypothesis import strategies as st


scalar = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(min_value=-10_000, max_value=10_000),
    st.floats(allow_nan=False, allow_infinity=False, width=32),
    st.text(min_size=0, max_size=128),
)

bool_like = st.one_of(
    st.booleans(),
    st.sampled_from(["true", "false", "1", "0", "yes", "no", "remote", ""]),
    st.none(),
)

salary_like = st.one_of(
    st.none(),
    st.integers(min_value=0, max_value=500_000),
    st.floats(min_value=0, max_value=500_000, allow_nan=False, allow_infinity=False),
    st.text(min_size=0, max_size=32),
)

skill_like = st.one_of(
    st.none(),
    st.text(min_size=0, max_size=64),
    st.lists(st.text(min_size=0, max_size=16), max_size=8),
)

row_strategy = st.fixed_dictionaries(
    {
        "id": st.text(min_size=0, max_size=32),
        "title": st.one_of(st.none(), st.text(min_size=0, max_size=80)),
        "company": st.one_of(st.none(), st.text(min_size=0, max_size=80)),
        "location": st.one_of(st.none(), st.text(min_size=0, max_size=120)),
        "remote": bool_like,
        "description_text": st.one_of(st.none(), st.text(min_size=0, max_size=500)),
        "description_html": st.one_of(st.none(), st.text(min_size=0, max_size=500)),
        "skills": skill_like,
        "salary_min": salary_like,
        "salary_max": salary_like,
        "apply_url": st.one_of(st.none(), st.text(min_size=0, max_size=120)),
        "posted_at": st.one_of(st.none(), st.text(min_size=0, max_size=64)),
    }
)

rows_strategy = st.lists(row_strategy, min_size=0, max_size=40)

plugin_name = st.from_regex(r"[a-z_][a-z0-9_]{0,15}", fullmatch=True)
plugin_kind = st.sampled_from(["filter", "label", "rate"])
callable_ref = st.one_of(
    st.just("tests.plugins.fixture_plugins:filter_min_quality"),
    st.just("tests.plugins.fixture_plugins:label_note"),
    st.just("tests.plugins.fixture_plugins:rate_bonus"),
    st.text(min_size=0, max_size=40),
)

cli_tokens = st.lists(
    st.one_of(
        st.sampled_from(
            [
                "run",
                "plugins",
                "config",
                "validate",
                "report-quality",
                "--pipeline-config",
                "--plugins",
                "--manifest",
                "--pipeline",
            ]
        ),
        st.text(min_size=0, max_size=24),
    ),
    min_size=0,
    max_size=12,
)
