from __future__ import annotations

import pandas as pd

import honestroles.match.rank as rank_module
from honestroles.match import CandidateProfile, build_application_plan, rank_jobs
from honestroles.match.models import DEFAULT_RESULT_COLUMNS


def _jobs_fixture() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "job_key": "job-1",
                "title": "Entry Level Data Scientist",
                "description_text": (
                    "Entry level role for new graduates. 0-2 years experience. "
                    "Must have Python and SQL. Visa sponsorship available."
                ),
                "skills": ["Python", "SQL"],
                "apply_url": "https://jobs.example.com/greenhouse/1",
                "remote_flag": True,
                "salary_max": 135000,
                "salary_currency": "USD",
                "rating": 0.9,
            },
            {
                "job_key": "job-2",
                "title": "Senior Platform Engineer",
                "description_text": (
                    "Senior role. 7+ years required. No sponsorship. "
                    "Kubernetes and Go required."
                ),
                "skills": ["Go", "Kubernetes"],
                "apply_url": "https://jobs.example.com/workday/2",
                "remote_flag": False,
                "salary_max": 115000,
                "salary_currency": "USD",
                "rating": 0.8,
            },
        ]
    )


def test_rank_jobs_prioritizes_new_grad_mds_profile() -> None:
    profile = CandidateProfile(
        required_skills=("python", "sql"),
        preferred_skills=("spark",),
        min_salary=120000,
        needs_visa_sponsorship=True,
        max_years_experience=2,
        remote_ok=True,
    )
    ranked = rank_jobs(_jobs_fixture(), profile=profile, use_llm_signals=False)
    columns = DEFAULT_RESULT_COLUMNS

    assert ranked.iloc[0]["job_key"] == "job-1"
    assert ranked.iloc[0][columns.fit_score] > ranked.iloc[1][columns.fit_score]
    assert "visa_sponsorship_not_available" in ranked.iloc[1][columns.missing_requirements]


def test_build_application_plan_adds_actions() -> None:
    profile = CandidateProfile(needs_visa_sponsorship=True)
    ranked = rank_jobs(_jobs_fixture(), profile=profile, use_llm_signals=False)
    planned = build_application_plan(ranked, profile=profile, top_n=2)
    columns = DEFAULT_RESULT_COLUMNS

    assert columns.next_actions in planned.columns
    assert isinstance(planned.iloc[0][columns.next_actions], list)
    assert planned.iloc[0][columns.next_actions]


def test_rank_jobs_component_override_hook() -> None:
    def _force_low_quality(row: pd.Series, profile: CandidateProfile) -> float:
        del row, profile
        return 0.0

    ranked = rank_jobs(
        _jobs_fixture(),
        profile=CandidateProfile(),
        use_llm_signals=False,
        component_overrides={"quality": _force_low_quality},
    )
    columns = DEFAULT_RESULT_COLUMNS
    assert ranked.iloc[0][columns.fit_breakdown]["quality"] == 0.0


def test_rank_jobs_remote_flag_string_false_is_not_treated_as_remote() -> None:
    df = pd.DataFrame(
        [
            {
                "job_key": "job-remote-string-false",
                "title": "Data Scientist",
                "description_text": "Entry level data role.",
                "location_raw": "Austin, TX, USA",
                "country": "US",
                "remote_flag": "False",
                "apply_url": "https://example.com",
                "salary_max": 120000,
            }
        ]
    )
    profile = CandidateProfile(remote_ok=True, preferred_countries=("CA",))
    ranked = rank_jobs(df, profile=profile, use_llm_signals=False)
    columns = DEFAULT_RESULT_COLUMNS
    assert ranked.iloc[0][columns.fit_breakdown]["location"] < 1.0


def test_rank_jobs_uses_target_roles_and_graduation_year() -> None:
    df = pd.DataFrame(
        [
            {
                "job_key": "data-role",
                "title": "Data Scientist",
                "description_text": "Entry level role for new graduates. 1 year experience.",
                "apply_url": "https://example.com",
                "salary_max": 125000,
            },
            {
                "job_key": "sales-role",
                "title": "Sales Manager",
                "description_text": "Customer pipeline role requiring 1 year experience.",
                "apply_url": "https://example.com",
                "salary_max": 125000,
            },
        ]
    )
    profile = CandidateProfile(
        target_roles=("data scientist", "machine learning engineer"),
        graduation_year=2025,
    )
    ranked = rank_jobs(df, profile=profile, use_llm_signals=False)
    columns = DEFAULT_RESULT_COLUMNS

    assert ranked.iloc[0]["job_key"] == "data-role"
    assert "role_alignment" in ranked.iloc[0][columns.fit_breakdown]
    assert "graduation_alignment" in ranked.iloc[0][columns.fit_breakdown]


def test_experience_score_defaults_when_years_min_missing() -> None:
    row = pd.Series({DEFAULT_RESULT_COLUMNS.experience_years_min: None})
    score = rank_module._experience_score(
        row,
        CandidateProfile(max_years_experience=2),
        DEFAULT_RESULT_COLUMNS,
    )
    assert score == 0.6
