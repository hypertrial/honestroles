from __future__ import annotations

from datetime import date

import pandas as pd

import honestroles.match.rank as rank_module
from honestroles.match.models import CandidateProfile, DEFAULT_RESULT_COLUMNS


def test_rank_helper_parsers_and_basic_paths() -> None:
    assert rank_module._as_float(float("nan")) is None
    assert rank_module._as_float("nope") is None
    assert rank_module._to_skill_set(float("nan")) == set()
    assert rank_module._to_skill_set("Python") == {"python"}
    assert rank_module._to_skill_set(7) == {"7"}

    row = pd.Series({})
    profile = CandidateProfile(required_skills=(), preferred_skills=())
    score, missing = rank_module._skill_score(row, profile, DEFAULT_RESULT_COLUMNS)
    assert score == 0.85
    assert missing == []

    assert rank_module._entry_level_score(pd.Series({}), DEFAULT_RESULT_COLUMNS) == 0.55
    assert (
        rank_module._experience_score(
            pd.Series({DEFAULT_RESULT_COLUMNS.experience_years_min: 2}),
            CandidateProfile(max_years_experience=-1),
            DEFAULT_RESULT_COLUMNS,
        )
        == 0.0
    )
    assert (
        rank_module._experience_score(
            pd.Series({DEFAULT_RESULT_COLUMNS.experience_years_min: "oops"}),
            CandidateProfile(max_years_experience=2),
            DEFAULT_RESULT_COLUMNS,
        )
        == 0.5
    )
    assert (
        rank_module._visa_score(
            pd.Series({DEFAULT_RESULT_COLUMNS.visa_sponsorship_signal: None}),
            CandidateProfile(needs_visa_sponsorship=True),
            DEFAULT_RESULT_COLUMNS,
        )
        == 0.5
    )
    assert (
        rank_module._visa_score(
            pd.Series(
                {
                    DEFAULT_RESULT_COLUMNS.visa_sponsorship_signal: None,
                    DEFAULT_RESULT_COLUMNS.citizenship_required: True,
                }
            ),
            CandidateProfile(needs_visa_sponsorship=True),
            DEFAULT_RESULT_COLUMNS,
            ranking_profile="job_seeker_v2",
        )
        == 0.0
    )


def test_rank_salary_location_quality_role_and_grad_branches() -> None:
    assert (
        rank_module._salary_score(
            pd.Series({}),
            CandidateProfile(min_salary=100000, salary_currency="USD"),
        )
        == 0.45
    )
    assert (
        rank_module._salary_score(
            pd.Series({"salary_max": 90000, "salary_currency": "EUR"}),
            CandidateProfile(min_salary=100000, salary_currency="USD"),
        )
        == 0.4
    )
    assert (
        rank_module._quality_score(
            pd.Series({"quality_score": 0.3}),
            DEFAULT_RESULT_COLUMNS,
        )
        == 0.3
    )
    assert (
        rank_module._role_alignment_score(
            pd.Series({"title": "Anything", "description_text": "Text"}),
            CandidateProfile(target_roles=()),
        )
        == 0.6
    )
    assert (
        rank_module._role_alignment_score(
            pd.Series({"title": "", "description_text": ""}),
            CandidateProfile(target_roles=("data scientist",)),
        )
        == 0.3
    )
    assert (
        rank_module._role_alignment_score(
            pd.Series({"title": "Engineer", "description_text": "Work with data scientist stakeholders."}),
            CandidateProfile(target_roles=("data scientist",)),
        )
        == 0.85
    )
    assert rank_module._role_alignment_score(
        pd.Series({"title": "Systems Role", "description_text": "Work on machine tooling."}),
        CandidateProfile(target_roles=("machine learning engineer",)),
    ) >= 0.35

    current_year = date.today().year
    assert (
        rank_module._graduation_alignment_score(
            pd.Series({DEFAULT_RESULT_COLUMNS.entry_level_likely: None}),
            CandidateProfile(graduation_year=current_year + 3),
            DEFAULT_RESULT_COLUMNS,
        )
        == 0.75
    )
    assert (
        rank_module._graduation_alignment_score(
            pd.Series({DEFAULT_RESULT_COLUMNS.experience_years_min: "bad"}),
            CandidateProfile(graduation_year=current_year),
            DEFAULT_RESULT_COLUMNS,
        )
        == 0.75
    )
    assert (
        rank_module._graduation_alignment_score(
            pd.Series({DEFAULT_RESULT_COLUMNS.experience_years_min: 3}),
            CandidateProfile(graduation_year=current_year),
            DEFAULT_RESULT_COLUMNS,
        )
        == 0.15
    )
    assert (
        rank_module._graduation_alignment_score(
            pd.Series({DEFAULT_RESULT_COLUMNS.experience_years_min: 6}),
            CandidateProfile(graduation_year=current_year - 3),
            DEFAULT_RESULT_COLUMNS,
        )
        == 0.4
    )
    assert (
        rank_module._graduation_alignment_score(
            pd.Series({DEFAULT_RESULT_COLUMNS.experience_years_min: 2}),
            CandidateProfile(graduation_year=current_year - 3),
            DEFAULT_RESULT_COLUMNS,
        )
        == 0.65
    )
    assert (
        rank_module._graduation_alignment_score(
            pd.Series({DEFAULT_RESULT_COLUMNS.experience_years_min: 2}),
            CandidateProfile(graduation_year=current_year - 10),
            DEFAULT_RESULT_COLUMNS,
        )
        == 0.45
    )


def test_rank_location_all_preference_branches() -> None:
    assert (
        rank_module._location_score(
            pd.Series({"city": "boston"}),
            CandidateProfile(preferred_cities=("Boston",)),
        )
        == 1.0
    )
    assert (
        rank_module._location_score(
            pd.Series({"region": "massachusetts"}),
            CandidateProfile(preferred_regions=("Massachusetts",)),
        )
        == 0.9
    )
    assert (
        rank_module._location_score(
            pd.Series({"country": "US"}),
            CandidateProfile(preferred_countries=("US",)),
        )
        == 0.85
    )
    assert (
        rank_module._location_score(
            pd.Series({"location_raw": "near boston office"}),
            CandidateProfile(preferred_cities=("Boston",)),
        )
        == 0.75
    )
    assert (
        rank_module._location_score(
            pd.Series({"location_raw": "roles in massachusetts"}),
            CandidateProfile(preferred_regions=("Massachusetts",)),
        )
        == 0.7
    )
    assert (
        rank_module._location_score(
            pd.Series({"remote_flag": "False", "country": "US"}),
            CandidateProfile(preferred_countries=("CA",), remote_ok=True),
        )
        == 0.25
    )
    assert (
        rank_module._location_score(
            pd.Series({"country": "US"}),
            CandidateProfile(preferred_countries=(), preferred_cities=(), preferred_regions=()),
        )
        == 0.7
    )


def test_rank_component_overrides_and_misc_branches() -> None:
    row = pd.Series(
        {
            DEFAULT_RESULT_COLUMNS.experience_years_min: "bad",
            DEFAULT_RESULT_COLUMNS.visa_sponsorship_signal: None,
            "title": "Role",
            "description_text": "Text",
        }
    )

    components, missing = rank_module._score_components(
        row,
        CandidateProfile(max_years_experience=2),
        DEFAULT_RESULT_COLUMNS,
        ranking_profile="legacy",
        component_overrides={
            "unknown_component": lambda r, p: 1.0,
            "skills": lambda r, p: float("nan-string"),  # type: ignore[arg-type]
        },
    )
    assert "skills" in components
    assert isinstance(missing, list)
    assert rank_module._weighted_score({"skills": 1.0}, {}) == 0.0
    assert "moderate fit" in rank_module._why_match({"skills": 0.2}, [], "Role")


def test_rank_new_component_scores_defaults() -> None:
    row = pd.Series(
        {
            DEFAULT_RESULT_COLUMNS.active_likelihood: None,
            DEFAULT_RESULT_COLUMNS.application_friction_score: 0.25,
            DEFAULT_RESULT_COLUMNS.signal_confidence: None,
            "salary_confidence": 0.8,
        }
    )
    assert rank_module._active_score(row, DEFAULT_RESULT_COLUMNS) == 0.5
    assert rank_module._friction_score(row, DEFAULT_RESULT_COLUMNS) == 0.75
    assert rank_module._confidence_score(row, DEFAULT_RESULT_COLUMNS) == 0.8

    row2 = pd.Series(
        {
            DEFAULT_RESULT_COLUMNS.application_friction_score: None,
            DEFAULT_RESULT_COLUMNS.signal_confidence: None,
            "salary_confidence": None,
        }
    )
    assert rank_module._friction_score(row2, DEFAULT_RESULT_COLUMNS) == 0.6
    assert rank_module._confidence_score(row2, DEFAULT_RESULT_COLUMNS) == 0.5


def test_rank_v2_missing_requirements_branches() -> None:
    row = pd.Series(
        {
            DEFAULT_RESULT_COLUMNS.work_authorization_required: True,
            DEFAULT_RESULT_COLUMNS.citizenship_required: True,
            DEFAULT_RESULT_COLUMNS.visa_sponsorship_signal: None,
            DEFAULT_RESULT_COLUMNS.active_likelihood: 0.2,
            DEFAULT_RESULT_COLUMNS.experience_years_min: 1,
        }
    )
    profile = CandidateProfile(needs_visa_sponsorship=True)
    assert (
        rank_module._visa_score(
            row,
            profile,
            DEFAULT_RESULT_COLUMNS,
            ranking_profile="job_seeker_v2",
        )
        == 0.0
    )
    row_no_citizenship = row.copy()
    row_no_citizenship[DEFAULT_RESULT_COLUMNS.citizenship_required] = False
    assert (
        rank_module._visa_score(
            row_no_citizenship,
            profile,
            DEFAULT_RESULT_COLUMNS,
            ranking_profile="job_seeker_v2",
        )
        == 0.3
    )

    _, missing = rank_module._score_components(
        row_no_citizenship,
        profile,
        DEFAULT_RESULT_COLUMNS,
        ranking_profile="job_seeker_v2",
    )
    assert "work_authorization_constraint" in missing
    assert "likely_stale_posting" in missing

    _, missing_with_citizenship = rank_module._score_components(
        row,
        profile,
        DEFAULT_RESULT_COLUMNS,
        ranking_profile="job_seeker_v2",
    )
    assert "citizenship_constraint" in missing_with_citizenship


def test_build_application_plan_role_category_and_risk_branches() -> None:
    df = pd.DataFrame(
        [
            {
                DEFAULT_RESULT_COLUMNS.fit_score: 0.8,
                DEFAULT_RESULT_COLUMNS.missing_requirements: ["likely_stale_posting", "salary_below_minimum"],
                DEFAULT_RESULT_COLUMNS.visa_sponsorship_signal: None,
                DEFAULT_RESULT_COLUMNS.citizenship_required: True,
                DEFAULT_RESULT_COLUMNS.work_authorization_required: True,
                DEFAULT_RESULT_COLUMNS.active_likelihood: 0.2,
                DEFAULT_RESULT_COLUMNS.application_friction_score: 0.2,
                DEFAULT_RESULT_COLUMNS.signal_confidence: 0.1,
                "role_category": "product",
            },
            {
                DEFAULT_RESULT_COLUMNS.fit_score: 0.7,
                DEFAULT_RESULT_COLUMNS.missing_requirements: [],
                DEFAULT_RESULT_COLUMNS.visa_sponsorship_signal: True,
                DEFAULT_RESULT_COLUMNS.citizenship_required: False,
                DEFAULT_RESULT_COLUMNS.work_authorization_required: False,
                DEFAULT_RESULT_COLUMNS.active_likelihood: 0.9,
                DEFAULT_RESULT_COLUMNS.application_friction_score: 0.9,
                DEFAULT_RESULT_COLUMNS.signal_confidence: 0.9,
                "role_category": "engineering",
            },
        ]
    )
    planned = rank_module.build_application_plan(
        df,
        profile=CandidateProfile(needs_visa_sponsorship=True),
        top_n=-1,
        include_diagnostics=True,
        max_actions_per_job=10,
    )
    first_actions = planned.loc[0, DEFAULT_RESULT_COLUMNS.next_actions]
    second_actions = planned.loc[1, DEFAULT_RESULT_COLUMNS.next_actions]
    assert any("roadmap prioritization" in action for action in first_actions)
    assert any("higher-friction process" in action for action in first_actions)
    assert any("quantified technical outcomes" in action for action in second_actions)
    assert planned.loc[0, DEFAULT_RESULT_COLUMNS.offer_risk] == "high"


def test_rank_jobs_weight_update_top_n_and_build_plan_branches() -> None:
    df = pd.DataFrame(
        [
            {
                "job_key": "a",
                "title": "Data Scientist",
                "description_text": "Entry level role.",
                "apply_url": "https://example.com",
                "salary_max": 100000,
            },
            {
                "job_key": "b",
                "title": "Data Analyst",
                "description_text": "Entry level role.",
                "apply_url": "https://example.com",
                "salary_max": 100000,
            },
        ]
    )
    ranked = rank_module.rank_jobs(
        df,
        profile=CandidateProfile(),
        weights={"skills": 1.0},
        top_n=1,
        use_llm_signals=False,
    )
    assert len(ranked) == 1

    # Trigger branch where rank_jobs is called inside build_application_plan.
    planned_from_raw = rank_module.build_application_plan(df, profile=CandidateProfile(), top_n=1)
    assert len(planned_from_raw) == 1

    # Trigger non-list missing branch, sponsorship-none branch, and salary warning branch.
    custom = ranked.copy()
    custom[DEFAULT_RESULT_COLUMNS.missing_requirements] = [
        "not-a-list",
    ]
    custom[DEFAULT_RESULT_COLUMNS.visa_sponsorship_signal] = [None]
    planned = rank_module.build_application_plan(
        custom,
        profile=CandidateProfile(needs_visa_sponsorship=True),
        top_n=-1,
    )
    assert len(planned) == 1

    custom2 = ranked.copy()
    custom2[DEFAULT_RESULT_COLUMNS.missing_requirements] = [["salary_below_minimum"]]
    planned2 = rank_module.build_application_plan(custom2, profile=CandidateProfile(), top_n=1)
    assert any(
        "compensation gap" in action
        for action in planned2.loc[0, DEFAULT_RESULT_COLUMNS.next_actions]
    )
