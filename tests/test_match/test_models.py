from __future__ import annotations

from honestroles.match.models import CandidateProfile, MatchWeights


def test_candidate_profile_mds_new_grad_factory() -> None:
    profile = CandidateProfile.mds_new_grad()
    assert isinstance(profile, CandidateProfile)
    assert profile.required_skills


def test_match_weights_as_dict_contains_new_components() -> None:
    weights = MatchWeights().as_dict()
    assert "role_alignment" in weights
    assert "graduation_alignment" in weights
    assert "active" in weights
    assert "friction" in weights
    assert "confidence" in weights
