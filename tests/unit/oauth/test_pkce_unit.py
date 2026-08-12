import pytest

from confluent_sql.oauth.pkce import challenge_for, generate_state, generate_verifier

pytestmark = pytest.mark.unit


def test_challenge_for_matches_rfc7636_vector():
    """Pinned against RFC 7636 Appendix B.1's worked example, independent of our own
    generate_verifier() -- this is the one place we can be sure S256 is implemented correctly
    rather than merely self-consistent."""
    verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
    assert challenge_for(verifier) == "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"


def test_generate_verifier_is_43_char_base64url():
    verifier = generate_verifier()
    assert len(verifier) == 43
    assert all(c.isalnum() or c in "-_" for c in verifier)
    assert "=" not in verifier


def test_generate_verifier_is_random():
    assert generate_verifier() != generate_verifier()


def test_generate_state_is_43_char_base64url():
    state = generate_state()
    assert len(state) == 43
    assert all(c.isalnum() or c in "-_" for c in state)


def test_generate_state_is_random():
    assert generate_state() != generate_state()


def test_challenge_for_is_deterministic_and_base64url():
    verifier = generate_verifier()
    challenge = challenge_for(verifier)
    assert challenge == challenge_for(verifier)
    assert "=" not in challenge
    assert all(c.isalnum() or c in "-_" for c in challenge)
