import dataclasses

import pytest

from confluent_sql import InterfaceError
from confluent_sql.oauth.policy import OAUTH_INTERACTIVE, OAUTH_UNATTENDED, OAuthPolicy

pytestmark = pytest.mark.unit


def test_default_policy_is_auto_reauth():
    assert OAuthPolicy().on_reauthentication_required == "auto"


def test_policy_is_frozen():
    with pytest.raises(dataclasses.FrozenInstanceError):
        OAuthPolicy().on_reauthentication_required = "raise"  # type: ignore[misc]


def test_invalid_on_reauthentication_required_rejected_at_construction():
    with pytest.raises(
        InterfaceError, match="on_reauthentication_required must be 'auto' or 'raise'"
    ):
        OAuthPolicy(on_reauthentication_required="bogus")  # type: ignore[arg-type]


def test_equal_policies_compare_and_hash_equal():
    """Frozen + eq gives structural equality/hash for free -- this is what will let the
    process-wide holder's future cross-connection guard compare whole policies with `!=` rather
    than field-by-field."""
    a = OAuthPolicy(on_reauthentication_required="auto")
    b = OAuthPolicy(on_reauthentication_required="auto")
    assert a == b
    assert hash(a) == hash(b)


def test_differing_policies_compare_unequal():
    assert OAuthPolicy(on_reauthentication_required="auto") != OAuthPolicy(
        on_reauthentication_required="raise"
    )


def test_oauth_interactive_preset():
    assert OAUTH_INTERACTIVE.on_reauthentication_required == "auto"


def test_oauth_unattended_preset():
    assert OAUTH_UNATTENDED.on_reauthentication_required == "raise"
