"""Integration test: reauthenticating as a different user is refused, end to end (#277).

One real account is enough to exercise this: after a real login we doctor the provider's cached
user id to one that cannot be ours, expire the session, and let `reauth="auto"` run a real
reauthentication. Prod mints tokens for the true us, whose id differs from the doctored pin, so
the driver sees exactly what it would see if someone else had signed in. We then restore the
true id and prove the latched session recovers.

Three human browser logins: the initial one, the refused reauthentication, and the recovery.
Its own module (like `test_oauth_rejected_org.py`) since it needs a process-wide login of its
own, and `fresh_oauth_process` discards it before and after.

    RUN_OAUTH_TESTS=1 uv run pytest -n0 tests/integration/test_oauth_user_mismatch.py
"""

import dataclasses
from datetime import datetime, timedelta, timezone

import pytest

from confluent_sql import Connection, OAuthLoginError, OAuthLoginFailure, connect
from confluent_sql.oauth import ProcessOAuthHolder
from confluent_sql.oauth.provider import CCloudOAuth
from confluent_sql.oauth.token_chain import UserIdentity

pytestmark = pytest.mark.oauth


DOCTORED_USER_RESOURCE_ID = "u-not-you"


def _expire_session(provider: CCloudOAuth) -> None:
    """Push the refresh, CP and DP tokens all into the past, so the next request hits the
    absolute-expiry wall without an 8h wait."""
    past = datetime.now(timezone.utc) - timedelta(minutes=1)
    with provider._token_lock:
        assert provider._token_set is not None
        provider._token_set = dataclasses.replace(
            provider._token_set,
            refresh_token_expires_at=past,
            cp_token_expires_at=past,
            dp_token_expires_at=past,
        )


def _set_pinned_user(provider: CCloudOAuth, user_identity: UserIdentity | None) -> None:
    with provider._token_lock:
        provider._user_identity = user_identity


def _probe(conn: Connection) -> None:
    """A cheap control-plane read that needs a live CP token."""
    assert isinstance(conn.get_artifacts_api().list(), list)


@pytest.mark.integration
def test_reauth_as_a_different_user_is_refused_then_recovers(
    fresh_oauth_process, oauth_base_connect_args: dict[str, str]
):
    conn: Connection = connect(reauth="auto", **oauth_base_connect_args)  # type: ignore
    try:
        provider = ProcessOAuthHolder.instance()._provider
        assert isinstance(provider, CCloudOAuth)
        true_identity = provider.user_identity
        assert true_identity is not None and true_identity.user_resource_id
        assert true_identity.user_resource_id != DOCTORED_USER_RESOURCE_ID
        _probe(conn)

        # Become "the impostor": the pinned userResourceId no longer matches whoever really
        # logs in next. (The other claims are kept so the identities stay comparable.)
        doctored = dataclasses.replace(true_identity, user_resource_id=DOCTORED_USER_RESOURCE_ID)
        _set_pinned_user(provider, doctored)
        _expire_session(provider)
        expired = provider.token_set

        with pytest.raises(OAuthLoginError) as caught:
            _probe(conn)  # auto-reauth: the browser pops, you sign in as yourself

        assert caught.value.reason is OAuthLoginFailure.USER_MISMATCH
        # Nothing from the refused login was installed; the session is still the dead one.
        assert provider.token_set is expired
        assert provider.user_identity == doctored

        # Restore the true identity: the next request reauths (browser again) and recovers.
        _set_pinned_user(provider, true_identity)
        _probe(conn)

        assert provider.user_identity == true_identity
        assert provider.token_set is not expired
    finally:
        conn.close()
