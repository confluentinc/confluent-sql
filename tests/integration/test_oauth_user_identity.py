"""Integration test: the control-plane token of a real login carries an extractable user id (#277).

Reauthenticating must land on the same user as the original login, which needs a stable user
identifier we can pin at first login and compare at reauth. This test settles, against
production, that the CP token's JWT payload really has one -- `userResourceId` -- before the
driver comes to depend on it.

The JWT is decoded here independently of the driver's own extraction code, so this stays a valid
oracle for it. Only claim *names* and the id's shape are reported or asserted, never the values of
anything PII-ish.

Opt-in on the same convention as `test_oauth.py` (skips unless RUN_OAUTH_TESTS is set, and the
shared `oauth_base_connect_args` env vars are present); one human browser login:

    RUN_OAUTH_TESTS=1 uv run pytest -n0 tests/integration/test_oauth_user_identity.py
"""

import base64
import json
from collections.abc import Generator
from typing import Any

import pytest

from confluent_sql import Connection, connect, oauth_discard_login
from confluent_sql.oauth import ProcessOAuthHolder
from confluent_sql.oauth.provider import CCloudOAuth

pytestmark = pytest.mark.oauth


def _jwt_payload(token: str) -> dict[str, Any]:
    """Unverified payload of a JWT, decoded with no help from the driver."""
    segment = token.split(".")[1]
    return json.loads(base64.urlsafe_b64decode(segment + "=" * (-len(segment) % 4)))


@pytest.fixture(scope="module")
def cp_token_payload(
    oauth_base_connect_args: dict[str, str],
) -> Generator[dict[str, Any], Any, None]:
    """The decoded CP token payload of one real login, discarded again afterward."""
    oauth_discard_login()
    conn: Connection = connect(**oauth_base_connect_args)  # type: ignore
    try:
        provider = ProcessOAuthHolder.instance()._provider
        assert isinstance(provider, CCloudOAuth) and provider.token_set is not None
        payload = _jwt_payload(provider.token_set.cp_token)
        # The driver's own extraction must agree with the independent decode above.
        assert provider.user_identity is not None
        assert provider.user_identity.user_resource_id == payload["userResourceId"]
        assert provider.user_identity.sub == payload["sub"]
        yield payload
    finally:
        conn.close()
        oauth_discard_login()


@pytest.mark.integration
class TestOauthUserIdentity:
    def test_cp_token_claim_names(self, cp_token_payload: dict[str, Any]):
        """Not an assertion about the driver: surfaces the claim names (run with -s / on failure)
        so a surprise in the token's shape is visible without dumping any values."""
        print(f"CP token claims: {sorted(cp_token_payload)}")
        assert cp_token_payload

    def test_user_resource_id_is_extractable(self, cp_token_payload: dict[str, Any]):
        user_resource_id = cp_token_payload.get("userResourceId")

        assert isinstance(user_resource_id, str) and user_resource_id, (
            f"No usable `userResourceId` in the CP token; claims present: "
            f"{sorted(cp_token_payload)}"
        )
        assert user_resource_id.startswith("u-"), "expected a Confluent `u-...` resource id"

    def test_sub_fallback_is_present(self, cp_token_payload: dict[str, Any]):
        """`sub` is the planned fallback identity if `userResourceId` is ever absent."""
        sub = cp_token_payload.get("sub")
        assert isinstance(sub, str) and sub
