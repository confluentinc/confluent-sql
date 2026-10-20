"""Integration test: a bogus `organization_id` under OAuth yields the org-aware error (#271).

Opt-in on the same convention as `test_oauth.py`, via the shared `oauth_base_connect_args`
fixture in conftest (skips unless RUN_OAUTH_TESTS is set), since the request that CCloud rejects
is only issued after a human completes one browser login.

Why this is not in `test_oauth.py`: that module shares one login across all its tests through a
module-scoped connection, for a single browser bounce per run. This test needs the opposite. It
must `oauth_discard_login()` first, because otherwise the process-wide holder hands back the
already-established provider and the one-identity guard fails with a *different* error before
CCloud is ever reached. But discarding closes the shared provider's HTTP client immediately, so
doing it mid-way through `test_oauth.py` would break that module's still-live connection at its
next token refresh. A separate module keeps the two isolated (pytest runs modules sequentially
under `-n0`), at the cost of up to two extra browser logins: the rejected one, then a fresh one
proving nothing leaked from it.

    RUN_OAUTH_TESTS=1 uv run pytest -n0 tests/integration/test_oauth_rejected_org.py
"""

import pytest

from confluent_sql import OperationalError, connect

pytestmark = pytest.mark.oauth


BOGUS_ORGANIZATION_ID = "00000000-0000-0000-0000-000000000000"


@pytest.mark.integration
def test_bogus_organization_id_is_explained(
    fresh_oauth_process, oauth_base_connect_args: dict[str, str]
):
    with pytest.raises(OperationalError) as caught:
        connect(organization_id=BOGUS_ORGANIZATION_ID, **oauth_base_connect_args)  # type: ignore

    assert BOGUS_ORGANIZATION_ID in str(caught.value)
    assert "Omit organization_id" in str(caught.value)
    assert "logged in as" in str(caught.value)
    assert caught.value.http_status_code == 403

    # Nothing was established: a follow-up connect without an org must run a fresh login
    # (and so succeed) rather than inherit anything from the failed attempt.
    conn = connect(**oauth_base_connect_args)  # type: ignore
    try:
        assert conn.organization_id
    finally:
        conn.close()
