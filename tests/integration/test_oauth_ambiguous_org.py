"""Integration test: an omitted `organization_id` that reaches several organizations (#311).

Opt-in on the same convention as `test_oauth.py` (skips unless RUN_OAUTH_TESTS is set, via the
shared `oauth_base_connect_args` fixture): the organizations route is only reachable after a
human completes one browser login.

Mostly live, partly faked, so it needs no particular account. The real login runs, and the real
`GET /org/v2/organizations` request runs with the real OAuth token and is really parsed. The one
fake: `Connection._lookup_organizations` is wrapped so that an account reaching a single
organization appears to reach two, by appending a bogus entry to the real answer. An account that
genuinely reaches several is left untouched. Either way the driver faces the ambiguity that the
issue is about, with no skip and no dependence on CONFLUENT_ORG_ID. The bogus entry is only ever
a candidate in the error; it is never used as an organization.

The test then retries naming a real organization from that same listing, as the error's message
tells a user to. That is the live check that the ambiguity error fails the login it came from
(which only held Confluent Cloud's default pick) instead of leaving it established: otherwise the
retry would be refused by the process's one-identity guard. The price is a second browser login,
so, like
`test_oauth_rejected_org.py`, this lives in a module of its own.

    RUN_OAUTH_TESTS=1 uv run pytest -n0 tests/integration/test_oauth_ambiguous_org.py
"""

import pytest

from confluent_sql import AmbiguousOrganizationError, connect
from confluent_sql.connection import Connection

pytestmark = pytest.mark.oauth

BOGUS_EXTRA_ORGANIZATION: tuple[str, str | None] = (
    "00000000-0000-0000-0000-00000000000b",
    "Bogus Extra Organization",
)
"""Appended to a single-organization account's real listing so it looks ambiguous."""


@pytest.mark.integration_with_mocks
def test_ambiguous_organization_is_reported_then_an_explicit_one_connects(
    fresh_oauth_process, oauth_base_connect_args: dict[str, str], monkeypatch
):
    real_organizations: list[tuple[str, str | None]] = []
    real_lookup = Connection._lookup_organizations

    def lookup_reaching_at_least_two(self: Connection) -> list[tuple[str, str | None]]:
        organizations = real_lookup(self)
        real_organizations.extend(organizations)
        if len(organizations) < 2:
            organizations = [*organizations, BOGUS_EXTRA_ORGANIZATION]
        return organizations

    monkeypatch.setattr(Connection, "_lookup_organizations", lookup_reaching_at_least_two)

    with pytest.raises(AmbiguousOrganizationError) as caught:
        connect(**oauth_base_connect_args)  # type: ignore

    assert real_organizations, "the real organizations route returned nothing"
    candidate_ids = caught.value.organization_ids
    assert len(candidate_ids) >= 2
    for organization_id, _ in real_organizations:
        assert organization_id in candidate_ids
        assert organization_id in str(caught.value)
    assert "organization_id" in str(caught.value)

    # Back to the real lookup, then do what the error says: name one of the real candidates.
    monkeypatch.undo()
    chosen_id = real_organizations[0][0]
    conn = connect(organization_id=chosen_id, **oauth_base_connect_args)  # type: ignore
    try:
        assert conn.organization_id == chosen_id
    finally:
        conn.close()
