"""Integration test: a global key with an omitted `organization_id` settles it in `connect()`
(#311), exactly as `auth="oauth"` does -- no browser needed.

Live, partly faked, so it needs no particular account. The real `GET /org/v2/organizations`
request runs with the real global key and is really parsed. In the ambiguity test one fake is
applied: `Connection._lookup_organizations` is wrapped so that an account reaching a single
organization appears to reach two, by appending a bogus entry to the real answer (an account that
genuinely reaches several is left untouched). The bogus entry is only ever a candidate in the
error; it is never used as an organization. Counterpart of `test_oauth_ambiguous_org.py`.

    uv run pytest tests/integration/test_global_key_organization.py
"""

import pytest

from confluent_sql import AmbiguousOrganizationError, connect
from confluent_sql.connection import Connection

BOGUS_EXTRA_ORGANIZATION: tuple[str, str | None] = (
    "00000000-0000-0000-0000-00000000000b",
    "Bogus Extra Organization",
)
"""Appended to a single-organization account's real listing so it looks ambiguous."""


@pytest.fixture
def recorded_lookups(monkeypatch) -> list[list[tuple[str, str | None]]]:
    """Wrap the real organizations lookup, recording each real answer (one list per call)."""
    lookups: list[list[tuple[str, str | None]]] = []
    real_lookup = Connection._lookup_organizations

    def recording_lookup(self: Connection) -> list[tuple[str, str | None]]:
        organizations = real_lookup(self)
        lookups.append(organizations)
        return organizations

    monkeypatch.setattr(Connection, "_lookup_organizations", recording_lookup)
    return lookups


@pytest.mark.integration
def test_omitted_organization_is_listed_during_connect(
    global_key_base_connect_args: dict[str, str],
    recorded_lookups: list[list[tuple[str, str | None]]],
):
    try:
        conn = connect(**global_key_base_connect_args)  # type: ignore
    except AmbiguousOrganizationError as exc:
        # A genuinely multi-organization account: the error itself proves the eager listing.
        assert len(recorded_lookups) == 1
        assert exc.organization_ids == [org_id for org_id, _ in recorded_lookups[0]]
        return

    try:
        assert len(recorded_lookups) == 1, "connect() should have listed organizations itself"
        assert [conn.organization_id] == [org_id for org_id, _ in recorded_lookups[0]]
        assert len(recorded_lookups) == 1, "reading organization_id must not list again"
    finally:
        conn.close()


@pytest.mark.integration_with_mocks
def test_ambiguous_organization_is_reported_then_an_explicit_one_connects(
    global_key_base_connect_args: dict[str, str], monkeypatch
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
        connect(**global_key_base_connect_args)  # type: ignore

    assert real_organizations, "the real organizations route returned nothing"
    candidate_ids = caught.value.organization_ids
    assert len(candidate_ids) >= 2
    for organization_id, _ in real_organizations:
        assert organization_id in candidate_ids
        assert organization_id in str(caught.value)
    assert "organization_id" in str(caught.value)

    # Back to the real lookup, then do what the error says: name one of the real candidates.
    # Naming it skips the listing entirely, so this also works for a multi-org account.
    monkeypatch.undo()
    chosen_id = real_organizations[0][0]
    conn = connect(organization_id=chosen_id, **global_key_base_connect_args)  # type: ignore
    try:
        assert conn.organization_id == chosen_id
    finally:
        conn.close()
