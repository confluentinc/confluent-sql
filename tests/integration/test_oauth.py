"""Integration tests for OAuth-authenticated (interactive human login) connections.

These tests pop the system browser for a one-time Confluent Cloud sign-in, which a human must
complete, so they are opt-in: they skip unless RUN_OAUTH_TESTS is set. They also need
CONFLUENT_ENV_ID, CONFLUENT_CLOUD_PROVIDER, and CONFLUENT_CLOUD_REGION; the routes that need a
Kafka cluster id (CMK, Tableflow, Connectors) also need CONFLUENT_TEST_DBNAME. Only production is
targeted.

Beyond the query round trip, each route family gets one minimal, read-only, side-effect-free
probe proving the OAuth control-plane token is accepted. Not-found probes assert the specific
*NotFoundError subclass: a 401/403 raises a plain OperationalError and so fails the test.

The login is shared by every test in this module (module-scoped fixture), so there is a single
browser bounce per run. Run with `-n0` to keep the login easy to watch:

    RUN_OAUTH_TESTS=1 uv run pytest -n0 tests/integration/test_oauth.py
"""

import os
import uuid
from collections.abc import Generator
from typing import Any

import pytest

from confluent_sql import (
    Connection,
    ConnectorNotFoundError,
    TableflowTopicNotFoundError,
    connect,
    oauth_discard_login,
)


@pytest.fixture(scope="module")
def oauth_connect_args(oauth_base_connect_args: dict[str, str]) -> dict[str, str]:
    """Return the connection arguments for an OAuth-authenticated connection, adding the
    database (needed to resolve the Kafka cluster id) to the shared base arguments."""
    if not os.getenv("CONFLUENT_TEST_DBNAME"):
        pytest.skip(
            "Missing required environment variable(s) for OAuth tests: CONFLUENT_TEST_DBNAME"
        )

    return {**oauth_base_connect_args, "database": os.environ["CONFLUENT_TEST_DBNAME"]}


@pytest.fixture(scope="module")
def oauth_connection(oauth_connect_args: dict[str, str]) -> Generator[Connection, Any, None]:
    """A real Connection authenticated via interactive OAuth login against production.

    Skips unless the shared OAuth gating passes (see `oauth_base_connect_args`) and
    CONFLUENT_TEST_DBNAME is set. organization_id is not supplied; it is discovered from the
    login session.
    """
    conn = connect(**oauth_connect_args)  # type: ignore

    yield conn

    conn.close()
    oauth_discard_login()


@pytest.mark.integration
class TestOauth:
    """Client functionality exercised over an OAuth-authenticated connection."""

    def test_oauth_infers_organization_id(
        self, oauth_connection: Connection, oauth_connect_args: dict[str, str]
    ):
        """An OAuth-authenticated connection can infer its organization_id."""
        # Not present in the connection arguments
        assert oauth_connect_args.get("organization_id") is None
        # But determined after oauth happens.
        assert (
            oauth_connection.organization_id
            and isinstance(oauth_connection.organization_id, str)
            and len(oauth_connection.organization_id) > 0
        )

    def test_query_round_trip(self, oauth_connection: Connection):
        """A simple snapshot query submitted over an OAuth login returns its row."""
        with oauth_connection.closing_cursor() as cursor:
            cursor.execute("SELECT 1 AS test_value_1, 2 AS test_value_2, 3 AS test_value_3")
            assert cursor.fetchall() == [(1, 2, 3)]

    def test_cmk_cluster_resolution(self, oauth_connection: Connection):
        """The CMK cluster lookup (Tableflow/CMK control-plane client) resolves the database."""
        assert oauth_connection.resolve_kafka_cluster_id().startswith("lkc-")

    def test_tableflow_route_authenticated(self, oauth_connection: Connection):
        """A Tableflow read of a nonexistent topic is a 404, not an auth failure."""
        with pytest.raises(TableflowTopicNotFoundError):
            oauth_connection.get_tableflow(f"no_such_table_{uuid.uuid4().hex}")

    def test_connector_route_authenticated(self, oauth_connection: Connection):
        """A Connect read of a nonexistent connector is a 404, not an auth failure."""
        with pytest.raises(ConnectorNotFoundError):
            oauth_connection.get_connector(f"no-such-connector-{uuid.uuid4().hex}")

    def test_artifact_route_authenticated(self, oauth_connection: Connection):
        """Listing artifacts (cloud/artifact control-plane client) succeeds, empty or not."""
        assert isinstance(oauth_connection.get_artifacts_api().list(), list)
