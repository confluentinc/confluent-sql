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

import confluent_sql
from confluent_sql import ConnectorNotFoundError, TableflowTopicNotFoundError
from confluent_sql.connection import Connection


@pytest.fixture(scope="module")
def oauth_connection(load_env_file) -> Generator[Connection, Any, None]:
    """A real Connection authenticated via interactive OAuth login against production.

    Skips unless RUN_OAUTH_TESTS is set and the environment/cloud/region/database vars are
    present (the database is needed to resolve the Kafka cluster id).
    organization_id is not supplied; it is discovered from the login session.
    """
    if not os.getenv("RUN_OAUTH_TESTS"):
        pytest.skip("RUN_OAUTH_TESTS not set; OAuth tests require a human browser login")

    required_vars = [
        "CONFLUENT_ENV_ID",
        "CONFLUENT_CLOUD_PROVIDER",
        "CONFLUENT_CLOUD_REGION",
        "CONFLUENT_TEST_DBNAME",
    ]
    if missing := [name for name in required_vars if not os.getenv(name)]:
        pytest.skip(f"Missing required environment variables for OAuth tests: {', '.join(missing)}")

    conn = confluent_sql.connect(
        auth="oauth",
        environment_id=os.environ["CONFLUENT_ENV_ID"],
        cloud_provider=os.environ["CONFLUENT_CLOUD_PROVIDER"],
        cloud_region=os.environ["CONFLUENT_CLOUD_REGION"],
        database=os.environ["CONFLUENT_TEST_DBNAME"],
    )

    yield conn

    conn.close()


@pytest.mark.integration
class TestOauth:
    """Client functionality exercised over an OAuth-authenticated connection."""

    def test_query_round_trip(self, oauth_connection: Connection):
        """A simple snapshot query submitted over an OAuth login returns its row."""
        assert oauth_connection.organization_id

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
        assert isinstance(oauth_connection.get_artifacts_api().list(page_size=1), list)
