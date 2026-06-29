"""Integration test for the connector lifecycle: create -> wait RUNNING -> delete, end to end.

Runs only when a connector-capable configuration is present in the environment. Uses a managed
**Datagen source** connector -- free, self-contained, and needing no external system to stand up.
"Green" here is a health check (state RUNNING), not a data read-back. The connector produces to its
own Kafka topic, which connector deletion does not remove; teardown makes a best-effort attempt to
drop it via the Flink table abstraction.
"""

from __future__ import annotations

import contextlib
import os
from collections.abc import Generator
from datetime import datetime

import pytest

import confluent_sql
from confluent_sql import ConnectorState
from confluent_sql.connection import Connection


def _connector_connection_from_env() -> Connection:
    """Build a connector-capable Connection from env, or skip if the configuration is incomplete.

    Beyond the standard Flink/environment vars, connectors need a control-plane credential (a global
    key covers it, else CONFLUENT_CONNECT_API_KEY/SECRET) and a way to know the cluster id (a global
    key resolves it via CMK, else CONFLUENT_DATABASE_KAFKA_CLUSTER_ID supplies it directly). The
    Datagen connector additionally needs its own Kafka API key pair to produce to the topic.
    """
    global_api_key = os.getenv("CONFLUENT_GLOBAL_API_KEY", "")
    global_api_secret = os.getenv("CONFLUENT_GLOBAL_API_SECRET", "")
    flink_api_key = os.getenv("CONFLUENT_FLINK_API_KEY", "")
    flink_api_secret = os.getenv("CONFLUENT_FLINK_API_SECRET", "")
    connect_api_key = os.getenv("CONFLUENT_CONNECT_API_KEY", "")
    connect_api_secret = os.getenv("CONFLUENT_CONNECT_API_SECRET", "")
    environment_id = os.getenv("CONFLUENT_ENV_ID", "")
    organization_id = os.getenv("CONFLUENT_ORG_ID", "")
    cloud_provider = os.getenv("CONFLUENT_CLOUD_PROVIDER", "")
    cloud_region = os.getenv("CONFLUENT_CLOUD_REGION", "")
    database = os.getenv("CONFLUENT_TEST_DBNAME", "")
    database_kafka_cluster_id = os.getenv("CONFLUENT_DATABASE_KAFKA_CLUSTER_ID", "")
    compute_pool_id = os.getenv("CONFLUENT_COMPUTE_POOL_ID", "")

    has_global = bool(global_api_key) and bool(global_api_secret)
    has_flink = bool(flink_api_key) and bool(flink_api_secret)
    has_connect = bool(connect_api_key) and bool(connect_api_secret)
    # A half-supplied pair (key without secret, or vice versa) would make connect() raise
    # InterfaceError; treat that as "not configured" and skip rather than erroring at setup.
    no_half_pairs = not (
        bool(global_api_key) != bool(global_api_secret)
        or bool(flink_api_key) != bool(flink_api_secret)
        or bool(connect_api_key) != bool(connect_api_secret)
    )
    # The connection's own (Flink) client needs a global or Flink-region pair.
    flink_auth_ok = has_global or has_flink
    # Control-plane credential: a global key, or a dedicated connect pair.
    controlplane_ok = has_global or has_connect
    # Cluster id obtainable: a global key (CMK lookup), or a directly-supplied id.
    cluster_ok = has_global or bool(database_kafka_cluster_id)
    base_ok = all([environment_id, organization_id, cloud_provider, cloud_region, database])
    if not (no_half_pairs and flink_auth_ok and controlplane_ok and cluster_ok and base_ok):
        pytest.skip("Missing environment variables for a connector-capable integration connection")

    return confluent_sql.connect(
        global_api_key=global_api_key,
        global_api_secret=global_api_secret,
        flink_api_key=flink_api_key,
        flink_api_secret=flink_api_secret,
        connect_api_key=connect_api_key,
        connect_api_secret=connect_api_secret,
        environment_id=environment_id,
        organization_id=organization_id,
        compute_pool_id=compute_pool_id,
        cloud_provider=cloud_provider,
        cloud_region=cloud_region,
        database=database,
        database_kafka_cluster_id=database_kafka_cluster_id or None,
    )


def _connector_kafka_credentials() -> tuple[str, str]:
    """The Datagen connector's own Kafka API key pair, or skip if absent."""
    key = os.getenv("CONFLUENT_CONNECTOR_KAFKA_API_KEY", "")
    secret = os.getenv("CONFLUENT_CONNECTOR_KAFKA_API_SECRET", "")
    if not (key and secret):
        pytest.skip(
            "Missing CONFLUENT_CONNECTOR_KAFKA_API_KEY/SECRET for the Datagen connector's producer"
        )
    return key, secret


@pytest.fixture()
def connector_connection(load_env_file) -> Generator[Connection, None, None]:
    """A connector-capable connection for the arc test, closed at the end of the test."""
    conn = _connector_connection_from_env()
    yield conn
    conn.close()


@pytest.fixture()
def connector_names(
    connector_connection: Connection, filtered_username: str
) -> Generator[tuple[str, str], None, None]:
    """A unique (connector_name, topic_name) pair; the topic is best-effort dropped on teardown.

    Depends on connector_connection so it tears down first (reverse setup order), keeping the
    connection alive for the topic cleanup.
    """
    stamp = f"{filtered_username}_{datetime.now():%Y%m%d_%H%M%S}"
    connector_name = f"confluentsql_pytest_connector_{stamp}"
    topic_name = f"confluentsql_pytest_topic_{stamp}"
    yield connector_name, topic_name
    # Connector deletion leaves its target topic behind; drop it via the Flink table abstraction.
    with connector_connection.closing_cursor() as cursor:
        try:
            cursor.execute(f"DROP TABLE IF EXISTS `{topic_name}`")
        except Exception as e:
            print(f"Error dropping topic {topic_name} during teardown: {e}")


@pytest.mark.integration
@pytest.mark.slow
class TestConnectorLifecycle:
    """End-to-end arc: create Datagen source -> wait RUNNING -> assert healthy -> delete -> gone."""

    def test_create_check_delete_arc(
        self,
        connector_connection: Connection,
        connector_names: tuple[str, str],
    ) -> None:
        conn = connector_connection
        connector_name, topic_name = connector_names
        kafka_api_key, kafka_api_secret = _connector_kafka_credentials()

        config = {
            "connector.class": "DatagenSource",
            "kafka.api.key": kafka_api_key,
            "kafka.api.secret": kafka_api_secret,
            "kafka.topic": topic_name,
            "output.data.format": "JSON",
            "quickstart": "ORDERS",
            "tasks.max": "1",
        }

        try:
            connector = conn.create_connector(
                connector_name, config=config, wait_for_running=True, timeout=600
            )
            assert connector.state is ConnectorState.RUNNING
            assert connector.spec.connector_class == "DatagenSource"

            # Re-read independently: the config-read + /status-read merge stays healthy.
            refreshed = conn.get_connector(connector_name)
            assert refreshed.state is ConnectorState.RUNNING

            # Delete and confirm it 404s afterward -- the teardown half of the arc under test.
            conn.delete_connector(connector_name, wait_for_removal=True, timeout=600)
            with pytest.raises(confluent_sql.ConnectorNotFoundError):
                conn.get_connector(connector_name)
        finally:
            # Best-effort cleanup. If create_connector's RUNNING wait raised (FAILED/timeout) the
            # connector still exists server-side, and the happy-path delete above never ran -- so
            # tear it down here to avoid orphaning it. A no-op (404) when that delete already ran.
            with contextlib.suppress(confluent_sql.ConnectorNotFoundError):
                conn.delete_connector(connector_name, wait_for_removal=False)
