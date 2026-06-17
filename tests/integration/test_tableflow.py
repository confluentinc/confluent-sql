"""Integration test for the Tableflow lifecycle: enable -> insert -> disable, end to end.

Runs only when a Tableflow-capable configuration is present in the environment. "Green" here is a
health check (phase RUNNING with no failing formats), not a materialization read-back: confirming
rows landed in the Iceberg/Delta table would need an external catalog + query engine this driver
does not ship.
"""

from __future__ import annotations

import os
from collections.abc import Generator
from datetime import datetime

import pytest

import confluent_sql
from confluent_sql import ManagedStorage, TableflowPhase, TableFormat
from confluent_sql.connection import Connection


def _tableflow_connection_from_env() -> Connection:
    """Build a Tableflow-capable Connection from env, or skip if the configuration is incomplete.

    Beyond the standard Flink/environment vars, Tableflow needs a control-plane credential (a global
    key covers it, else CONFLUENT_TABLEFLOW_API_KEY/SECRET) and a way to know the cluster id (a
    global key resolves it via CMK, else CONFLUENT_DATABASE_KAFKA_CLUSTER_ID supplies it directly).
    A compute pool is required to run the DDL/DML in the arc.
    """
    global_api_key = os.getenv("CONFLUENT_GLOBAL_API_KEY", "")
    global_api_secret = os.getenv("CONFLUENT_GLOBAL_API_SECRET", "")
    flink_api_key = os.getenv("CONFLUENT_FLINK_API_KEY", "")
    flink_api_secret = os.getenv("CONFLUENT_FLINK_API_SECRET", "")
    tableflow_api_key = os.getenv("CONFLUENT_TABLEFLOW_API_KEY", "")
    tableflow_api_secret = os.getenv("CONFLUENT_TABLEFLOW_API_SECRET", "")
    environment_id = os.getenv("CONFLUENT_ENV_ID", "")
    organization_id = os.getenv("CONFLUENT_ORG_ID", "")
    cloud_provider = os.getenv("CONFLUENT_CLOUD_PROVIDER", "")
    cloud_region = os.getenv("CONFLUENT_CLOUD_REGION", "")
    database = os.getenv("CONFLUENT_TEST_DBNAME", "")
    database_kafka_cluster_id = os.getenv("CONFLUENT_DATABASE_KAFKA_CLUSTER_ID", "")
    compute_pool_id = os.getenv("CONFLUENT_COMPUTE_POOL_ID", "")

    has_global = bool(global_api_key) and bool(global_api_secret)
    has_tableflow = bool(tableflow_api_key) and bool(tableflow_api_secret)
    # Control-plane credential: a global key, or a dedicated tableflow pair.
    controlplane_ok = has_global or has_tableflow
    # Cluster id obtainable: a global key (CMK lookup), or a directly-supplied id.
    cluster_ok = has_global or bool(database_kafka_cluster_id)
    base_ok = all(
        [environment_id, organization_id, cloud_provider, cloud_region, database, compute_pool_id]
    )
    if not (controlplane_ok and cluster_ok and base_ok):
        pytest.skip("Missing environment variables for a Tableflow-capable integration connection")

    return confluent_sql.connect(
        global_api_key=global_api_key,
        global_api_secret=global_api_secret,
        flink_api_key=flink_api_key,
        flink_api_secret=flink_api_secret,
        tableflow_api_key=tableflow_api_key,
        tableflow_api_secret=tableflow_api_secret,
        environment_id=environment_id,
        organization_id=organization_id,
        compute_pool_id=compute_pool_id,
        cloud_provider=cloud_provider,
        cloud_region=cloud_region,
        database=database,
        database_kafka_cluster_id=database_kafka_cluster_id or None,
    )


@pytest.fixture()
def tableflow_connection(load_env_file) -> Generator[Connection, None, None]:
    """A Tableflow-capable connection for the arc test, closed at the end of the test."""
    conn = _tableflow_connection_from_env()
    yield conn
    conn.close()


@pytest.fixture()
def tableflow_table_name(
    tableflow_connection: Connection, filtered_username: str
) -> Generator[str, None, None]:
    """A unique table name, dropped on teardown via the still-open tableflow_connection.

    Depends on tableflow_connection so it tears down first (reverse setup order), keeping the
    connection alive for the DROP.
    """
    name = f"confluentsql_pytest_{filtered_username}_tableflow_{datetime.now():%Y%m%d_%H%M%S}"
    yield name
    with tableflow_connection.closing_cursor() as cursor:
        try:
            cursor.execute(f"DROP TABLE IF EXISTS `{name}`")
        except Exception as e:
            print(f"Error dropping table {name} during teardown: {e}")


@pytest.mark.integration
@pytest.mark.slow
class TestTableflowLifecycle:
    """End-to-end arc: create table -> enable -> insert -> assert healthy -> disable -> drop."""

    def test_enable_insert_disable_arc(
        self,
        tableflow_connection: Connection,
        tableflow_table_name: str,
    ) -> None:
        conn = tableflow_connection
        table = tableflow_table_name

        # Create the Flink table (and its backing Kafka topic) Tableflow will materialize.
        conn.execute_snapshot_ddl(
            f"CREATE TABLE `{table}` (id INT, description STRING)"
        )

        tableflow_enabled = False
        try:
            topic = conn.enable_tableflow(
                table,
                tableflow_formats=TableFormat.ICEBERG,
                storage=ManagedStorage(),
                wait_for_running=True,
            )
            tableflow_enabled = True
            assert topic.phase is TableflowPhase.RUNNING

            # Push data through, then re-check health: Tableflow stayed RUNNING with no failing
            # formats *through* the insert. (We can't verify rows materialized -- see docstring.)
            # INSERT is DML, not DDL -- run it through a regular cursor, not execute_snapshot_ddl.
            with conn.closing_cursor() as cursor:
                cursor.execute(f"INSERT INTO `{table}` VALUES (1, 'one'), (2, 'two')")
            refreshed = conn.get_tableflow(table)
            assert refreshed.phase is TableflowPhase.RUNNING
            assert refreshed.status.failing_table_formats == []
        finally:
            # Gate the DROP on confirmed Tableflow removal: dropping the table drops its backing
            # topic, which would otherwise race an active materialization.
            if tableflow_enabled:
                conn.disable_tableflow(table, wait_for_removal=True)
                with pytest.raises(confluent_sql.TableflowTopicNotFoundError):
                    conn.get_tableflow(table)
            # The table itself is dropped by the tableflow_table_name fixture teardown.
