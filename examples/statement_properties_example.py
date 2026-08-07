"""Example of setting Flink SQL statement properties: the typed `StatementProperties` dataclass,
the raw dict form it's built on top of, and the connection-level `local_time_zone` default.

`StatementProperties` covers the curated `sql.*` options with autocomplete-friendly fields,
validated at construction time; a raw `dict[str, str | int | bool]` remains available for options
it doesn't model yet (or via its own `extra=` escape hatch).
"""

import os
from datetime import timedelta

import confluent_sql
from confluent_sql import Property, ScanStartupMode, SnapshotWriteMode, StatementProperties

conn = confluent_sql.connect(
    flink_api_key=os.environ["CONFLUENT_FLINK_API_KEY"],
    flink_api_secret=os.environ["CONFLUENT_FLINK_API_SECRET"],
    environment_id=os.environ["CONFLUENT_ENV_ID"],
    organization_id=os.environ["CONFLUENT_ORG_ID"],
    cloud_provider=os.environ["CONFLUENT_CLOUD_PROVIDER"],
    cloud_region=os.environ["CONFLUENT_CLOUD_REGION"],
    compute_pool_id=os.getenv("CONFLUENT_COMPUTE_POOL_ID"),  # optional; None -> default pool
    # Seeds sql.local-time-zone for every statement this connection executes; a statement's own
    # properties (below) can still override it for itself.
    local_time_zone="America/Chicago",
)
cursor = conn.cursor()
try:
    # StatementProperties (recommended): typed fields, validated at construction. Only fields you
    # set are emitted, so unset ones never pin a server default.

    # This example is a bit silly, considering these particular properties would not affect this query,
    # but it illustrates the API.
    cursor.execute(
        "SELECT * FROM orders WHERE status = %s",
        ("pending",),
        properties=StatementProperties(
            state_ttl=timedelta(hours=1),  # rendered to the Flink duration string "3600 s"
            snapshot_write_mode=SnapshotWriteMode.FAST_WRITE,
            scan_startup_mode=ScanStartupMode.EARLIEST_OFFSET,
            # `extra` escape hatch for a property not yet a typed field; `Property` still gives
            # the key autocomplete and validation even though the value stays a raw string.
            extra={Property.SCAN_IDLE_TIMEOUT: "30 s"},
        ),
    )
    # See the concrete submitted properties, including the connection-level default for local_time_zone.
    print(f"statement.properties: {cursor.statement.properties}")

    # The equivalent raw dict form -- useful for a one-off option StatementProperties doesn't
    # model, without constructing the dataclass at all.
    cursor.execute(
        "SELECT * FROM orders WHERE status = %s",
        ("pending",),
        properties={"sql.state-ttl": "3600 s"},
    )

    # This statement didn't set local_time_zone itself, so it picks up the connection's default
    # (America/Chicago) set above.
    cursor.execute("SELECT CURRENT_TIMESTAMP")
    print(f"inherited local_time_zone: {cursor.statement.properties['sql.local-time-zone']}")

    # A statement can still override the connection-level default for itself.
    cursor.execute(
        "SELECT CURRENT_TIMESTAMP",
        properties=StatementProperties(local_time_zone="America/Los_Angeles"),
    )
    print(f"overridden local_time_zone: {cursor.statement.properties['sql.local-time-zone']}")
finally:
    cursor.close()
    conn.close()
