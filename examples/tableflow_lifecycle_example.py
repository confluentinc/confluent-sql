"""Example of the Tableflow lifecycle: enable, inspect, and disable an Iceberg sink on the Kafka
topic backing a Flink table.

Tableflow materializes that topic into an Iceberg/Delta table, unlocking efficiency gains for
snapshot queries against it. Tableflow is a control-plane surface and is not reachable under
BYOIDC -- use an API-key connection (a Global key, as here, or a narrower tableflow_api_key /
tableflow_api_secret pair) instead of external_access_token / identity_pool_id.

The table (and its backing Kafka topic) must already exist; this example only manages the
Tableflow sink on top of it, not the table itself.
"""

import os

import confluent_sql
from confluent_sql import ManagedStorage, TableflowPhase, TableFormat

conn = confluent_sql.connect(
    global_api_key=os.environ["CONFLUENT_GLOBAL_API_KEY"],
    global_api_secret=os.environ["CONFLUENT_GLOBAL_API_SECRET"],
    environment_id=os.environ["CONFLUENT_ENV_ID"],
    organization_id=os.environ["CONFLUENT_ORG_ID"],
    cloud_provider=os.environ["CONFLUENT_CLOUD_PROVIDER"],
    cloud_region=os.environ["CONFLUENT_CLOUD_REGION"],
    compute_pool_id=os.getenv("CONFLUENT_COMPUTE_POOL_ID"),  # optional; None -> default pool
    database=os.environ["CONFLUENT_DATABASE"],  # Kafka cluster name; resolved to lkc-... via CMK
)
table_name = os.getenv("CONFLUENT_TABLEFLOW_TABLE", "orders")

try:
    # wait_for_running=True is the default: blocks until the topic reaches RUNNING, raising
    # OperationalError if it goes FAILED instead.
    topic = conn.enable_tableflow(
        table_name,
        tableflow_formats=TableFormat.ICEBERG,
        storage=ManagedStorage(),
    )
    print(f"enabled Tableflow on {table_name!r}: phase={topic.phase}")
    print(f"formats: {topic.spec.table_formats}")
    assert topic.phase is TableflowPhase.RUNNING

    topic = conn.get_tableflow(table_name)
    print(f"current state: phase={topic.phase}, formats={topic.spec.table_formats}")

    # wait_for_removal=True is the default: blocks until get_tableflow(...) 404s, so a following
    # DROP TABLE on the underlying topic is safe to issue right after this returns.
    conn.disable_tableflow(table_name)
    print(f"disabled Tableflow on {table_name!r}")
finally:
    conn.close()
