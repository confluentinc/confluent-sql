"""Example of enabling and updating Tableflow on Google Cloud Storage (GCS).

This mirrors ``tableflow_lifecycle_example.py`` but targets a customer-owned GCS bucket via the
new ``GcsStorage`` backend, and additionally shows the ``update_tableflow`` in-place path and a
non-blocking enable. Tableflow materializes the Kafka topic backing a Flink table into an
Iceberg/Delta table; here the underlying files land in your own GCS bucket rather than a
Confluent-managed one.

GCS bring-your-own-bucket needs a provider integration (``cspi-...``) already set up for the
environment, granting Confluent write access to the bucket -- ``provider_integration_id`` below.
``bucket_region`` and ``table_path`` are server-assigned and read-only, so they aren't inputs to
``GcsStorage``; read them back off ``topic.spec.raw["storage"]`` after enabling if you need them.

Tableflow is a control-plane surface and isn't reachable under BYOIDC -- use an API-key connection
(a Global key, as here, or a narrower tableflow_api_key / tableflow_api_secret pair).

Nothing here is meant to be run as-is: it's a tour of the call shapes and the new imports. The
table (and its backing Kafka topic) must already exist; this only manages the Tableflow sink.
"""

import os

import confluent_sql
from confluent_sql import (
    GcsStorage,
    TableflowErrorHandlingLog,
    TableflowPhase,
    TableflowTopicConfig,
    TableFormat,
)

conn = confluent_sql.connect(
    global_api_key=os.environ["CONFLUENT_GLOBAL_API_KEY"],
    global_api_secret=os.environ["CONFLUENT_GLOBAL_API_SECRET"],
    environment_id=os.environ["CONFLUENT_ENV_ID"],
    organization_id=os.environ["CONFLUENT_ORG_ID"],
    cloud_provider="gcp",  # GCS storage lives in a GCP environment
    cloud_region=os.environ["CONFLUENT_CLOUD_REGION"],  # e.g. "us-central1"
    compute_pool_id=os.getenv("CONFLUENT_COMPUTE_POOL_ID"),  # optional; None -> default pool
    database=os.environ["CONFLUENT_DATABASE"],  # Kafka cluster name; resolved to lkc-... via CMK
)
table_name = os.getenv("CONFLUENT_TABLEFLOW_TABLE", "orders")

# A customer-owned GCS bucket. Only bucket_name + provider_integration_id are writable; the
# server assigns bucket_region and table_path (gs://...) and reports them read-only.
gcs = GcsStorage(
    bucket_name=os.getenv("CONFLUENT_GCS_BUCKET", "my-gcs-bucket"),
    provider_integration_id=os.environ["CONFLUENT_GCS_PROVIDER_INTEGRATION_ID"],  # cspi-...
)

try:
    # Enable Iceberg on the GCS bucket, with a topic-level config. wait_for_running=True (the
    # default) blocks until the topic reaches RUNNING, raising OperationalError if it goes FAILED.
    topic = conn.enable_tableflow(
        table_name,
        table_formats=TableFormat.ICEBERG,
        storage=gcs,
        config=TableflowTopicConfig(
            retention_ms=604_800_000,  # 7 days; int in, string-encoded int64 on the wire
            error_handling=TableflowErrorHandlingLog(target="orders_dlq"),  # bad records -> DLQ
        ),
    )
    print(f"enabled Tableflow on {table_name!r}: phase={topic.phase}")
    assert topic.phase is TableflowPhase.RUNNING
    # Read-only, server-assigned storage fields live on the raw spec, not the GcsStorage input.
    print(f"table_path: {topic.spec.raw['storage'].get('table_path')}")
    # The read side parses back into the same typed class, with the server-assigned read-only
    # fields dropped -- so it compares equal to the GcsStorage we sent.
    assert isinstance(topic.spec.storage, GcsStorage)
    assert topic.spec.storage == gcs

    # Add DELTA alongside the existing ICEBERG, in place -- no disable/re-enable cycle. Passing
    # only table_formats leaves config untouched; storage is immutable and has no update path.
    topic = conn.update_tableflow(
        table_name,
        table_formats={TableFormat.ICEBERG, TableFormat.DELTA},
    )
    print(f"updated formats: {topic.spec.table_formats}")

    # Bump retention without touching the format set. Each of config's own sub-fields is
    # independently "leave unchanged" when None, so this only rewrites retention_ms.
    conn.update_tableflow(
        table_name,
        config=TableflowTopicConfig(retention_ms=2_592_000_000),  # 30 days
    )

    # A non-blocking enable would look like this (shown, not run, since it's already enabled):
    #   conn.enable_tableflow(
    #       other_table,
    #       table_formats=TableFormat.ICEBERG,
    #       storage=gcs,
    #       wait_for_running=False,  # returns immediately with the topic in PENDING
    #   )

    conn.disable_tableflow(table_name)
    print(f"disabled Tableflow on {table_name!r}")
finally:
    conn.close()
