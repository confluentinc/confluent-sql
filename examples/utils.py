import logging
import os
from contextlib import contextmanager

import confluent_sql


def setup_logging(level=logging.INFO):
    """Setup logging so it only shows messages from "confluent_sql" at the given level."""
    logging.basicConfig(level=level)
    logging.getLogger().handlers[0].addFilter(logging.Filter("confluent_sql"))

@contextmanager
def connection_manager():
    flink_api_key = os.getenv("CONFLUENT_FLINK_API_KEY")
    flink_api_secret = os.getenv("CONFLUENT_FLINK_API_SECRET")
    environment = os.getenv("CONFLUENT_ENV_ID")
    organization_id = os.getenv("CONFLUENT_ORG_ID")
    compute_pool_id = os.getenv("CONFLUENT_COMPUTE_POOL_ID")
    cloud_provider = os.getenv("CONFLUENT_CLOUD_PROVIDER", "aws")
    cloud_region = os.getenv("CONFLUENT_CLOUD_REGION", "us-east-2")

    if not all(
        [
            flink_api_key,
            flink_api_secret,
            environment,
            organization_id,
            compute_pool_id,
            cloud_region,
            cloud_provider,
        ]
    ):
        raise ValueError("Missing env vars")
    conn = confluent_sql.connect(
        flink_api_key=flink_api_key,
        flink_api_secret=flink_api_secret,
        environment=environment,
        organization_id=organization_id,
        compute_pool_id=compute_pool_id,
        cloud_region=cloud_region,
        cloud_provider=cloud_provider,
    )
    try:
        yield conn
    finally:
        conn.close()
