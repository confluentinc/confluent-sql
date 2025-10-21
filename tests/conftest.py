import os
from contextlib import contextmanager

import confluent_sql
import pytest


@pytest.fixture
def connection_manager():
    """Create a connection for testing."""
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
        pytest.skip("Missing required environment variables for integration test")

    @contextmanager
    def manager():
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
    return manager
