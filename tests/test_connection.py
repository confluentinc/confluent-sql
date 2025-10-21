"""
Integration test for the confluent-sql DB-API v2 driver.

This test makes a real API call to Confluent Cloud Flink environment.
Credentials must be provided via environment variables:
- FLINK_API_KEY
- FLINK_API_SECRET
- ENV_ID
- ORG_ID
- COMPUTE_POOL_ID
- CLOUD_REGION
- CLOUD_PROVIDER
"""

import os
import pytest
from confluent_sql import (
    connect,
    InterfaceError,
    DatabaseError,
    ProgrammingError,
    OperationalError,
)


def test_confluent_sql_connection():
    """Test connection to Confluent Cloud Flink SQL service."""
    # Get connection parameters from environment variables
    flink_api_key = os.getenv("FLINK_API_KEY")
    flink_api_secret = os.getenv("FLINK_API_SECRET")
    environment = os.getenv("ENV_ID")
    organization_id = os.getenv("ORG_ID")
    compute_pool_id = os.getenv("COMPUTE_POOL_ID")
    cloud_provider = os.getenv("CLOUD_PROVIDER", "aws")
    cloud_region = os.getenv("CLOUD_REGION", "us-east-2")

    # Optional credentials
    api_key = os.getenv("CONFLUENT_API_KEY")
    api_secret = os.getenv("CONFLUENT_API_SECRET")

    # Skip test if required credentials are not available
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

    print(f"flink_api_key: {flink_api_key}")
    print(f"environment: {environment}")
    print(f"organization_id: {organization_id}")
    print(f"compute_pool_id: {compute_pool_id}")
    print(f"cloud_region: {cloud_region}")
    print(f"cloud_provider: {cloud_provider}")

    # Create connection
    connection = connect(
        flink_api_key=flink_api_key,
        flink_api_secret=flink_api_secret,
        environment=environment,
        organization_id=organization_id,
        compute_pool_id=compute_pool_id,
        cloud_region=cloud_region,
        cloud_provider=cloud_provider,
        api_key=api_key,
        api_secret=api_secret,
    )

    try:
        # Test cursor creation
        cursor = connection.cursor()
        assert cursor is not None

        # Test simple query execution
        cursor.execute("SELECT 1 as test_value")

        # Verify cursor state after execution
        assert cursor._statement_id is not None
        assert cursor._statement_status in ["COMPLETED", "FINISHED", "RUNNING"]

        # Fetch results
        results = cursor.fetchall()
        assert isinstance(results, list)
        assert len(results) > 0

        # Verify result format
        for row in results:
            assert isinstance(row, tuple)
            assert len(row) >= 2  # operation + test_value
            assert row[0] in ["+I", "-D", "-U", "+U"]  # Valid changelog operation

        # Clean up
        cursor.close()

    finally:
        connection.close()


if __name__ == "__main__":
    pytest.main([__file__])
