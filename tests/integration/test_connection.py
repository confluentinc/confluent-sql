"""
Integration test for the confluent-sql DB-API v2 driver.

This test makes a real API call to Confluent Cloud Flink environment.
Credentials must be provided via environment variables.
"""

import os
import pytest

import confluent_sql
from confluent_sql.statement import Phase


@pytest.mark.skipif(
    os.getenv("CONFLUENT_FLINK_API_KEY") is None
    or os.getenv("CONFLUENT_FLINK_API_SECRET") is None
    or os.getenv("CONFLUENT_ENV_ID") is None
    or os.getenv("CONFLUENT_ORG_ID") is None
    or os.getenv("CONFLUENT_COMPUTE_POOL_ID") is None
    or os.getenv("CONFLUENT_CLOUD_PROVIDER") is None
    or os.getenv("CONFLUENT_CLOUD_REGION") is None,
    reason="Missing confluent environment variables",
)
def test_confluent_sql_connection():
    """Test connection to Confluent Cloud Flink SQL service."""
    flink_api_key = os.getenv("CONFLUENT_FLINK_API_KEY")
    flink_api_secret = os.getenv("CONFLUENT_FLINK_API_SECRET")
    environment_id = os.getenv("CONFLUENT_ENV_ID")
    organization_id = os.getenv("CONFLUENT_ORG_ID")
    compute_pool_id = os.getenv("CONFLUENT_COMPUTE_POOL_ID")
    cloud_provider = os.getenv("CONFLUENT_CLOUD_PROVIDER")
    cloud_region = os.getenv("CONFLUENT_CLOUD_REGION")
    connection = confluent_sql.connect(
        flink_api_key=flink_api_key,
        flink_api_secret=flink_api_secret,
        environment=environment_id,
        organization_id=organization_id,
        compute_pool_id=compute_pool_id,
        cloud_region=cloud_region,
        cloud_provider=cloud_provider,
    )
    # Test cursor creation
    cursor = connection.cursor(with_schema=True)
    assert cursor is not None

    # Check that we can list the catalogs (environments) available
    cursor.execute("SHOW CATALOGS")
    result = cursor.fetchall()
    # We should always have the catalog that corresponds to the environment
    # of the connection. SHOW CATALOGS returns a list of rows with name and id.
    # We can check that the `environment_id` passed to the `connect` function is present:
    catalog_ids = [res["row"]["Catalog ID"] for res in result]
    assert environment_id in catalog_ids
