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

import pytest


def test_confluent_sql_connection(connection_manager):
    """Test connection to Confluent Cloud Flink SQL service."""
    with connection_manager() as connection:
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


if __name__ == "__main__":
    pytest.main([__file__])
