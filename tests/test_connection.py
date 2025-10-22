"""
Integration test for the confluent-sql DB-API v2 driver.

This test makes a real API call to Confluent Cloud Flink environment.
Credentials must be provided via environment variables.
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
        assert cursor._statement is not None
        assert cursor._statement.phase == "COMPLETED"

        # Fetch results
        results = cursor.fetchall()
        assert isinstance(results, list)
        assert len(results) > 0

        # Verify result format
        for row in results:
            assert isinstance(row, tuple)
            assert row[0] == "1"


if __name__ == "__main__":
    pytest.main([__file__])
