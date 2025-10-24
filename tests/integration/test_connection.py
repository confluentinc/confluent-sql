"""
Integration test for the confluent-sql DB-API v2 driver.

This test makes a real API call to Confluent Cloud Flink environment.
Credentials must be provided via environment variables.
"""

from confluent_sql.statement import Phase


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
        assert cursor._statement.phase is Phase.COMPLETED

        # Fetch results
        results = cursor.fetchall()
        assert isinstance(results, list)
        assert len(results) == 1
        assert results == [("+I", ("1",))]
