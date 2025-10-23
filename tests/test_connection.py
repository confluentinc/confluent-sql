"""
Integration test for the confluent-sql DB-API v2 driver.

This test makes a real API call to Confluent Cloud Flink environment.
Credentials must be provided via environment variables.
"""

from unittest.mock import patch, Mock

import httpx
import pytest
from confluent_sql.exceptions import OperationalError
from confluent_sql.cursor import Phase


def raise_not_found():
    mock_response = Mock()
    mock_response.status_code = 404
    raise httpx.HTTPStatusError("Statement not found", request=Mock(), response=mock_response)


def test_errors(mock_connection_manager):
    with mock_connection_manager() as connection:
        with patch.object(connection._client, "request") as request_mock:
            response_mock = Mock()
            response_mock.raise_for_status = raise_not_found
            request_mock.return_value = response_mock
            with pytest.raises(OperationalError, match="Error sending request 404"):
                connection.get_statement_status("test-name")


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
        assert len(results) > 0

        # Verify result format
        for row in results:
            assert isinstance(row, tuple)
            assert row[0] == "+I"
            assert row[1] == "1"


if __name__ == "__main__":
    pytest.main([__file__])
