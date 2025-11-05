import httpx
import pytest
from unittest.mock import Mock
from confluent_sql.exceptions import OperationalError


def raise_not_found():
    """Mock function that raises an HTTPStatusError with a mocked 404 response."""
    mock_response = Mock()
    mock_response.status_code = 404
    raise httpx.HTTPStatusError(
        "Statement not found", request=Mock(), response=mock_response
    )


def test_connection_error(mock_connection, mocker):
    """Test that we get meaningful error message when a response returns an error."""
    request_mock = mocker.patch.object(mock_connection._client, "request")
    response_mock = Mock()
    response_mock.raise_for_status = raise_not_found
    request_mock.return_value = response_mock
    with pytest.raises(OperationalError, match="Error sending request 404"):
        mock_connection._get_statement_status("test-name")
