from unittest.mock import Mock

import httpx
import pytest

from confluent_sql import InterfaceError, OperationalError


def raise_not_found():
    """Mock function that raises an HTTPStatusError with a mocked 404 response."""
    mock_response = Mock()
    mock_response.status_code = 404
    raise httpx.HTTPStatusError(
        "Statement not found", request=Mock(), response=mock_response
    )


@pytest.mark.unit
def test_connection_error(connection_factory, mocker):
    """Test that we get meaningful error message when a response returns an error."""
    connection = connection_factory()
    request_mock = mocker.patch.object(connection._client, "request")
    response_mock = Mock()
    response_mock.raise_for_status = raise_not_found
    request_mock.return_value = response_mock
    with pytest.raises(OperationalError, match="Error sending request 404"):
        connection._get_statement("test-name")


@pytest.mark.unit
class TestConnectionClosedThrows:
    """Tests for operations on a closed connection."""

    def test_cursor_when_closed_raises(self, connection_factory):
        """Test that asking for a cursor when the connection is closed raises an error."""
        connection = connection_factory()
        connection.close()
        with pytest.raises(InterfaceError, match="Connection is closed"):
            connection.cursor()

    def making_requests_when_closed_raises(self, connection_factory):
        """Test that making requests when the connection is closed raises an error."""
        connection = connection_factory()
        connection.close()
        with pytest.raises(InterfaceError, match="Connection is closed"):
            connection._get_statement("test-name")


@pytest.mark.unit
class TestConnectChecks:
    """Tests for connection checks when creating a connection."""

    empty_values = [None, ""]

    @pytest.mark.parametrize("value", empty_values)
    def test_requires_environment(self, connection_factory, value):
        """Test that creating a connection without an environment raises an error."""
        with pytest.raises(InterfaceError, match="Environment ID is required"):
            connection_factory(environment=value)

    @pytest.mark.parametrize("value", empty_values)
    def test_requires_compute_pool_id(self, connection_factory, value):
        """Test that creating a connection without a compute pool ID raises an error."""
        with pytest.raises(InterfaceError, match="Compute pool ID is required"):
            connection_factory(compute_pool_id=value)

    @pytest.mark.parametrize("value", empty_values)
    def test_requires_organization_id(self, connection_factory, value):
        """Test that creating a connection without an organization ID raises an error."""
        with pytest.raises(InterfaceError, match="Organization ID is required"):
            connection_factory(organization_id=value)

    @pytest.mark.parametrize("value", empty_values)
    def test_requires_cloud_provider(self, connection_factory, value):
        """Test that creating a connection without a cloud provider raises an error."""
        with pytest.raises(InterfaceError, match="Cloud provider is required"):
            connection_factory(cloud_provider=value)

    @pytest.mark.parametrize("value", empty_values)
    def test_requires_cloud_region(self, connection_factory, value):
        """Test that creating a connection without a cloud region raises an error."""
        with pytest.raises(InterfaceError, match="Cloud region is required"):
            connection_factory(cloud_region=value)

    @pytest.mark.parametrize("value", empty_values)
    def test_requires_flink_api_key(self, connection_factory, value):
        """Test that creating a connection without a Flink API key raises an error."""
        with pytest.raises(
            InterfaceError, match="Flink API key and secret are required"
        ):
            connection_factory(flink_api_key=value)

    @pytest.mark.parametrize("value", empty_values)
    def test_requires_flink_api_secret(self, connection_factory, value):
        """Test that creating a connection without a Flink API secret raises an error."""
        with pytest.raises(
            InterfaceError, match="Flink API key and secret are required"
        ):
            connection_factory(flink_api_secret=value)
