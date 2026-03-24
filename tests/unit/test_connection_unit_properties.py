"""Unit tests for statement properties parameter support (Issue #68)."""

from unittest.mock import Mock

import pytest

from confluent_sql import InterfaceError, PropertiesDict, connect
from confluent_sql.connection import Connection
from confluent_sql.execution_mode import ExecutionMode


@pytest.fixture()
def invalid_credential_connection() -> Connection:
    """A connection with invalid credentials for testing."""
    return connect(
        environment="env-id",
        organization_id="org-id",
        compute_pool_id="cp-id",
        cloud_provider="aws",
        cloud_region="us-east-1",
        flink_api_key="key",
        flink_api_secret="secret",
    )


@pytest.mark.unit
class TestExecuteStatementProperties:
    """Tests for properties parameter in _execute_statement method."""

    def install_request_mock(self, connection: Connection, mocker):
        """Helper method to set up a request mock for testing _execute_statement."""
        mock_response = Mock()
        mock_response.json.return_value = {
            "name": "test-statement",
            "status": {"phase": "RUNNING"},
        }
        return mocker.patch.object(connection._client, "request", return_value=mock_response)

    def test_properties_none_uses_defaults(self, invalid_credential_connection, mocker):
        """Verify default properties are set when properties=None."""
        request_mock = self.install_request_mock(invalid_credential_connection, mocker)

        invalid_credential_connection._execute_statement(
            "SELECT 1",
            ExecutionMode.SNAPSHOT,
            properties=None,
        )

        payload = request_mock.call_args[1]["json"]
        assert "sql.current-catalog" in payload["spec"]["properties"]
        assert "sql.snapshot.mode" in payload["spec"]["properties"]

    def test_properties_empty_dict_uses_defaults(self, invalid_credential_connection, mocker):
        """Verify empty dict doesn't interfere with default properties."""
        request_mock = self.install_request_mock(invalid_credential_connection, mocker)

        invalid_credential_connection._execute_statement(
            "SELECT 1",
            ExecutionMode.SNAPSHOT,
            properties={},
        )

        payload = request_mock.call_args[1]["json"]
        assert "sql.current-catalog" in payload["spec"]["properties"]

    def test_properties_user_values_added(self, invalid_credential_connection, mocker):
        """Verify user properties are included in payload."""
        request_mock = self.install_request_mock(invalid_credential_connection, mocker)

        user_props = {"custom-prop": "custom-value"}
        invalid_credential_connection._execute_statement(
            "SELECT 1",
            ExecutionMode.SNAPSHOT,
            properties=user_props,
        )

        payload = request_mock.call_args[1]["json"]
        props = payload["spec"]["properties"]
        assert props["custom-prop"] == "custom-value"

    def test_properties_validates_dict_type(self, invalid_credential_connection):
        """Verify InterfaceError is raised if properties is not a dict."""
        with pytest.raises(InterfaceError, match="properties must be a dict"):
            invalid_credential_connection._execute_statement(
                "SELECT 1",
                ExecutionMode.SNAPSHOT,
                properties="not-a-dict",
            )

    def test_properties_validates_key_types(self, invalid_credential_connection):
        """Verify InterfaceError is raised if property keys are not strings."""
        with pytest.raises(InterfaceError, match="properties keys must be strings"):
            invalid_credential_connection._execute_statement(
                "SELECT 1",
                ExecutionMode.SNAPSHOT,
                properties={123: "value"},
            )

    @pytest.mark.parametrize(
        "invalid_value",
        [
            [1, 2, 3],  # list
            {"nested": "dict"},  # dict
            3.14,  # float
            None,  # None
        ],
    )
    def test_properties_validates_value_types(self, invalid_credential_connection, invalid_value):
        """Verify InterfaceError is raised for invalid value types."""
        with pytest.raises(InterfaceError, match="properties values must be str, int, or bool"):
            invalid_credential_connection._execute_statement(
                "SELECT 1",
                ExecutionMode.SNAPSHOT,
                properties={"key": invalid_value},
            )

    def test_properties_supports_all_valid_types(self, invalid_credential_connection, mocker):
        """Verify str, int, and bool values all work."""
        request_mock = self.install_request_mock(invalid_credential_connection, mocker)

        user_props = {
            "string-prop": "value",
            "int-prop": 42,
            "bool-prop": True,
        }
        invalid_credential_connection._execute_statement(
            "SELECT 1",
            ExecutionMode.SNAPSHOT,
            properties=user_props,
        )

        payload = request_mock.call_args[1]["json"]
        props = payload["spec"]["properties"]
        assert props["string-prop"] == "value"
        assert props["int-prop"] == 42
        assert props["bool-prop"] is True

    def test_properties_mixed_user_and_system(self, invalid_credential_connection, mocker):
        """Verify mixed user and system properties work correctly."""
        request_mock = self.install_request_mock(invalid_credential_connection, mocker)

        user_props = {
            "custom-prop-1": "value1",
            "custom-prop-2": 123,
        }

        invalid_credential_connection._execute_statement(
            "SELECT 1",
            ExecutionMode.SNAPSHOT,
            properties=user_props,
        )

        payload = request_mock.call_args[1]["json"]
        props = payload["spec"]["properties"]

        # User properties should be present
        assert props["custom-prop-1"] == "value1"
        assert props["custom-prop-2"] == 123
        # System properties should be present
        assert "sql.current-catalog" in props
        assert "sql.snapshot.mode" in props

    def test_properties_streaming_mode_no_snapshot(self, invalid_credential_connection, mocker):
        """Verify snapshot.mode is not set in streaming mode."""
        request_mock = self.install_request_mock(invalid_credential_connection, mocker)

        invalid_credential_connection._execute_statement(
            "SELECT 1",
            ExecutionMode.STREAMING_QUERY,
            properties=None,
        )

        payload = request_mock.call_args[1]["json"]
        props = payload["spec"]["properties"]

        # snapshot.mode should NOT be set for streaming mode
        assert "sql.snapshot.mode" not in props
        # But catalog should still be set
        assert "sql.current-catalog" in props

    @pytest.mark.parametrize(
        "reserved_property",
        [
            "sql.current-catalog",
            "sql.current-database",
            "sql.snapshot.mode",
        ],
    )
    def test_properties_rejects_reserved_system_properties(
        self, invalid_credential_connection, reserved_property
    ):
        """Verify InterfaceError is raised when user attempts to provide reserved system properties."""
        with pytest.raises(InterfaceError, match="is a reserved system property"):
            invalid_credential_connection._execute_statement(
                "SELECT 1",
                ExecutionMode.SNAPSHOT,
                properties={reserved_property: "user-value"},
            )
