import copy
from collections import namedtuple
from dataclasses import dataclass
from typing import NamedTuple
from unittest.mock import Mock

import httpx
import pytest

from confluent_sql import InterfaceError, OperationalError, StatementNotFoundError
from confluent_sql.__version__ import VERSION
from confluent_sql.connection import Connection, RowTypeRegistry, connect
from confluent_sql.connection import logger as connection_module_logger
from confluent_sql.execution_mode import ExecutionMode
from confluent_sql.statement import HIDDEN_LABEL, LABEL_PREFIX, Statement
from tests.conftest import ConnectionFactory
from tests.unit.conftest import StatementResponseFactory


@pytest.fixture()
def invalid_credential_connection() -> Connection:
    """A fixture that returns a connection with invalid credentials set.

    This is useful for testing connection error handling.
    """
    return connect(
        environment="env-id",
        organization_id="org-id",
        compute_pool_id="cp-id",
        cloud_provider="aws",
        cloud_region="us-east-1",
        flink_api_key="invalid-key",
        flink_api_secret="invalid-secret",
    )


def _create_http_error_404():
    """Helper function that raises an HTTPStatusError with a mocked 404 response."""
    mock_response = Mock()
    mock_response.status_code = 404
    raise httpx.HTTPStatusError("Statement not found", request=Mock(), response=mock_response)


@pytest.mark.unit
class TestHTTPStatusErrorHandling:
    """Tests for enhanced error handling in Connection._request method."""

    def test_connection_error_with_valid_error_details(
        self, invalid_credential_connection: Connection, mocker
    ):
        """Test that 404 errors are converted to StatementNotFoundError.

        When the API returns a 404 error for a statement GET request,
        _get_statement should raise StatementNotFoundError with the statement name.
        """
        request_mock = mocker.patch.object(invalid_credential_connection._client, "request")

        # Create mock response with valid JSON error structure
        response_mock = Mock()
        response_mock.status_code = 404
        response_mock.json.return_value = {
            "errors": [
                {"detail": "Statement not found"},
                {"detail": "Please check statement name"},
            ]
        }

        def raise_http_error():
            raise httpx.HTTPStatusError("Not Found", request=Mock(), response=response_mock)

        response_mock.raise_for_status = raise_http_error
        request_mock.return_value = response_mock

        with pytest.raises(StatementNotFoundError) as exc_info:
            invalid_credential_connection._get_statement("test-name")

        # Verify exception has statement name
        assert exc_info.value.statement_name == "test-name"
        assert "test-name" in str(exc_info.value)

    def test_connection_error_with_invalid_json(
        self, invalid_credential_connection: Connection, mocker
    ):
        """Test error message fallback when response JSON is invalid or unparseable.

        When the API returns a non-JSON response or the JSON parsing fails,
        the exception message should include a "no more details" fallback.
        """
        request_mock = mocker.patch.object(invalid_credential_connection._client, "request")

        # Create mock response that raises when .json() is called
        response_mock = Mock()
        response_mock.status_code = 500
        response_mock.json.side_effect = ValueError("Invalid JSON")

        def raise_http_error():
            raise httpx.HTTPStatusError("Server Error", request=Mock(), response=response_mock)

        response_mock.raise_for_status = raise_http_error
        request_mock.return_value = response_mock

        with pytest.raises(
            OperationalError,
            match="error sending request '500' - no more details",
        ):
            invalid_credential_connection._get_statement("test-name")


@pytest.mark.unit
def test_connection_constructor_hates_negative_pause_millis(connection_factory: ConnectionFactory):
    """Test that the Connection constructor raises an error if given a negative
    statement_results_page_fetch_pause_millis."""
    with pytest.raises(InterfaceError, match="result_page_fetch_pause_millis must be non-negative"):
        connection_factory(
            environment="foo_id",
            compute_pool_id="1234",
            organization_id="4567",
            cloud_provider="aws",
            cloud_region="us-east-1",
            flink_api_key="valid-key",
            flink_api_secret="valid-secret",
            result_page_fetch_pause_millis=-100,
        )


@pytest.mark.unit
class TestConnectionDeleteStatementErrors:
    """Tests for delete_statement error handling."""

    def test_delete_statement_not_found(self, invalid_credential_connection: Connection, mocker):
        """Test that deleting a non-existent statement raises the appropriate error."""
        request_mock = mocker.patch.object(invalid_credential_connection._client, "request")
        response_mock = Mock()
        response_mock.raise_for_status = _create_http_error_404
        request_mock.return_value = response_mock

        # Will not raise since we ignore 404s in delete_statement
        invalid_credential_connection.delete_statement("non-existent-statement")

    def test_delete_statement_other_error(self, invalid_credential_connection: Connection, mocker):
        """Test that deleting a statement that raises an error other than 404
        raises OperationalError."""
        request_mock = mocker.patch.object(invalid_credential_connection._client, "request")
        response_mock = Mock()

        def raise_internal_server_error():
            mock_response = Mock()
            mock_response.status_code = 500
            raise httpx.HTTPStatusError(
                "Internal Server Error", request=Mock(), response=mock_response
            )

        response_mock.raise_for_status = raise_internal_server_error
        request_mock.return_value = response_mock

        with pytest.raises(OperationalError, match="Error deleting statement"):
            invalid_credential_connection.delete_statement("statement-with-error")


@pytest.mark.unit
class TestConnectionClosedThrows:
    """Tests for operations on a closed connection."""

    def test_cursor_when_closed_raises(self, invalid_credential_connection: Connection):
        """Test that asking for a cursor when the connection is closed raises an error."""
        invalid_credential_connection.close()
        with pytest.raises(InterfaceError, match="Connection is closed"):
            invalid_credential_connection.cursor()

    def test_making_requests_when_closed_raises(self, invalid_credential_connection: Connection):
        """Test that making requests when the connection is closed raises an error."""
        invalid_credential_connection.close()
        with pytest.raises(InterfaceError, match="Connection is closed"):
            invalid_credential_connection._get_statement("test-name")


@pytest.mark.unit
class TestGetNextPageToken:
    """Tests for the Connection._get_next_page_token method."""

    @pytest.mark.parametrize(
        "url,expected",
        [
            (None, None),
            ("https://api.confluent.cloud/statements?label_selector=foo&other_param=bar", None),
            (
                "https://api.confluent.cloud/statements?label_selector=foo&page_token=abc123&other_param=bar",
                "abc123",
            ),
        ],
    )
    def test_get_next_page_token(self, invalid_credential_connection: Connection, url, expected):
        """Test that _get_next_page_token correctly extracts the page token from a URL."""
        assert invalid_credential_connection._get_next_page_token(url) == expected


@pytest.mark.unit
class TestDeprecatedDbnameParameter:
    """Tests for deprecated 'dbname' parameter handling in connect()."""

    def test_dbname_alone_works_with_deprecation_warning(self):
        """Test that passing dbname (without database) works but raises deprecation warning.

        Verifies that:
        1. The connection is created successfully
        2. A DeprecationWarning is raised
        3. The database value is set from dbname
        4. The connection stores the database correctly
        """
        with pytest.warns(DeprecationWarning, match="'dbname' parameter is deprecated"):
            conn = connect(
                environment="env-id",
                compute_pool_id="cp-id",
                organization_id="org-id",
                cloud_provider="aws",
                cloud_region="us-east-1",
                flink_api_key="key",
                flink_api_secret="secret",
                dbname="my_database",  # Using deprecated parameter
                database=None,  # Not providing new parameter
            )

        # Verify the connection was created
        assert conn is not None
        # Verify the database was set from dbname
        assert conn._database == "my_database"

    def test_both_dbname_and_database_raises_interface_error(self):
        """Test that providing both dbname and database raises InterfaceError.

        Verifies that when both parameters are provided, an InterfaceError is raised
        with a clear error message indicating the conflict.
        """
        with pytest.raises(
            InterfaceError,
            match="Cannot specify both 'database' and deprecated 'dbname' parameters",
        ):
            connect(
                environment="env-id",
                compute_pool_id="cp-id",
                organization_id="org-id",
                cloud_provider="aws",
                cloud_region="us-east-1",
                flink_api_key="key",
                flink_api_secret="secret",
                dbname="old_database",  # Deprecated parameter
                database="new_database",  # New parameter - conflict!
            )


@pytest.mark.unit
class TestConnectionInit:
    """Tests for Connection.__init__ method."""

    @pytest.mark.parametrize(
        "cloud_provider,cloud_region,endpoint",
        [
            (None, None, None),
            ("aws", None, None),
            (None, "us-east-1", None),
            ("", "", None),
            ("aws", "", None),
            ("", "us-east-1", None),
        ],
    )
    def test_requires_endpoint_or_cloud_info(self, cloud_provider, cloud_region, endpoint):
        """Test that creating a connection without proper endpoint or cloud info raises an error."""
        with pytest.raises(
            InterfaceError,
            match="cloud_provider and cloud_region are required when endpoint is not provided",
        ):
            Connection(
                environment="foo_id",
                compute_pool_id="1234",
                organization_id="4567",
                flink_api_key="valid-key",
                flink_api_secret="valid-secret",
                cloud_provider=cloud_provider,
                cloud_region=cloud_region,
                endpoint=endpoint,
            )


@pytest.mark.unit
class TestConnectChecks:
    """Tests for connection checks when creating a connection."""

    def test_requires_environment(self, connection_factory: ConnectionFactory):
        """Test that creating a connection without an environment raises an error."""
        with pytest.raises(InterfaceError, match="Environment ID is required"):
            connection_factory(environment="")

    def test_requires_compute_pool_id(self, connection_factory: ConnectionFactory):
        """Test that creating a connection without a compute pool ID raises an error."""
        with pytest.raises(InterfaceError, match="Compute pool ID is required"):
            connection_factory(environment="foo_id", compute_pool_id="")

    def test_requires_organization_id(self, connection_factory: ConnectionFactory):
        """Test that creating a connection without an organization ID raises an error."""
        with pytest.raises(InterfaceError, match="Organization ID is required"):
            connection_factory(environment="foo_id", compute_pool_id="1234", organization_id="")

    def test_requires_cloud_provider(self, connection_factory: ConnectionFactory):
        """Test that cloud provider is required when endpoint is not provided."""
        with pytest.raises(
            InterfaceError, match="Cloud provider is required when endpoint is not provided"
        ):
            connection_factory(
                environment="foo_id",
                compute_pool_id="1234",
                organization_id="4567",
                cloud_provider="",
            )

    def test_requires_cloud_region(self, connection_factory: ConnectionFactory):
        """Test that cloud region is required when host is not provided."""
        with pytest.raises(
            InterfaceError, match="Cloud region is required when endpoint is not provided"
        ):
            connection_factory(
                environment="foo_id",
                compute_pool_id="1234",
                organization_id="4567",
                cloud_provider="aws",
                cloud_region="",
            )

    def test_builds_endpoint_from_cloud_info(self, connection_factory: ConnectionFactory):
        """Test that when endpoint is not provided, it is built correctly from cloud info."""
        conn = connection_factory(
            environment="env-123",
            organization_id="org-456",
            compute_pool_id="cp-789",
            cloud_provider="aws",
            cloud_region="us-east-1",
            flink_api_key="test-key",
            flink_api_secret="test-secret",
        )

        expected_base_url = "https://flink.us-east-1.aws.confluent.cloud/sql/v1/organizations/org-456/environments/env-123/"
        assert str(conn._client.base_url) == expected_base_url

    def test_requires_flink_api_key(self, connection_factory: ConnectionFactory):
        """Test that creating a connection without a Flink API key raises an error."""
        with pytest.raises(InterfaceError, match="Flink API key and secret are required"):
            connection_factory(
                environment="foo_id",
                compute_pool_id="1234",
                organization_id="4567",
                cloud_provider="aws",
                cloud_region="us-east-1",
                flink_api_key="",
            )

    def test_requires_flink_api_secret(self, connection_factory: ConnectionFactory):
        """Test that creating a connection without a Flink API secret raises an error."""
        with pytest.raises(InterfaceError, match="Flink API key and secret are required"):
            connection_factory(
                environment="foo_id",
                compute_pool_id="1234",
                organization_id="4567",
                cloud_provider="aws",
                cloud_region="us-east-1",
                flink_api_key="valid-key",
                flink_api_secret="",
            )

    def test_endpoint_parameter_uses_custom_endpoint(self, connection_factory: ConnectionFactory):
        """Test that providing endpoint parameter uses the custom endpoint."""
        conn = connection_factory(
            environment="env-123",
            organization_id="org-456",
            compute_pool_id="cp-789",
            flink_api_key="test-key",
            flink_api_secret="test-secret",
            endpoint="https://custom.example.com",
        )

        # Verify base_url constructed with custom host (httpx adds trailing slash)
        expected_base_url = (
            "https://custom.example.com/sql/v1/organizations/org-456/environments/env-123/"
        )
        assert str(conn._client.base_url) == expected_base_url

    def test_endpoint_parameter_with_trailing_slash(self, connection_factory: ConnectionFactory):
        """Test that endpoint parameter with trailing slash is stripped correctly."""
        conn = connection_factory(
            environment="env-123",
            organization_id="org-456",
            compute_pool_id="cp-789",
            flink_api_key="test-key",
            flink_api_secret="test-secret",
            endpoint="https://custom.example.com/",
        )

        # Verify trailing slash was stripped and URL is clean (no double slashes)
        base_url = str(conn._client.base_url)
        assert "custom.example.com" in base_url
        # Verify no double slashes before /sql (trailing slash should be stripped)
        assert "//sql" not in base_url
        assert (
            base_url
            == "https://custom.example.com/sql/v1/organizations/org-456/environments/env-123/"
        )

    def test_endpoint_parameter_without_trailing_slash(self, connection_factory: ConnectionFactory):
        """Test that endpoint parameter without trailing slash works correctly."""
        conn = connection_factory(
            environment="env-123",
            organization_id="org-456",
            compute_pool_id="cp-789",
            flink_api_key="test-key",
            flink_api_secret="test-secret",
            endpoint="https://custom.example.com",
        )

        # Verify URL is clean (same result as with trailing slash)
        base_url = str(conn._client.base_url)
        assert "custom.example.com" in base_url
        # Verify no double slashes
        assert "//sql" not in base_url
        assert (
            base_url
            == "https://custom.example.com/sql/v1/organizations/org-456/environments/env-123/"
        )

    def test_endpoint_raises_error_when_cloud_provider_also_provided(
        self, connection_factory: ConnectionFactory
    ):
        """Test that providing endpoint with cloud_provider raises an error."""
        with pytest.raises(
            InterfaceError,
            match=(
                "cloud_provider and cloud_region should not be provided when endpoint is specified"
            ),
        ):
            connection_factory(
                environment="env-123",
                organization_id="org-456",
                compute_pool_id="cp-789",
                flink_api_key="test-key",
                flink_api_secret="test-secret",
                endpoint="https://custom.example.com",
                cloud_provider="aws",
            )

    def test_endpoint_raises_error_when_cloud_region_also_provided(
        self, connection_factory: ConnectionFactory
    ):
        """Test that providing endpoint with cloud_region raises an error."""
        with pytest.raises(
            InterfaceError,
            match=(
                "cloud_provider and cloud_region should not be provided when endpoint is specified"
            ),
        ):
            connection_factory(
                environment="env-123",
                organization_id="org-456",
                compute_pool_id="cp-789",
                flink_api_key="test-key",
                flink_api_secret="test-secret",
                endpoint="https://custom.example.com",
                cloud_region="us-east-1",
            )


@pytest.mark.unit
@pytest.mark.typeconv
class TestRowTypeRegistry:
    """Unit tests over RowTypeRegistry functionality."""

    def test_get_row_class_bad_field_names_type(self):
        registry = RowTypeRegistry()
        with pytest.raises(
            TypeError,
            match="field_names must be a list or tuple of strings",
        ):
            registry.get_row_class(field_names="not a list")  # type: ignore

    def test_get_row_class_bad_field_name_element(self):
        registry = RowTypeRegistry()
        with pytest.raises(
            TypeError,
            match="All field names must be strings",
        ):
            registry.get_row_class(field_names=["valid", 123])  # type: ignore

    def test_get_row_class_caches_classes(self):
        registry = RowTypeRegistry()
        field_names = ["field1", "field2", "field3"]
        row_class_1 = registry.get_row_class(field_names=field_names)
        assert issubclass(row_class_1, tuple), "Expected row class to be subclass of tuple"
        assert row_class_1._fields == tuple(field_names), "Field names do not match"  # pyright: ignore[reportAttributeAccessIssue]
        row_class_2 = registry.get_row_class(field_names=field_names)
        assert row_class_1 is row_class_2, "Expected same class instance from cache"

    MySimpleNamedTuple = namedtuple("MySimpleNamedTuple", ["x", "y"])

    class MyTypingNamedTuple(NamedTuple):
        a: int
        b: str

    @dataclass
    class MyDataClass:
        p: float
        q: bool

    @pytest.mark.parametrize(
        "not_a_class",
        [
            "just a string",
            42,
            ["a", "list", "of", "strings"],
            {"a": "dict"},
            MySimpleNamedTuple(1, 2),
            MyTypingNamedTuple(3, "four"),
            MyDataClass(1.0, True),
        ],
    )
    def test_register_user_types_hates_instances(self, not_a_class):
        registry = RowTypeRegistry()

        with pytest.raises(
            TypeError,
            match="Expected a namedtuple, NamedTuple, or @dataclass type",
        ):
            registry.register_row_type(not_a_class)  # type: ignore

    class NotATuple:
        pass

    @pytest.mark.parametrize(
        "not_a_supported_class",
        [
            int,
            dict,
            NotATuple,
            list,
        ],
    )
    def test_register_row_type_hates_unsupported_classes(self, not_a_supported_class):
        registry = RowTypeRegistry()

        with pytest.raises(
            TypeError,
            match="Expected a namedtuple, NamedTuple, or @dataclass type",
        ):
            registry.register_row_type(not_a_supported_class)  # type: ignore

    @pytest.mark.parametrize(
        "row_type,field_names",
        [
            (MySimpleNamedTuple, ["x", "y"]),
            (MyTypingNamedTuple, ["a", "b"]),
            (MyDataClass, ["p", "q"]),
        ],
    )
    def test_register_user_type_success(self, row_type, field_names):
        registry = RowTypeRegistry()

        registry.register_row_type(row_type)
        retrieved_class = registry.get_row_class(field_names)
        assert retrieved_class is row_type, "Expected to retrieve the registered class"


@pytest.mark.unit
class TestConnectionDeleteStatement:
    """Tests for connection.delete_statement method."""

    def test_delete_already_deleted_statement_is_noop(
        self,
        invalid_credential_connection: Connection,
        statement_response_factory: StatementResponseFactory,
        mocker,
    ):
        # Make a mock statement
        statement = Statement.from_response(
            invalid_credential_connection,
            statement_response_factory(),
        )

        # Make smell deleted already.
        statement._deleted = True
        assert statement.is_deleted

        logger_info_spy = mocker.spy(connection_module_logger, "info")

        invalid_credential_connection.delete_statement(statement)  # Should be a no-op

        logger_info_spy.assert_called_with(
            f"Statement {statement.name} is already deleted, ignoring"
        )

    def test_delete_statement_not_by_name_or_statement_raises(
        self,
        invalid_credential_connection: Connection,
    ):
        with pytest.raises(
            TypeError,
            match="Statement to delete must be specified by name or Statement object",
        ):
            invalid_credential_connection.delete_statement(123)  # type: ignore


@pytest.mark.unit
class TestConnectionListStatements:
    """Tests for connection.list_statements method."""

    def test_list_statements_single_page(
        self,
        invalid_credential_connection: Connection,
        statement_response_factory: StatementResponseFactory,
        mocker,
    ):
        """Test listing statements that fit in a single page."""
        # Create mock response with 2 statements, no pagination
        statement1 = statement_response_factory(name="stmt-1")
        statement2 = statement_response_factory(name="stmt-2")

        mock_response = Mock()
        mock_response.json.return_value = {"data": [statement1, statement2], "metadata": {}}

        request_mock = mocker.patch.object(
            invalid_credential_connection._client, "request", return_value=mock_response
        )

        # Call list_statements
        statements = invalid_credential_connection.list_statements(label="my-label", page_size=100)

        # Verify request was made with correct parameters
        request_mock.assert_called_once_with(
            "GET",
            "/statements",
            params={"label_selector": "user.confluent.io/my-label=true", "page_size": 100},
        )

        # Verify we got 2 statements back
        assert len(statements) == 2
        assert statements[0].name == "stmt-1"
        assert statements[1].name == "stmt-2"

    def test_list_statements_multiple_pages(
        self,
        invalid_credential_connection: Connection,
        statement_response_factory: StatementResponseFactory,
        mocker,
    ):
        """Test listing statements with pagination across multiple pages."""
        # Create mock responses for 2 pages
        statement1 = statement_response_factory(name="stmt-1")
        statement2 = statement_response_factory(name="stmt-2")
        statement3 = statement_response_factory(name="stmt-3")
        statement4 = statement_response_factory(name="stmt-4")

        # First page response with 'next' URL
        mock_response_page1 = Mock()
        mock_response_page1.json.return_value = {
            "data": [statement1, statement2],
            "metadata": {
                "next": "https://api.confluent.cloud/statements?page_token=token123&label_selector=user.confluent.io%2Fmy-label%3Dtrue"
            },
        }

        # Second page response without 'next' URL (last page)
        mock_response_page2 = Mock()
        mock_response_page2.json.return_value = {"data": [statement3, statement4], "metadata": {}}

        # Track the actual parameters passed on each call
        captured_params = []

        def capture_request(*args, **kwargs):
            # Deep copy the params to avoid mutation issues
            captured_params.append(copy.deepcopy(kwargs.get("params", {})))
            # Vary which mock response we return based on how many times we've been called (first
            # call gets page 1, second call gets page 2)
            return [mock_response_page1, mock_response_page2][len(captured_params) - 1]

        request_mock = mocker.patch.object(
            invalid_credential_connection._client, "request", side_effect=capture_request
        )

        # Call list_statements with small page_size to force pagination
        statements = invalid_credential_connection.list_statements(label="my-label", page_size=2)

        # Verify we made 2 requests
        assert request_mock.call_count == 2

        # Verify first request (no page_token)
        assert captured_params[0] == {
            "label_selector": "user.confluent.io/my-label=true",
            "page_size": 2,
        }

        # Verify second request includes page_token
        assert captured_params[1] == {
            "label_selector": "user.confluent.io/my-label=true",
            "page_size": 2,
            "page_token": "token123",
        }

        # Verify we got all 4 statements back
        assert len(statements) == 4
        assert statements[0].name == "stmt-1"
        assert statements[1].name == "stmt-2"
        assert statements[2].name == "stmt-3"
        assert statements[3].name == "stmt-4"

    def test_list_statements_empty_result(
        self,
        invalid_credential_connection: Connection,
        mocker,
    ):
        """Test listing statements when no statements match the label."""
        mock_response = Mock()
        mock_response.json.return_value = {"data": [], "metadata": {}}

        request_mock = mocker.patch.object(
            invalid_credential_connection._client, "request", return_value=mock_response
        )

        # Call list_statements with a label that doesn't match any statements
        statements = invalid_credential_connection.list_statements(label="nonexistent-label")

        # Verify request was made with correct parameters
        request_mock.assert_called_once_with(
            "GET",
            "/statements",
            params={"label_selector": "user.confluent.io/nonexistent-label=true", "page_size": 100},
        )

        # Verify we got empty list
        assert len(statements) == 0
        assert statements == []

    def test_list_statements_custom_page_size(
        self,
        invalid_credential_connection: Connection,
        statement_response_factory: StatementResponseFactory,
        mocker,
    ):
        """Test that custom page_size is passed correctly."""
        mock_response = Mock()
        mock_response.json.return_value = {"data": [statement_response_factory()], "metadata": {}}

        request_mock = mocker.patch.object(
            invalid_credential_connection._client, "request", return_value=mock_response
        )

        # Call with custom page_size
        invalid_credential_connection.list_statements(label="test-label", page_size=50)

        # Verify page_size parameter
        request_mock.assert_called_once()
        call_params = request_mock.call_args[1]["params"]
        assert call_params["page_size"] == 50

    def test_list_statements_label_prefix_added(
        self,
        invalid_credential_connection: Connection,
        statement_response_factory: StatementResponseFactory,
        mocker,
    ):
        """Test that the label prefix is correctly added to the filter."""
        mock_response = Mock()
        mock_response.json.return_value = {"data": [statement_response_factory()], "metadata": {}}

        request_mock = mocker.patch.object(
            invalid_credential_connection._client, "request", return_value=mock_response
        )

        # Call list_statements
        invalid_credential_connection.list_statements(label="custom-label")

        # Verify label_selector has the prefix
        call_params = request_mock.call_args[1]["params"]
        assert call_params["label_selector"] == "user.confluent.io/custom-label=true"

    def test_does_not_double_prefix_label(
        self,
        invalid_credential_connection: Connection,
        statement_response_factory: StatementResponseFactory,
        mocker,
    ):
        """Test that if the user includes the label prefix, it is not added again."""
        mock_response = Mock()
        mock_response.json.return_value = {"data": [statement_response_factory()], "metadata": {}}

        request_mock = mocker.patch.object(
            invalid_credential_connection._client, "request", return_value=mock_response
        )

        # Call list_statements with label that already has prefix
        invalid_credential_connection.list_statements(label="user.confluent.io/already-prefixed")

        # Verify label_selector is not double-prefixed
        call_params = request_mock.call_args[1]["params"]
        assert call_params["label_selector"] == "user.confluent.io/already-prefixed=true"


@pytest.mark.unit
class TestGetStatement:
    """Tests for connection.get_statement method."""

    def test_get_statement_with_string_name(
        self,
        invalid_credential_connection: Connection,
        statement_response_factory: StatementResponseFactory,
        mocker,
    ):
        """Test getting a statement by string name."""
        statement_data = statement_response_factory(name="my-statement")

        # Mock _get_statement to return the statement data
        get_statement_mock = mocker.patch.object(
            invalid_credential_connection,
            "_get_statement",
            return_value=statement_data,
        )

        # Call get_statement with string name
        result = invalid_credential_connection.get_statement("my-statement")

        # Verify _get_statement was called with correct name
        get_statement_mock.assert_called_once_with("my-statement")

        # Verify we got a Statement object back
        assert isinstance(result, Statement)
        assert result.name == "my-statement"

    def test_get_statement_with_statement_object(
        self,
        invalid_credential_connection: Connection,
        statement_response_factory: StatementResponseFactory,
        mocker,
    ):
        """Test getting a statement by passing a Statement object."""
        statement_data = statement_response_factory(name="existing-statement")

        # Create an initial Statement object
        initial_statement = Statement.from_response(invalid_credential_connection, statement_data)

        # Mock _get_statement to return updated data
        updated_data = statement_response_factory(name="existing-statement")
        get_statement_mock = mocker.patch.object(
            invalid_credential_connection,
            "_get_statement",
            return_value=updated_data,
        )

        # Call get_statement with Statement object
        result = invalid_credential_connection.get_statement(initial_statement)

        # Verify _get_statement was called with the statement name
        get_statement_mock.assert_called_once_with("existing-statement")

        # Verify we got a Statement object back
        assert isinstance(result, Statement)
        assert result.name == "existing-statement"

    def test_get_statement_with_invalid_type(
        self,
        invalid_credential_connection: Connection,
    ):
        """Test that passing an invalid type raises TypeError."""
        with pytest.raises(
            TypeError,
            match="Statement must be specified by name or Statement object",
        ):
            invalid_credential_connection.get_statement(123)  # type: ignore

    def test_get_statement_not_found(
        self,
        invalid_credential_connection: Connection,
        mocker,
    ):
        """Test that getting a non-existent statement raises StatementNotFoundError."""
        # Mock _request to raise HTTPStatusError with 404
        request_mock = mocker.patch.object(invalid_credential_connection._client, "request")
        response_mock = Mock()
        response_mock.status_code = 404
        response_mock.json.return_value = {
            "errors": [{"detail": "Statement not found"}]
        }

        def raise_http_error():
            raise httpx.HTTPStatusError(
                "Statement not found", request=Mock(), response=response_mock
            )

        response_mock.raise_for_status = raise_http_error
        request_mock.return_value = response_mock

        # Should raise the more specific StatementNotFoundError
        with pytest.raises(StatementNotFoundError) as exc_info:
            invalid_credential_connection.get_statement("non-existent")

        # Verify exception has correct attributes
        assert exc_info.value.statement_name == "non-existent"
        assert "non-existent" in str(exc_info.value)

    def test_get_statement_api_error(
        self,
        invalid_credential_connection: Connection,
        mocker,
    ):
        """Test that non-404 errors remain as OperationalError."""
        # Mock _request to raise HTTPStatusError with 500
        request_mock = mocker.patch.object(invalid_credential_connection._client, "request")
        response_mock = Mock()
        response_mock.status_code = 500
        response_mock.json.return_value = {
            "errors": [{"detail": "Internal server error"}]
        }

        def raise_http_error():
            raise httpx.HTTPStatusError(
                "Internal server error", request=Mock(), response=response_mock
            )

        response_mock.raise_for_status = raise_http_error
        request_mock.return_value = response_mock

        # Should raise OperationalError, not StatementNotFoundError
        with pytest.raises(OperationalError) as exc_info:
            invalid_credential_connection.get_statement("some-statement")

        # Verify it's not the specific subclass
        assert not isinstance(exc_info.value, StatementNotFoundError)
        assert "500" in str(exc_info.value)

    def test_get_statement_logs_operation(
        self,
        invalid_credential_connection: Connection,
        statement_response_factory: StatementResponseFactory,
        mocker,
    ):
        """Test that get_statement logs the operation."""
        statement_data = statement_response_factory(name="log-test-statement")

        # Mock _get_statement
        mocker.patch.object(
            invalid_credential_connection,
            "_get_statement",
            return_value=statement_data,
        )

        # Spy on logger.info
        logger_info_spy = mocker.spy(connection_module_logger, "info")

        # Call get_statement
        invalid_credential_connection.get_statement("log-test-statement")

        # Verify logger was called with expected message
        logger_info_spy.assert_called_with("Getting statement 'log-test-statement'")


@pytest.mark.unit
class TestExecuteStatement:
    """Tests for _execute_statement method."""

    def install_request_mock(self, connection: Connection, mocker):
        """Helper method to set up a request mock for testing _execute_statement.

        Args:
            connection: The connection object to mock requests on
            mocker: The pytest mocker fixture

        Returns:
            The mocked request object that can be used for assertions
        """
        mock_response = Mock()
        mock_response.json.return_value = {
            "name": "test-statement",
            "status": {"phase": "RUNNING"},
        }

        return mocker.patch.object(connection._client, "request", return_value=mock_response)

    def test_no_label_omits_metadata_labels(
        self,
        invalid_credential_connection: Connection,
        mocker,
    ):
        """Test that when no label is provided, the payload metadata has no labels key."""
        request_mock = self.install_request_mock(invalid_credential_connection, mocker)

        # Execute statement without a label
        invalid_credential_connection._execute_statement(
            "SELECT 1",
            ExecutionMode.STREAMING_QUERY,
            statement_name="test-stmt",
            statement_labels=None,
        )

        # Verify the request was made
        request_mock.assert_called_once()
        call_args = request_mock.call_args

        # Get the JSON payload
        payload = call_args[1]["json"]

        # Verify no metadata key exists in payload
        assert "metadata" not in payload, (
            "Payload should not contain metadata when no label provided"
        )

    def test_unprefixed_label_gets_prefixed(
        self,
        invalid_credential_connection: Connection,
        mocker,
    ):
        """Test that when an un-prefixed label is provided, it gets prefixed.

        Verifies that the label is present in payload metadata['labels'].
        """
        request_mock = self.install_request_mock(invalid_credential_connection, mocker)

        # Execute statement with un-prefixed label
        test_label = "my-custom-label"
        invalid_credential_connection._execute_statement(
            "SELECT 1",
            ExecutionMode.STREAMING_QUERY,
            statement_name="test-stmt",
            statement_labels=[test_label],
        )

        # Verify the request was made
        request_mock.assert_called_once()
        call_args = request_mock.call_args

        # Get the JSON payload
        payload = call_args[1]["json"]

        # Verify metadata exists and has labels
        assert "metadata" in payload, "Payload should contain metadata when label provided"
        assert "labels" in payload["metadata"], "Metadata should contain labels"

        # Verify the label was prefixed correctly
        expected_label_key = f"{LABEL_PREFIX}{test_label}"
        assert expected_label_key in payload["metadata"]["labels"], (
            f"Label should be prefixed with {LABEL_PREFIX}"
        )
        assert payload["metadata"]["labels"][expected_label_key] == "true"

    def test_prefixed_label_not_double_prefixed(
        self,
        invalid_credential_connection: Connection,
        mocker,
    ):
        """Test that when an already-prefixed label is provided, it does not get double-prefixed."""
        request_mock = self.install_request_mock(invalid_credential_connection, mocker)

        # Execute statement with already-prefixed label
        already_prefixed_label = f"{LABEL_PREFIX}already-prefixed"
        invalid_credential_connection._execute_statement(
            "SELECT 1",
            ExecutionMode.STREAMING_QUERY,
            statement_name="test-stmt",
            statement_labels=[already_prefixed_label],
        )

        # Verify the request was made
        request_mock.assert_called_once()
        call_args = request_mock.call_args

        # Get the JSON payload
        payload = call_args[1]["json"]

        # Verify metadata exists and has labels
        assert "metadata" in payload, "Payload should contain metadata when label provided"
        assert "labels" in payload["metadata"], "Metadata should contain labels"

        # Verify the label was NOT double-prefixed
        assert already_prefixed_label in payload["metadata"]["labels"], (
            "Label should not be double-prefixed"
        )
        assert payload["metadata"]["labels"][already_prefixed_label] == "true"

        # Verify no double-prefixed version exists
        double_prefixed = f"{LABEL_PREFIX}{already_prefixed_label}"
        assert double_prefixed not in payload["metadata"]["labels"], (
            "Label should not be double-prefixed"
        )

    def test_empty_list_omits_metadata_labels(
        self,
        invalid_credential_connection: Connection,
        mocker,
    ):
        """Test that when an empty list is provided, no metadata is added (same as None)."""
        request_mock = self.install_request_mock(invalid_credential_connection, mocker)

        # Execute statement with empty list
        invalid_credential_connection._execute_statement(
            "SELECT 1",
            ExecutionMode.STREAMING_QUERY,
            statement_name="test-stmt",
            statement_labels=[],
        )

        # Verify the request was made
        request_mock.assert_called_once()
        call_args = request_mock.call_args

        # Get the JSON payload
        payload = call_args[1]["json"]

        # Verify no metadata key exists in payload (same behavior as None)
        assert "metadata" not in payload, (
            "Payload should not contain metadata when empty list provided"
        )

    def test_multiple_labels_all_added(
        self,
        invalid_credential_connection: Connection,
        mocker,
    ):
        """Test that when multiple labels are provided, all are added to metadata."""
        request_mock = self.install_request_mock(invalid_credential_connection, mocker)

        # Execute statement with multiple labels
        labels = ["label1", "label2", "label3"]
        invalid_credential_connection._execute_statement(
            "SELECT 1",
            ExecutionMode.STREAMING_QUERY,
            statement_name="test-stmt",
            statement_labels=labels,
        )

        # Verify the request was made
        request_mock.assert_called_once()
        call_args = request_mock.call_args

        # Get the JSON payload
        payload = call_args[1]["json"]

        # Verify metadata exists and has labels
        assert "metadata" in payload, "Payload should contain metadata when labels provided"
        assert "labels" in payload["metadata"], "Metadata should contain labels"

        # Verify all labels are present and prefixed correctly
        for label in labels:
            expected_label_key = f"{LABEL_PREFIX}{label}"
            assert expected_label_key in payload["metadata"]["labels"], (
                f"Label {label} should be in metadata"
            )
            assert payload["metadata"]["labels"][expected_label_key] == "true"

        # Verify we have exactly 3 labels
        assert len(payload["metadata"]["labels"]) == 3

    def test_multiple_labels_mixed_prefixes(
        self,
        invalid_credential_connection: Connection,
        mocker,
    ):
        """Test that mixed prefixed and unprefixed labels are handled correctly."""
        request_mock = self.install_request_mock(invalid_credential_connection, mocker)

        # Execute statement with mixed prefix labels
        prefixed_label = f"{LABEL_PREFIX}already-prefixed"
        unprefixed_label = "not-prefixed"
        invalid_credential_connection._execute_statement(
            "SELECT 1",
            ExecutionMode.STREAMING_QUERY,
            statement_name="test-stmt",
            statement_labels=[prefixed_label, unprefixed_label],
        )

        # Verify the request was made
        request_mock.assert_called_once()
        call_args = request_mock.call_args

        # Get the JSON payload
        payload = call_args[1]["json"]

        # Verify metadata exists and has labels
        assert "metadata" in payload, "Payload should contain metadata when labels provided"
        assert "labels" in payload["metadata"], "Metadata should contain labels"

        # Verify the prefixed label was not double-prefixed
        assert prefixed_label in payload["metadata"]["labels"]
        assert payload["metadata"]["labels"][prefixed_label] == "true"

        # Verify the unprefixed label was prefixed
        expected_unprefixed_key = f"{LABEL_PREFIX}{unprefixed_label}"
        assert expected_unprefixed_key in payload["metadata"]["labels"]
        assert payload["metadata"]["labels"][expected_unprefixed_key] == "true"

        # Verify we have exactly 2 labels
        assert len(payload["metadata"]["labels"]) == 2

    def test_non_list_raises_error(
        self,
        invalid_credential_connection: Connection,
        mocker,
    ):
        """Test that providing a non-list value raises InterfaceError."""
        self.install_request_mock(invalid_credential_connection, mocker)

        # Execute statement with a string instead of list
        with pytest.raises(InterfaceError, match="statement_labels must be a list of strings"):
            invalid_credential_connection._execute_statement(
                "SELECT 1",
                ExecutionMode.STREAMING_QUERY,
                statement_name="test-stmt",
                statement_labels="not-a-list",  # type: ignore[arg-type]
            )

    def test_non_string_label_raises_error(
        self,
        invalid_credential_connection: Connection,
        mocker,
    ):
        """Test that providing non-string elements in the list raises InterfaceError."""
        self.install_request_mock(invalid_credential_connection, mocker)

        # Execute statement with non-string label element
        with pytest.raises(InterfaceError, match="All statement labels must be strings"):
            invalid_credential_connection._execute_statement(
                "SELECT 1",
                ExecutionMode.STREAMING_QUERY,
                statement_name="test-stmt",
                statement_labels=["valid", 123],  # type: ignore[list-item]
            )

    def test_hidden_label_constant(
        self,
        invalid_credential_connection: Connection,
        mocker,
    ):
        """Test that HIDDEN_LABEL constant works correctly."""
        request_mock = self.install_request_mock(invalid_credential_connection, mocker)

        # Execute statement with HIDDEN_LABEL
        invalid_credential_connection._execute_statement(
            "SELECT 1",
            ExecutionMode.STREAMING_QUERY,
            statement_name="test-stmt",
            statement_labels=[HIDDEN_LABEL],
        )

        # Verify the request was made
        request_mock.assert_called_once()
        call_args = request_mock.call_args

        # Get the JSON payload
        payload = call_args[1]["json"]

        # Verify metadata exists and has labels
        assert "metadata" in payload, "Payload should contain metadata when label provided"
        assert "labels" in payload["metadata"], "Metadata should contain labels"

        # Verify HIDDEN_LABEL is present (already prefixed, so no double-prefix)
        assert HIDDEN_LABEL in payload["metadata"]["labels"]
        assert payload["metadata"]["labels"][HIDDEN_LABEL] == "true"


@pytest.fixture()
def http_agent_connection_factory(
    connection_factory: ConnectionFactory,
) -> ConnectionFactory:
    """Factory fixture that creates connections with custom http_user_agent.

    Returns a function that accepts an optional http_user_agent string and creates
    a connection with that agent (or default if None).
    """

    def _create_with_agent(http_user_agent: str | None = None) -> Connection:
        return connection_factory(
            environment="test-env",
            compute_pool_id="test-pool",
            organization_id="test-org",
            cloud_provider="aws",
            cloud_region="us-east-1",
            flink_api_key="test-key",
            flink_api_secret="test-secret",
            http_user_agent=http_user_agent,
        )

    return _create_with_agent


@pytest.mark.unit
class TestClosingCursor:
    """Tests for the closing_cursor context manager."""

    def test_closing_cursor_creates_cursor(self, invalid_credential_connection):
        """Test that closing_cursor creates a cursor with specified parameters."""
        with invalid_credential_connection.closing_cursor(as_dict=False) as cursor:
            assert cursor is not None
            assert cursor.execution_mode == ExecutionMode.SNAPSHOT  # Default mode

    def test_closing_cursor_respects_as_dict_parameter(self, invalid_credential_connection):
        """Test that closing_cursor respects the as_dict parameter."""
        # Test with as_dict=False (default)
        with invalid_credential_connection.closing_cursor(as_dict=False) as cursor:
            assert cursor is not None
            assert cursor.as_dict is False

        # Test with as_dict=True
        with invalid_credential_connection.closing_cursor(as_dict=True) as cursor:
            assert cursor is not None
            assert cursor.as_dict is True

    def test_closing_cursor_respects_mode_parameter(self, invalid_credential_connection):
        """Test that closing_cursor respects the mode parameter."""
        # Test with SNAPSHOT mode (default)
        with invalid_credential_connection.closing_cursor(mode=ExecutionMode.SNAPSHOT) as cursor:
            assert cursor is not None
            assert cursor.execution_mode == ExecutionMode.SNAPSHOT

        # Test with STREAMING_QUERY mode
        with invalid_credential_connection.closing_cursor(
            mode=ExecutionMode.STREAMING_QUERY
        ) as cursor:
            assert cursor is not None
            assert cursor.execution_mode == ExecutionMode.STREAMING_QUERY

    def test_closing_cursor_closes_cursor_on_exit(self, invalid_credential_connection):
        """Test that cursor is properly closed after exiting context manager."""
        cursor = None
        with invalid_credential_connection.closing_cursor() as c:
            cursor = c
            assert cursor.is_closed is False

        # Verify cursor is closed after context manager exits
        assert cursor is not None
        assert cursor.is_closed is True

    def test_closing_cursor_closes_even_on_exception(self, invalid_credential_connection):
        """Test that cursor is closed even if an exception is raised in the context."""
        cursor = None
        with pytest.raises(ValueError), invalid_credential_connection.closing_cursor() as c:  # noqa: SIM117,E501
            cursor = c
            assert cursor.is_closed is False
            raise ValueError("Test exception")

        # Verify cursor is closed even after exception
        assert cursor is not None
        assert cursor.is_closed is True


@pytest.mark.unit
class TestClosingStreamingCursor:
    """Tests for the closing_streaming_cursor context manager."""

    def test_closing_streaming_cursor_creates_streaming_cursor(self, invalid_credential_connection):
        """Test that closing_streaming_cursor creates a cursor in STREAMING_QUERY mode."""
        with invalid_credential_connection.closing_streaming_cursor() as cursor:
            assert cursor is not None
            assert cursor.is_streaming is True, "Expected cursor to be in streaming mode"
            assert cursor.execution_mode == ExecutionMode.STREAMING_QUERY

    def test_closing_streaming_cursor_respects_as_dict_parameter(
        self, invalid_credential_connection
    ):
        """Test that closing_streaming_cursor respects the as_dict parameter."""
        # Test with as_dict=False (default)
        with invalid_credential_connection.closing_streaming_cursor(as_dict=False) as cursor:
            assert cursor is not None
            assert cursor.as_dict is False

        # Test with as_dict=True
        with invalid_credential_connection.closing_streaming_cursor(as_dict=True) as cursor:
            assert cursor is not None
            assert cursor.as_dict is True

    def test_closing_streaming_cursor_closes_cursor_on_exit(self, invalid_credential_connection):
        """Test that cursor is properly closed after exiting context manager."""
        cursor = None
        with invalid_credential_connection.closing_streaming_cursor() as c:
            cursor = c
            assert cursor.is_closed is False

        # Verify cursor is closed after context manager exits
        assert cursor is not None
        assert cursor.is_closed is True

    def test_closing_streaming_cursor_closes_even_on_exception(self, invalid_credential_connection):
        """Test that cursor is closed even if an exception is raised in the context."""
        cursor = None
        with (
            pytest.raises(ValueError),
            invalid_credential_connection.closing_streaming_cursor() as c,
        ):  # noqa: SIM117,E501
            cursor = c
            assert cursor.is_closed is False
            raise ValueError("Test exception")

        # Verify cursor is closed even after exception
        assert cursor is not None
        assert cursor.is_closed is True

    def test_closing_streaming_cursor_equivalent_to_closing_cursor_with_mode(
        self, invalid_credential_connection
    ):
        """Test that closing_streaming_cursor is equivalent to closing_cursor.

        Verifies equivalence to closing_cursor(mode=ExecutionMode.STREAMING_QUERY).
        """
        # Create two cursors - one with closing_streaming_cursor, one with closing_cursor
        with (
            invalid_credential_connection.closing_streaming_cursor(as_dict=True) as cursor1,
            invalid_credential_connection.closing_cursor(
                as_dict=True, mode=ExecutionMode.STREAMING_QUERY
            ) as cursor2,
        ):
            # Both should have the same execution mode
            assert cursor1.execution_mode == cursor2.execution_mode
            assert cursor1.is_streaming == cursor2.is_streaming
            assert cursor1.as_dict == cursor2.as_dict


@pytest.mark.unit
class TestHttpUserAgentProperty:
    """Tests for the http_user_agent property getter/setter."""

    def test_default_user_agent(self, invalid_credential_connection: Connection):
        """Test that the default user agent is set correctly."""
        expected = f"Confluent-SQL-Dbapi/v{VERSION} (https://confluent.io; support@confluent.io)"
        assert invalid_credential_connection.http_user_agent == expected
        assert invalid_credential_connection.http_user_agent == Connection.DEFAULT_USER_AGENT
        # Verify the header is applied to the httpx client
        assert invalid_credential_connection._client.headers.get("User-Agent") == expected

    def test_custom_user_agent_via_constructor(
        self, http_agent_connection_factory: ConnectionFactory
    ):
        """Test that a custom user agent can be set via constructor."""
        custom_agent = "my-app/1.0.0"
        conn = http_agent_connection_factory(http_user_agent=custom_agent)
        assert conn.http_user_agent == custom_agent
        # Verify the header is applied to the httpx client at construction time
        assert conn._client.headers.get("User-Agent") == custom_agent

    def test_set_user_agent_via_property(self, invalid_credential_connection: Connection):
        """Test that user agent can be set via property setter."""
        new_agent = "updated-app/2.0"
        invalid_credential_connection.http_user_agent = new_agent
        assert invalid_credential_connection.http_user_agent == new_agent
        # Verify the header is updated in the httpx client when property is set
        assert invalid_credential_connection._client.headers.get("User-Agent") == new_agent

    def test_set_user_agent_accepts_boundary_values(
        self, invalid_credential_connection: Connection
    ):
        """Test that user agent accepts values at length boundaries."""
        # Exactly 1 character (minimum valid)
        invalid_credential_connection.http_user_agent = "a"
        assert invalid_credential_connection.http_user_agent == "a"

        # Exactly 100 characters (maximum valid)
        max_length = "a" * 100
        invalid_credential_connection.http_user_agent = max_length
        assert invalid_credential_connection.http_user_agent == max_length

    @pytest.mark.parametrize(
        "invalid_value,expected_error",
        [
            (123, "http_user_agent must be a string, got int"),
            (None, "http_user_agent must be a string, got NoneType"),
            (["list"], "http_user_agent must be a string, got list"),
            ({"dict": "value"}, "http_user_agent must be a string, got dict"),
            ("", "http_user_agent length must be between 1 and 100 characters, got 0"),
            ("a" * 101, "http_user_agent length must be between 1 and 100 characters, got 101"),
            ("a" * 200, "http_user_agent length must be between 1 and 100 characters, got 200"),
        ],
    )
    def test_set_user_agent_rejects_invalid_values(
        self,
        invalid_credential_connection: Connection,
        invalid_value,
        expected_error,
    ):
        """Test that setting user agent to invalid type or length raises InterfaceError."""
        with pytest.raises(InterfaceError, match=expected_error):
            invalid_credential_connection.http_user_agent = invalid_value

    @pytest.mark.parametrize(
        "invalid_value,expected_error",
        [
            (123, "http_user_agent must be a string, got int"),
            ("", "http_user_agent length must be between 1 and 100 characters, got 0"),
            ("a" * 101, "http_user_agent length must be between 1 and 100 characters, got 101"),
        ],
    )
    def test_constructor_validation(
        self, http_agent_connection_factory: ConnectionFactory, invalid_value, expected_error
    ):
        """Test that constructor validates user agent type and length."""
        with pytest.raises(InterfaceError, match=expected_error):
            http_agent_connection_factory(http_user_agent=invalid_value)
