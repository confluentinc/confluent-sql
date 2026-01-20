from collections import namedtuple
from dataclasses import dataclass
from typing import NamedTuple
from unittest.mock import Mock

import httpx
import pytest

from confluent_sql import InterfaceError, OperationalError
from confluent_sql.connection import Connection, RowTypeRegistry, connect
from confluent_sql.connection import logger as connection_module_logger
from confluent_sql.statement import Statement
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


def raise_not_found():
    """Mock function that raises an HTTPStatusError with a mocked 404 response."""
    mock_response = Mock()
    mock_response.status_code = 404
    raise httpx.HTTPStatusError("Statement not found", request=Mock(), response=mock_response)


@pytest.mark.unit
def test_connection_error(invalid_credential_connection, mocker):
    """Test that we get meaningful error message when a response returns an error."""

    request_mock = mocker.patch.object(invalid_credential_connection._client, "request")
    response_mock = Mock()
    response_mock.raise_for_status = raise_not_found
    request_mock.return_value = response_mock
    with pytest.raises(OperationalError, match="Error sending request 404"):
        invalid_credential_connection._get_statement("test-name")


@pytest.mark.unit
class TestConnectionDeleteStatementErrors:
    """Tests for delete_statement error handling."""

    def test_delete_statement_not_found(self, invalid_credential_connection: Connection, mocker):
        """Test that deleting a non-existent statement raises the appropriate error."""
        request_mock = mocker.patch.object(invalid_credential_connection._client, "request")
        response_mock = Mock()
        response_mock.raise_for_status = raise_not_found
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
        """Test that creating a connection without a cloud provider raises an error."""
        with pytest.raises(InterfaceError, match="Cloud provider is required"):
            connection_factory(
                environment="foo_id",
                compute_pool_id="1234",
                organization_id="4567",
                cloud_provider="",
            )

    def test_requires_cloud_region(self, connection_factory: ConnectionFactory):
        """Test that creating a connection without a cloud region raises an error."""
        with pytest.raises(InterfaceError, match="Cloud region is required"):
            connection_factory(
                environment="foo_id",
                compute_pool_id="1234",
                organization_id="4567",
                cloud_provider="aws",
                cloud_region="",
            )

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
