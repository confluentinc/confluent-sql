import copy
import json
from collections import namedtuple
from dataclasses import dataclass
from typing import NamedTuple
from unittest.mock import Mock, patch

import httpx
import pytest

from confluent_sql import InterfaceError, OperationalError, StatementNotFoundError
from confluent_sql.__version__ import VERSION
from confluent_sql.connection import (
    DEFAULT_HTTP_TIMEOUT_SECS,
    Connection,
    RowTypeRegistry,
    _resolve_api_credentials,
    connect,
)
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
        environment_id="env-id",
        organization_id="org-id",
        compute_pool_id="cp-id",
        cloud_provider="aws",
        cloud_region="us-east-1",
        flink_api_key="invalid-key",
        flink_api_secret="invalid-secret",
    )


@pytest.fixture()
def poolless_connection() -> Connection:
    """A connection created without a compute pool, exercising the default-pool path."""
    return connect(
        environment_id="env-id",
        organization_id="org-id",
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

    def test_get_statement_converts_404_to_statement_not_found_error(
        self, invalid_credential_connection: Connection, mocker
    ):
        """Test that 404 errors are converted to StatementNotFoundError.

        When the API returns a 404 error for a statement GET request,
        _get_statement should raise StatementNotFoundError with the statement name.
        """
        request_mock = mocker.patch.object(
            invalid_credential_connection._get_flink_client(), "request"
        )

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
        request_mock = mocker.patch.object(
            invalid_credential_connection._get_flink_client(), "request"
        )

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
            environment_id="foo_id",
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
        request_mock = mocker.patch.object(
            invalid_credential_connection._get_flink_client(), "request"
        )
        response_mock = Mock()
        response_mock.raise_for_status = _create_http_error_404
        request_mock.return_value = response_mock

        # Will not raise since we ignore 404s in delete_statement
        invalid_credential_connection.delete_statement("non-existent-statement")

    def test_delete_statement_other_error(self, invalid_credential_connection: Connection, mocker):
        """Test that deleting a statement that raises an error other than 404
        raises OperationalError."""
        request_mock = mocker.patch.object(
            invalid_credential_connection._get_flink_client(), "request"
        )
        response_mock = Mock()

        def raise_internal_server_error():
            mock_response = Mock()
            mock_response.status_code = 500
            raise httpx.HTTPStatusError(
                "Internal Server Error", request=Mock(), response=mock_response
            )

        response_mock.raise_for_status = raise_internal_server_error
        request_mock.return_value = response_mock

        with pytest.raises(OperationalError, match="Error deleting statement") as exc_info:
            invalid_credential_connection.delete_statement("statement-with-error")
        assert exc_info.value.http_status_code == 500


def _ok_response(statement_json: dict) -> Mock:
    """A 200 response mock whose .json() returns the given statement dict."""
    response_mock = Mock()
    response_mock.status_code = 200
    response_mock.raise_for_status = Mock()
    response_mock.json.return_value = statement_json
    return response_mock


def _http_error_response(status_code: int) -> Mock:
    """A response mock whose raise_for_status() raises an HTTPStatusError of the given code."""
    response_mock = Mock()
    response_mock.status_code = status_code

    def _raise():
        error_response = Mock()
        error_response.status_code = status_code
        raise httpx.HTTPStatusError("boom", request=Mock(), response=error_response)

    response_mock.raise_for_status = _raise
    return response_mock


@pytest.mark.unit
class TestConnectionStopStatement:
    """Tests for Connection.stop_statement."""

    def test_non_blocking_issues_json_patch_and_returns_stop_requested(
        self,
        invalid_credential_connection: Connection,
        statement_response_factory: StatementResponseFactory,
        mocker,
    ):
        """wait_for_stopped=False issues a single RFC 6902 PATCH and returns without polling. Per
        the real API the accepted-stop response flips spec.stopped to true while status.phase may
        still read RUNNING, so the caller's reliable signal is stop_requested, not the phase."""
        request_mock = mocker.patch.object(
            invalid_credential_connection._get_flink_client(), "request"
        )
        request_mock.return_value = _ok_response(
            statement_response_factory(name="stmt-1", phase="RUNNING", stopped=True)
        )

        result = invalid_credential_connection.stop_statement("stmt-1", wait_for_stopped=False)

        assert result.stop_requested
        # The phase has not yet transitioned -- stop_requested, not the phase, proves acceptance.
        assert result.is_running
        assert not result.is_stopped
        request_mock.assert_called_once()
        args, kwargs = request_mock.call_args
        assert args == ("PATCH", "/statements/stmt-1")
        assert kwargs["headers"]["Content-Type"] == "application/json-patch+json"
        assert json.loads(kwargs["content"]) == [
            {"op": "replace", "path": "/spec/stopped", "value": True}
        ]

    def test_blocking_polls_until_stopped(
        self,
        invalid_credential_connection: Connection,
        statement_response_factory: StatementResponseFactory,
        mocker,
    ):
        """wait_for_stopped=True polls get-statement until the phase settles to STOPPED."""
        mocker.patch("time.sleep")
        request_mock = mocker.patch.object(
            invalid_credential_connection._get_flink_client(), "request"
        )
        # Per the real API, every response after the accepted stop carries spec.stopped=true; the
        # phase trails behind, transitioning STOPPING -> STOPPED asynchronously.
        request_mock.side_effect = [
            # PATCH, then poll 1, then poll 2.
            _ok_response(statement_response_factory(name="stmt-1", phase="STOPPING", stopped=True)),
            _ok_response(statement_response_factory(name="stmt-1", phase="STOPPING", stopped=True)),
            _ok_response(statement_response_factory(name="stmt-1", phase="STOPPED", stopped=True)),
        ]

        result = invalid_credential_connection.stop_statement("stmt-1", wait_for_stopped=True)

        assert result.is_stopped
        assert result.stop_requested
        # One PATCH + two GET polls.
        assert request_mock.call_count == 3
        assert [call.args[0] for call in request_mock.call_args_list] == ["PATCH", "GET", "GET"]

    def test_blocking_returns_on_completed_when_stop_races_a_bounded_query(
        self,
        invalid_credential_connection: Connection,
        statement_response_factory: StatementResponseFactory,
        mocker,
    ):
        """A bounded query that COMPLETES before the stop lands satisfies the stop rather than
        erroring: the blocking wait returns the COMPLETED statement, not just STOPPED. Guards the
        documented contract that any non-FAILED terminal phase is an acceptable outcome."""
        mocker.patch("time.sleep")
        request_mock = mocker.patch.object(
            invalid_credential_connection._get_flink_client(), "request"
        )
        request_mock.side_effect = [
            # PATCH (stop accepted while still RUNNING), then a poll that finds it COMPLETED.
            _ok_response(statement_response_factory(name="stmt-1", phase="RUNNING", stopped=True)),
            _ok_response(
                statement_response_factory(name="stmt-1", phase="COMPLETED", stopped=True)
            ),
        ]

        result = invalid_credential_connection.stop_statement("stmt-1", wait_for_stopped=True)

        assert result.phase.value == "COMPLETED"
        assert result.phase.is_terminal

    def test_blocking_returns_without_polling_when_patch_already_terminal(
        self,
        invalid_credential_connection: Connection,
        statement_response_factory: StatementResponseFactory,
        mocker,
    ):
        """If the PATCH response is already terminal, the blocking wait returns it directly,
        without an additional get-statement poll."""
        request_mock = mocker.patch.object(
            invalid_credential_connection._get_flink_client(), "request"
        )
        request_mock.return_value = _ok_response(
            statement_response_factory(name="stmt-1", phase="STOPPED", stopped=True)
        )

        result = invalid_credential_connection.stop_statement("stmt-1", wait_for_stopped=True)

        assert result.is_stopped
        # Just the PATCH -- the PATCH response was already STOPPED, so no GET poll is needed.
        request_mock.assert_called_once()
        assert request_mock.call_args.args[0] == "PATCH"

    @pytest.mark.parametrize("by_object", [False, True])
    def test_accepts_name_or_statement_object(
        self,
        invalid_credential_connection: Connection,
        statement_response_factory: StatementResponseFactory,
        mocker,
        by_object: bool,
    ):
        """stop_statement accepts either a statement name or a Statement object."""
        request_mock = mocker.patch.object(
            invalid_credential_connection._get_flink_client(), "request"
        )
        request_mock.return_value = _ok_response(
            statement_response_factory(name="stmt-1", phase="STOPPING", stopped=True)
        )

        if by_object:
            running = Statement.from_response(
                invalid_credential_connection,
                statement_response_factory(name="stmt-1", phase="RUNNING"),
            )
            target = running
        else:
            target = "stmt-1"

        result = invalid_credential_connection.stop_statement(target, wait_for_stopped=False)

        assert result.is_stopping
        assert request_mock.call_args.args == ("PATCH", "/statements/stmt-1")

    def test_already_terminal_statement_short_circuits(
        self,
        invalid_credential_connection: Connection,
        statement_response_factory: StatementResponseFactory,
        mocker,
    ):
        """A passed-in Statement already in a terminal phase returns unchanged with no API call."""
        request_mock = mocker.patch.object(
            invalid_credential_connection._get_flink_client(), "request"
        )
        already_stopped = Statement.from_response(
            invalid_credential_connection,
            statement_response_factory(name="stmt-1", phase="STOPPED", stopped=True),
        )

        result = invalid_credential_connection.stop_statement(already_stopped)

        assert result is already_stopped
        request_mock.assert_not_called()

    def test_rejects_bad_type(self, invalid_credential_connection: Connection):
        """A non-str, non-Statement argument raises TypeError."""
        with pytest.raises(TypeError, match="name or Statement object"):
            invalid_credential_connection.stop_statement(42)  # type: ignore[arg-type]

    def test_404_raises_statement_not_found(
        self, invalid_credential_connection: Connection, mocker
    ):
        """A 404 from the PATCH surfaces as StatementNotFoundError naming the statement."""
        request_mock = mocker.patch.object(
            invalid_credential_connection._get_flink_client(), "request"
        )
        request_mock.return_value = _http_error_response(404)

        with pytest.raises(StatementNotFoundError) as exc_info:
            invalid_credential_connection.stop_statement("ghost", wait_for_stopped=False)
        assert exc_info.value.statement_name == "ghost"

    def test_other_http_error_raises_operational_error(
        self, invalid_credential_connection: Connection, mocker
    ):
        """A non-404 error from the PATCH surfaces as OperationalError carrying the status code."""
        request_mock = mocker.patch.object(
            invalid_credential_connection._get_flink_client(), "request"
        )
        request_mock.return_value = _http_error_response(500)

        with pytest.raises(OperationalError, match="Error stopping statement") as exc_info:
            invalid_credential_connection.stop_statement("stmt-1", wait_for_stopped=False)
        assert exc_info.value.http_status_code == 500

    def test_blocking_timeout_raises(
        self,
        invalid_credential_connection: Connection,
        statement_response_factory: StatementResponseFactory,
        mocker,
    ):
        """If the statement never reaches STOPPED within the timeout, OperationalError is raised."""
        mocker.patch("time.sleep")
        request_mock = mocker.patch.object(
            invalid_credential_connection._get_flink_client(), "request"
        )
        request_mock.return_value = _ok_response(
            statement_response_factory(name="stmt-1", phase="STOPPING", stopped=True)
        )

        with pytest.raises(OperationalError, match="did not reach STOPPED within"):
            invalid_credential_connection.stop_statement(
                "stmt-1", wait_for_stopped=True, timeout=0
            )

    def test_blocking_failed_raises(
        self,
        invalid_credential_connection: Connection,
        statement_response_factory: StatementResponseFactory,
        mocker,
    ):
        """A statement that transitions to FAILED while waiting raises OperationalError."""
        mocker.patch("time.sleep")
        request_mock = mocker.patch.object(
            invalid_credential_connection._get_flink_client(), "request"
        )
        request_mock.side_effect = [
            # PATCH (stop accepted), then a poll that finds it transitioned to FAILED.
            _ok_response(statement_response_factory(name="stmt-1", phase="STOPPING", stopped=True)),
            _ok_response(statement_response_factory(name="stmt-1", phase="FAILED", stopped=True)),
        ]

        with pytest.raises(OperationalError, match="stmt-1"):
            invalid_credential_connection.stop_statement("stmt-1", wait_for_stopped=True)

    def test_closed_connection_raises(self, invalid_credential_connection: Connection):
        """Stopping via a closed connection raises InterfaceError."""
        invalid_credential_connection.close()
        with pytest.raises(InterfaceError, match="Connection is closed"):
            invalid_credential_connection.stop_statement("stmt-1", wait_for_stopped=False)


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

    def test_get_next_page_token_empty_string_returns_none(
        self, invalid_credential_connection: Connection
    ):
        """An empty page_token (`?page_token=`) must read as None, not "". list_statements'
        pagination loop terminates on `next_page_token is not None`; an empty string would keep
        that guard True while `if next_page_token:` never sets the token, spinning forever."""
        url = "https://api.confluent.cloud/statements?label_selector=foo&page_token="
        assert invalid_credential_connection._get_next_page_token(url) is None


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
                environment_id="env-id",
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
                environment_id="env-id",
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
                environment_id="foo_id",
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

    def test_requires_environment_id(self, connection_factory: ConnectionFactory):
        """Test that creating a connection without an environment ID raises an error."""
        with pytest.raises(InterfaceError, match="Environment ID is required"):
            connection_factory(environment_id="")

    @pytest.mark.parametrize(
        "compute_pool_id",
        [
            None,
            "",
        ],
    )
    def test_compute_pool_id_optional(self, compute_pool_id: str | None):
        """A connection may be created without or a blank compute pool; Flink will use the default
        compute pool for the env+cloud-region."""
        connection = connect(
            environment_id="foo_id",
            organization_id="org-id",
            cloud_provider="aws",
            cloud_region="us-east-1",
            flink_api_key="valid-key",
            flink_api_secret="valid-secret",
            compute_pool_id=compute_pool_id,
        )
        # Should end up None either way.
        assert connection.compute_pool_id is None

    def test_connection_constructible_without_compute_pool(self):
        """Connection() is directly constructible without a pool -- optional end-to-end (#108).

        Direct Connection instantiation must not force callers to pass compute_pool_id=None;
        omitting it leaves the connection poolless.
        """
        connection = Connection(
            flink_api_key="valid-key",
            flink_api_secret="valid-secret",
            environment_id="env-id",
            organization_id="org-id",
            cloud_provider="aws",
            cloud_region="us-east-1",
            endpoint=None,
        )
        assert connection.compute_pool_id is None

    def test_requires_organization_id(self, connection_factory: ConnectionFactory):
        """A Flink-only key still requires organization_id (#132 regression guard): the
        relaxation for a global key must not leak into this path. global_api_key/secret are
        pinned empty so a CONFLUENT_GLOBAL_API_KEY env var can't smuggle real global creds in
        and mask the raise (see test_requires_some_api_credentials for the same concern).
        """
        with pytest.raises(InterfaceError, match="Organization ID is required"):
            connection_factory(
                environment_id="foo_id",
                compute_pool_id="1234",
                organization_id="",
                global_api_key="",
                global_api_secret="",
            )

    def test_requires_organization_id_for_dedicated_tableflow_key(
        self, connection_factory: ConnectionFactory
    ):
        """A dedicated Tableflow key (no global key) still requires organization_id (#132
        regression guard): it has no /org/v2 reach, so the relaxation must not apply to it."""
        with pytest.raises(InterfaceError, match="Organization ID is required"):
            connection_factory(
                environment_id="foo_id",
                compute_pool_id="1234",
                organization_id="",
                global_api_key="",
                global_api_secret="",
                flink_api_key="",
                flink_api_secret="",
                tableflow_api_key="tk",
                tableflow_api_secret="ts",
            )

    def test_half_supplied_global_pair_with_omitted_org_reports_credential_error(
        self, connection_factory: ConnectionFactory
    ):
        """A half-supplied global pair (key without secret) plus an omitted organization_id must
        surface the specific credential-pairing error, not the generic "Organization ID is
        required" -- the org-omission gate must not treat a half-supplied global key as "no
        global key attempted" and mask the more accurate diagnosis (caught by Copilot on #144)."""
        with pytest.raises(
            InterfaceError, match="global_api_key and global_api_secret must be provided together"
        ):
            connection_factory(
                environment_id="foo_id",
                organization_id="",
                cloud_provider="aws",
                cloud_region="us-east-1",
                global_api_key="half-key",
                global_api_secret="",
                flink_api_key="",
                flink_api_secret="",
            )

    def test_construction_does_not_resolve_organization_id(self, mocker):
        """Global key + omitted organization_id: connect() must not make the /org/v2 network
        call -- inference is deferred to first use of the connection (#132)."""
        mock_lookup = mocker.patch.object(
            Connection,
            "_controlplane_request",
            Mock(side_effect=AssertionError("must not be called during construction")),
        )
        connect(
            environment_id="env-id",
            organization_id="",
            cloud_provider="aws",
            cloud_region="us-east-1",
            global_api_key="global-key",
            global_api_secret="global-secret",
        )
        mock_lookup.assert_not_called()

    def test_infers_organization_id_from_global_key_on_first_use(self, mocker):
        """Global key + omitted organization_id: the org is inferred on first use, and both the
        Flink client's base_url and the statement-create payload reflect the discovered org."""
        mock_response = mocker.Mock()
        mock_response.json.return_value = {"data": [{"id": "org-99"}], "metadata": {}}
        mocker.patch.object(Connection, "_controlplane_request", return_value=mock_response)

        conn = connect(
            environment_id="env-id",
            organization_id="",
            cloud_provider="aws",
            cloud_region="us-east-1",
            global_api_key="global-key",
            global_api_secret="global-secret",
        )

        assert conn.organization_id == "org-99"
        assert "/organizations/org-99/" in str(conn._get_flink_client().base_url)

        statement_response = mocker.Mock()
        statement_response.json.return_value = {"name": "test-statement", "spec": {}}
        request_mock = mocker.patch.object(
            conn._get_flink_client(), "request", return_value=statement_response
        )
        conn._execute_statement("SELECT 1", ExecutionMode.SNAPSHOT)
        payload = request_mock.call_args.kwargs["json"]
        assert payload["organization_id"] == "org-99"

    def test_organization_id_resolved_once_and_cached(self, mocker):
        """The /org/v2 lookup fires at most once per connection, even across repeated access."""
        mock_response = mocker.Mock()
        mock_response.json.return_value = {"data": [{"id": "org-99"}], "metadata": {}}
        mock_lookup = mocker.patch.object(
            Connection, "_controlplane_request", return_value=mock_response
        )

        conn = connect(
            environment_id="env-id",
            organization_id="",
            cloud_provider="aws",
            cloud_region="us-east-1",
            global_api_key="global-key",
            global_api_secret="global-secret",
        )

        assert conn.organization_id == "org-99"
        assert conn.organization_id == "org-99"
        conn._get_flink_client()
        mock_lookup.assert_called_once()

    def test_supplied_organization_id_skips_org_lookup(self, mocker):
        """A caller-supplied organization_id is used verbatim, with no /org/v2 call at all --
        even though a global key is present and would otherwise make inference possible."""
        mock_lookup = mocker.patch.object(
            Connection,
            "_controlplane_request",
            Mock(side_effect=AssertionError("must not be called when organization_id is supplied")),
        )
        conn = connect(
            environment_id="env-id",
            organization_id="org-explicit",
            cloud_provider="aws",
            cloud_region="us-east-1",
            global_api_key="global-key",
            global_api_secret="global-secret",
        )

        assert conn.organization_id == "org-explicit"
        conn._get_flink_client()
        mock_lookup.assert_not_called()

    def test_requires_cloud_provider(self, connection_factory: ConnectionFactory):
        """Test that cloud provider is required when endpoint is not provided."""
        with pytest.raises(
            InterfaceError, match="Cloud provider is required when endpoint is not provided"
        ):
            connection_factory(
                environment_id="foo_id",
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
                environment_id="foo_id",
                compute_pool_id="1234",
                organization_id="4567",
                cloud_provider="aws",
                cloud_region="",
            )

    def test_builds_endpoint_from_cloud_info(self, connection_factory: ConnectionFactory):
        """Test that when endpoint is not provided, it is built correctly from cloud info."""
        conn = connection_factory(
            environment_id="env-123",
            organization_id="org-456",
            compute_pool_id="cp-789",
            cloud_provider="aws",
            cloud_region="us-east-1",
            flink_api_key="test-key",
            flink_api_secret="test-secret",
        )

        expected_base_url = "https://flink.us-east-1.aws.confluent.cloud/sql/v1/organizations/org-456/environments/env-123/"
        assert str(conn._get_flink_client().base_url) == expected_base_url

    def test_requires_some_api_credentials(self, connection_factory: ConnectionFactory):
        """connect() with neither a global nor a Flink credential pair raises (#112)."""
        with pytest.raises(
            InterfaceError,
            match=(
                "Either global_api_key/global_api_secret or "
                "flink_api_key/flink_api_secret must be provided"
            ),
        ):
            connection_factory(
                environment_id="foo_id",
                compute_pool_id="1234",
                organization_id="4567",
                cloud_provider="aws",
                cloud_region="us-east-1",
                # Pin every credential empty so the factory's CONFLUENT_GLOBAL_API_KEY/SECRET
                # env-var fallback can't smuggle real credentials in and mask the raise.
                global_api_key="",
                global_api_secret="",
                flink_api_key="",
                flink_api_secret="",
            )

    def test_half_flink_pair_raises_through_connect(
        self, connection_factory: ConnectionFactory
    ):
        """A lone flink_api_key (no secret) raises the half-pair error end-to-end (#112)."""
        with pytest.raises(
            InterfaceError,
            match="flink_api_key and flink_api_secret must be provided together",
        ):
            connection_factory(
                environment_id="foo_id",
                compute_pool_id="1234",
                organization_id="4567",
                cloud_provider="aws",
                cloud_region="us-east-1",
                # Pin global empty so an env-var fallback can't resolve real global creds and
                # short-circuit the half-pair check regardless of helper ordering.
                global_api_key="",
                global_api_secret="",
                flink_api_key="valid-key",
                flink_api_secret="",
            )

    def test_endpoint_parameter_uses_custom_endpoint(self, connection_factory: ConnectionFactory):
        """Test that providing endpoint parameter uses the custom endpoint."""
        conn = connection_factory(
            environment_id="env-123",
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
        assert str(conn._get_flink_client().base_url) == expected_base_url

    def test_endpoint_parameter_with_trailing_slash(self, connection_factory: ConnectionFactory):
        """Test that endpoint parameter with trailing slash is stripped correctly."""
        conn = connection_factory(
            environment_id="env-123",
            organization_id="org-456",
            compute_pool_id="cp-789",
            flink_api_key="test-key",
            flink_api_secret="test-secret",
            endpoint="https://custom.example.com/",
        )

        # Verify trailing slash was stripped and URL is clean (no double slashes)
        base_url = str(conn._get_flink_client().base_url)
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
            environment_id="env-123",
            organization_id="org-456",
            compute_pool_id="cp-789",
            flink_api_key="test-key",
            flink_api_secret="test-secret",
            endpoint="https://custom.example.com",
        )

        # Verify URL is clean (same result as with trailing slash)
        base_url = str(conn._get_flink_client().base_url)
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
                environment_id="env-123",
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
                environment_id="env-123",
                organization_id="org-456",
                compute_pool_id="cp-789",
                flink_api_key="test-key",
                flink_api_secret="test-secret",
                endpoint="https://custom.example.com",
                cloud_region="us-east-1",
            )


@pytest.mark.unit
class TestApiCredentialResolution:
    """Unit tests over _resolve_api_credentials -- the single source of truth for which
    key/secret pair authenticates a connection (#112)."""

    @pytest.mark.parametrize(
        ("global_api_key", "global_api_secret", "flink_api_key", "flink_api_secret", "expected"),
        [
            ("gk", "gs", None, None, ("gk", "gs")),
            (None, None, "fk", "fs", ("fk", "fs")),
            # Global is the superset, so it wins when both pairs are fully supplied.
            ("gk", "gs", "fk", "fs", ("gk", "gs")),
            # Empty strings are as good as absent.
            ("gk", "gs", "", "", ("gk", "gs")),
            ("", "", "fk", "fs", ("fk", "fs")),
        ],
    )
    def test_resolves_effective_pair(
        self, global_api_key, global_api_secret, flink_api_key, flink_api_secret, expected
    ):
        assert (
            _resolve_api_credentials(
                global_api_key, global_api_secret, flink_api_key, flink_api_secret
            )
            == expected
        )

    @pytest.mark.parametrize(
        ("global_api_key", "global_api_secret"),
        [("gk", None), ("gk", ""), (None, "gs"), ("", "gs")],
    )
    def test_half_global_pair_raises(self, global_api_key, global_api_secret):
        with pytest.raises(
            InterfaceError,
            match="global_api_key and global_api_secret must be provided together",
        ):
            _resolve_api_credentials(global_api_key, global_api_secret, None, None)

    @pytest.mark.parametrize(
        ("flink_api_key", "flink_api_secret"),
        [("fk", None), ("fk", ""), (None, "fs"), ("", "fs")],
    )
    def test_half_flink_pair_raises(self, flink_api_key, flink_api_secret):
        with pytest.raises(
            InterfaceError,
            match="flink_api_key and flink_api_secret must be provided together",
        ):
            _resolve_api_credentials(None, None, flink_api_key, flink_api_secret)

    @pytest.mark.parametrize(
        ("global_api_key", "global_api_secret", "flink_api_key", "flink_api_secret"),
        [(None, None, None, None), ("", "", "", "")],
    )
    def test_neither_pair_raises(
        self, global_api_key, global_api_secret, flink_api_key, flink_api_secret
    ):
        with pytest.raises(
            InterfaceError,
            match=(
                "Either global_api_key/global_api_secret or "
                "flink_api_key/flink_api_secret must be provided"
            ),
        ):
            _resolve_api_credentials(
                global_api_key, global_api_secret, flink_api_key, flink_api_secret
            )

    def test_both_pairs_warns_that_flink_is_ignored(self, caplog):
        """Supplying both pairs uses global and warns the flink pair is ignored -- a confused
        caller needs to see this, so it is a warning, not a debug line."""
        with caplog.at_level("WARNING", logger=connection_module_logger.name):
            resolved = _resolve_api_credentials("gk", "gs", "fk", "fs")
        assert resolved == ("gk", "gs")
        assert any(
            "ignoring flink_api_key/flink_api_secret" in record.getMessage()
            and record.levelname == "WARNING"
            for record in caplog.records
        )


@pytest.mark.unit
class TestConnectAuthWiring:
    """End-to-end proof that the resolved pair is what gets handed to httpx for auth (#112)."""

    @staticmethod
    def _connect_spying_on_basic_auth(
        *,
        global_api_key: str | None = None,
        global_api_secret: str | None = None,
        flink_api_key: str | None = None,
        flink_api_secret: str | None = None,
    ) -> Mock:
        """connect() with the given credential kwargs, returning the spy on httpx.BasicAuth so the
        caller can assert exactly which username/password the client was wired with."""
        with patch(
            "confluent_sql.connection.httpx.BasicAuth", wraps=httpx.BasicAuth
        ) as basic_auth_spy:
            connect(
                environment_id="env-id",
                organization_id="org-id",
                cloud_provider="aws",
                cloud_region="us-east-1",
                global_api_key=global_api_key,
                global_api_secret=global_api_secret,
                flink_api_key=flink_api_key,
                flink_api_secret=flink_api_secret,
            )
        return basic_auth_spy

    def test_global_only_authenticates_with_global_creds(self):
        spy = self._connect_spying_on_basic_auth(
            global_api_key="global-key", global_api_secret="global-secret"
        )
        spy.assert_called_once_with(username="global-key", password="global-secret")

    def test_flink_only_authenticates_with_flink_creds(self):
        spy = self._connect_spying_on_basic_auth(
            flink_api_key="flink-key", flink_api_secret="flink-secret"
        )
        spy.assert_called_once_with(username="flink-key", password="flink-secret")

    def test_both_pairs_authenticate_with_global_creds(self):
        spy = self._connect_spying_on_basic_auth(
            global_api_key="global-key",
            global_api_secret="global-secret",
            flink_api_key="flink-key",
            flink_api_secret="flink-secret",
        )
        spy.assert_called_once_with(username="global-key", password="global-secret")


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
            invalid_credential_connection._get_flink_client(), "request", return_value=mock_response
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
            invalid_credential_connection._get_flink_client(),
            "request",
            side_effect=capture_request,
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
            invalid_credential_connection._get_flink_client(), "request", return_value=mock_response
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
            invalid_credential_connection._get_flink_client(), "request", return_value=mock_response
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
            invalid_credential_connection._get_flink_client(), "request", return_value=mock_response
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
            invalid_credential_connection._get_flink_client(), "request", return_value=mock_response
        )

        # Call list_statements with label that already has prefix
        invalid_credential_connection.list_statements(label="user.confluent.io/already-prefixed")

        # Verify label_selector is not double-prefixed
        call_params = request_mock.call_args[1]["params"]
        assert call_params["label_selector"] == "user.confluent.io/already-prefixed=true"

    def test_list_statements_no_filters_lists_all(
        self,
        invalid_credential_connection: Connection,
        statement_response_factory: StatementResponseFactory,
        mocker,
    ):
        """With no filters, list_statements sends only page_size and returns everything."""
        mock_response = Mock()
        mock_response.json.return_value = {
            "data": [statement_response_factory(name="stmt-1")],
            "metadata": {},
        }

        request_mock = mocker.patch.object(
            invalid_credential_connection._get_flink_client(), "request", return_value=mock_response
        )

        statements = invalid_credential_connection.list_statements()

        # No label_selector, no compute pool filter -- just pagination.
        request_mock.assert_called_once_with(
            "GET",
            "/statements",
            params={"page_size": 100},
        )
        assert [s.name for s in statements] == ["stmt-1"]

    def test_list_statements_compute_pool_only(
        self,
        invalid_credential_connection: Connection,
        statement_response_factory: StatementResponseFactory,
        mocker,
    ):
        """compute_pool_id is sent server-side as the spec.compute_pool_id query param."""
        mock_response = Mock()
        mock_response.json.return_value = {
            "data": [statement_response_factory()],
            "metadata": {},
        }

        request_mock = mocker.patch.object(
            invalid_credential_connection._get_flink_client(), "request", return_value=mock_response
        )

        invalid_credential_connection.list_statements(compute_pool_id="lfcp-xyz")

        request_mock.assert_called_once_with(
            "GET",
            "/statements",
            params={"page_size": 100, "spec.compute_pool_id": "lfcp-xyz"},
        )

    def test_list_statements_label_and_compute_pool_combined(
        self,
        invalid_credential_connection: Connection,
        statement_response_factory: StatementResponseFactory,
        mocker,
    ):
        """label and compute_pool_id both narrow server-side, in one request."""
        mock_response = Mock()
        mock_response.json.return_value = {
            "data": [statement_response_factory()],
            "metadata": {},
        }

        request_mock = mocker.patch.object(
            invalid_credential_connection._get_flink_client(), "request", return_value=mock_response
        )

        invalid_credential_connection.list_statements(
            label="daily-report", compute_pool_id="lfcp-xyz"
        )

        request_mock.assert_called_once_with(
            "GET",
            "/statements",
            params={
                "page_size": 100,
                "label_selector": "user.confluent.io/daily-report=true",
                "spec.compute_pool_id": "lfcp-xyz",
            },
        )

    def test_list_statements_name_contains_sent_server_side(
        self,
        invalid_credential_connection: Connection,
        statement_response_factory: StatementResponseFactory,
        mocker,
    ):
        """name_contains is forwarded verbatim as the statement_name_search_query query param,
        accompanied by time_ordered=true (the server ignores the search term without it). The
        server does the substring matching, so fetched results are returned as-is."""
        mock_response = Mock()
        mock_response.json.return_value = {
            "data": [
                statement_response_factory(name="report-a"),
                statement_response_factory(name="daily-report"),
            ],
            "metadata": {},
        }

        request_mock = mocker.patch.object(
            invalid_credential_connection._get_flink_client(), "request", return_value=mock_response
        )

        statements = invalid_credential_connection.list_statements(name_contains="report")

        request_mock.assert_called_once_with(
            "GET",
            "/statements",
            params={
                "page_size": 100,
                "statement_name_search_query": "report",
                "time_ordered": "true",
            },
        )
        # Whatever the server returns is returned verbatim -- no client-side re-filtering.
        assert [s.name for s in statements] == ["report-a", "daily-report"]

    def test_list_statements_all_three_filters_combined(
        self,
        invalid_credential_connection: Connection,
        statement_response_factory: StatementResponseFactory,
        mocker,
    ):
        """label, compute_pool_id, and name_contains all narrow server-side in one request."""
        mock_response = Mock()
        mock_response.json.return_value = {
            "data": [statement_response_factory(name="report-a")],
            "metadata": {},
        }

        request_mock = mocker.patch.object(
            invalid_credential_connection._get_flink_client(), "request", return_value=mock_response
        )

        statements = invalid_credential_connection.list_statements(
            label="daily-report",
            compute_pool_id="lfcp-xyz",
            name_contains="report",
        )

        request_mock.assert_called_once_with(
            "GET",
            "/statements",
            params={
                "page_size": 100,
                "label_selector": "user.confluent.io/daily-report=true",
                "spec.compute_pool_id": "lfcp-xyz",
                "statement_name_search_query": "report",
                "time_ordered": "true",
            },
        )
        assert [s.name for s in statements] == ["report-a"]


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
        request_mock = mocker.patch.object(
            invalid_credential_connection._get_flink_client(), "request"
        )
        response_mock = Mock()
        response_mock.status_code = 404
        response_mock.json.return_value = {"errors": [{"detail": "Statement not found"}]}

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
        request_mock = mocker.patch.object(
            invalid_credential_connection._get_flink_client(), "request"
        )
        response_mock = Mock()
        response_mock.status_code = 500
        response_mock.json.return_value = {"errors": [{"detail": "Internal server error"}]}

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
class TestConnectionRetriesIdempotentGets:
    """Tests that idempotent GET paths retry transient transport errors (#137), while
    mutating (POST/PATCH/DELETE) paths deliberately do not."""

    def test_list_statements_retries_and_succeeds(
        self,
        invalid_credential_connection: Connection,
        statement_response_factory: StatementResponseFactory,
        mocker,
    ):
        mocker.patch("confluent_sql.retry.time.sleep")
        mock_response = Mock()
        mock_response.json.return_value = {
            "data": [statement_response_factory(name="stmt-1")],
            "metadata": {},
        }
        request_mock = mocker.patch.object(
            invalid_credential_connection._get_flink_client(),
            "request",
            side_effect=[httpx.ReadError("reset"), mock_response],
        )

        statements = invalid_credential_connection.list_statements()

        assert [s.name for s in statements] == ["stmt-1"]
        assert request_mock.call_count == 2

    def test_get_statement_retries_and_succeeds(
        self,
        invalid_credential_connection: Connection,
        statement_response_factory: StatementResponseFactory,
        mocker,
    ):
        mocker.patch("confluent_sql.retry.time.sleep")
        mock_response = Mock()
        mock_response.json.return_value = statement_response_factory(name="stmt-1")
        request_mock = mocker.patch.object(
            invalid_credential_connection._get_flink_client(),
            "request",
            side_effect=[httpx.ReadError("reset"), mock_response],
        )

        result = invalid_credential_connection._get_statement("stmt-1")

        assert result["name"] == "stmt-1"
        assert request_mock.call_count == 2

    def test_get_statement_results_retries_and_succeeds(
        self,
        invalid_credential_connection: Connection,
        mocker,
    ):
        mocker.patch("confluent_sql.retry.time.sleep")
        mock_response = Mock()
        mock_response.json.return_value = {
            "results": {"data": [{"row": ["v1"]}]},
            "metadata": {},
        }
        request_mock = mocker.patch.object(
            invalid_credential_connection._get_flink_client(),
            "request",
            side_effect=[httpx.ReadError("reset"), mock_response],
        )

        results, next_url = invalid_credential_connection._get_statement_results("stmt-1", None)

        assert [r.row for r in results] == [["v1"]]
        assert next_url is None
        assert request_mock.call_count == 2

    def test_idempotent_get_reraises_original_exception_after_exhausting_retries(
        self,
        invalid_credential_connection: Connection,
        mocker,
    ):
        """Documents today's unwrapped-transport-error behavior (tracked as #138): after retries
        are exhausted, the raw httpx exception propagates rather than an OperationalError."""
        mocker.patch("confluent_sql.retry.time.sleep")
        request_mock = mocker.patch.object(
            invalid_credential_connection._get_flink_client(),
            "request",
            side_effect=[httpx.ReadError("reset")] * 4,
        )

        with pytest.raises(httpx.ReadError):
            invalid_credential_connection._get_statement("stmt-1")

        assert request_mock.call_count == 4

    def test_non_idempotent_path_does_not_retry_on_transient_error(
        self,
        invalid_credential_connection: Connection,
        mocker,
    ):
        """delete_statement's DELETE must not be retried: retrying a mutating call after a
        connection reset could double-mutate state, which is exactly what #137 scopes retries
        away from."""
        request_mock = mocker.patch.object(
            invalid_credential_connection._get_flink_client(),
            "request",
            side_effect=httpx.ReadError("reset"),
        )

        with pytest.raises(httpx.ReadError):
            invalid_credential_connection.delete_statement("stmt-1")

        request_mock.assert_called_once()

    def test_list_statements_retries_on_retryable_status_and_succeeds(
        self,
        invalid_credential_connection: Connection,
        statement_response_factory: StatementResponseFactory,
        mocker,
    ):
        mocker.patch("confluent_sql.retry.time.sleep")
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": [statement_response_factory(name="stmt-1")],
            "metadata": {},
        }
        request_mock = mocker.patch.object(
            invalid_credential_connection._get_flink_client(),
            "request",
            side_effect=[_http_error_response(503), mock_response],
        )

        statements = invalid_credential_connection.list_statements()

        assert [s.name for s in statements] == ["stmt-1"]
        assert request_mock.call_count == 2

    def test_get_statement_retries_on_retryable_status_and_succeeds(
        self,
        invalid_credential_connection: Connection,
        statement_response_factory: StatementResponseFactory,
        mocker,
    ):
        mocker.patch("confluent_sql.retry.time.sleep")
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = statement_response_factory(name="stmt-1")
        request_mock = mocker.patch.object(
            invalid_credential_connection._get_flink_client(),
            "request",
            side_effect=[_http_error_response(503), mock_response],
        )

        result = invalid_credential_connection._get_statement("stmt-1")

        assert result["name"] == "stmt-1"
        assert request_mock.call_count == 2

    def test_get_statement_results_retries_on_retryable_status_and_succeeds(
        self,
        invalid_credential_connection: Connection,
        mocker,
    ):
        mocker.patch("confluent_sql.retry.time.sleep")
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "results": {"data": [{"row": ["v1"]}]},
            "metadata": {},
        }
        request_mock = mocker.patch.object(
            invalid_credential_connection._get_flink_client(),
            "request",
            side_effect=[_http_error_response(503), mock_response],
        )

        results, next_url = invalid_credential_connection._get_statement_results("stmt-1", None)

        assert [r.row for r in results] == [["v1"]]
        assert next_url is None
        assert request_mock.call_count == 2

    def test_idempotent_get_raises_operational_error_after_exhausting_status_retries(
        self,
        invalid_credential_connection: Connection,
        mocker,
    ):
        """Unlike #137's transport-exception exhaustion (which leaks the raw httpx exception,
        tracked as #138), a persistently-retryable status IS translated to OperationalError --
        _raise_for_status_as_operational_error runs on the final response either way."""
        mocker.patch("confluent_sql.retry.time.sleep")
        request_mock = mocker.patch.object(
            invalid_credential_connection._get_flink_client(),
            "request",
            side_effect=[_http_error_response(503)] * 4,
        )

        with pytest.raises(OperationalError) as exc_info:
            invalid_credential_connection._get_statement("stmt-1")

        assert exc_info.value.http_status_code == 503
        assert request_mock.call_count == 4

    def test_get_statement_does_not_retry_non_retryable_status(
        self,
        invalid_credential_connection: Connection,
        mocker,
    ):
        """A 404 must not be treated as retryable -- it's the caller's job to see
        StatementNotFoundError on the very first response, not after a wasted retry budget."""
        request_mock = mocker.patch.object(
            invalid_credential_connection._get_flink_client(),
            "request",
            return_value=_http_error_response(404),
        )

        with pytest.raises(StatementNotFoundError):
            invalid_credential_connection._get_statement("stmt-1")

        request_mock.assert_called_once()

    def test_get_statement_retries_share_budget_across_transient_error_and_status(
        self,
        invalid_credential_connection: Connection,
        statement_response_factory: StatementResponseFactory,
        mocker,
    ):
        """A transport exception and a retryable status draw from the same attempt/backoff
        budget -- proof that #140's status-code retries reuse #137's call_with_retries loop
        rather than adding a second, independent retry mechanism."""
        mocker.patch("confluent_sql.retry.time.sleep")
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = statement_response_factory(name="stmt-1")
        request_mock = mocker.patch.object(
            invalid_credential_connection._get_flink_client(),
            "request",
            side_effect=[httpx.ReadError("reset"), _http_error_response(503), mock_response],
        )

        result = invalid_credential_connection._get_statement("stmt-1")

        assert result["name"] == "stmt-1"
        assert request_mock.call_count == 3

    def test_delete_statement_does_not_retry_on_retryable_status(
        self,
        invalid_credential_connection: Connection,
        mocker,
    ):
        """delete_statement's DELETE must not be retried on a retryable status either -- #140's
        status-code check must live in _request_get, not leak into _request itself."""
        request_mock = mocker.patch.object(
            invalid_credential_connection._get_flink_client(),
            "request",
            return_value=_http_error_response(503),
        )

        with pytest.raises(OperationalError, match="Error deleting statement"):
            invalid_credential_connection.delete_statement("stmt-1")

        request_mock.assert_called_once()

    def test_request_get_forwards_arbitrary_kwargs_to_request(
        self,
        invalid_credential_connection: Connection,
        mocker,
    ):
        """A future GET call site needing e.g. headers/timeout must not have to bypass
        _request_get (and thus the retry policy) just because _request_get only knew about
        `params`."""
        request_mock = mocker.patch.object(
            invalid_credential_connection._get_flink_client(), "request", return_value=Mock()
        )

        invalid_credential_connection._request_get(
            "/statements", headers={"X-Test": "1"}, timeout=5
        )

        request_mock.assert_called_once_with(
            "GET", "/statements", headers={"X-Test": "1"}, timeout=5
        )

    def test_request_get_forces_get_method_even_if_caller_passes_method(
        self,
        invalid_credential_connection: Connection,
        mocker,
    ):
        """_request_get must never issue anything but a GET, even if a caller mistakenly
        passes a method= kwarg -- that guarantee is what makes it safe to retry."""
        request_mock = mocker.patch.object(
            invalid_credential_connection._get_flink_client(), "request", return_value=Mock()
        )

        invalid_credential_connection._request_get("/statements", method="POST")

        request_mock.assert_called_once_with("GET", "/statements")

    def test_request_get_drops_caller_supplied_raise_for_status_kwarg(
        self,
        invalid_credential_connection: Connection,
        mocker,
    ):
        """_request_get always manages raise_for_status itself (it must see the raw response
        to decide whether to retry before translating it) -- a caller passing raise_for_status
        in kwargs must not collide with that and blow up with a duplicate-keyword TypeError."""
        request_mock = mocker.patch.object(
            invalid_credential_connection._get_flink_client(), "request", return_value=Mock()
        )

        invalid_credential_connection._request_get("/statements", raise_for_status=True)

        request_mock.assert_called_once_with("GET", "/statements")


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

        return mocker.patch.object(
            connection._get_flink_client(), "request", return_value=mock_response
        )

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
            environment_id="test-env",
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
        client = invalid_credential_connection._get_flink_client()
        assert client.headers.get("User-Agent") == expected

    def test_custom_user_agent_via_constructor(
        self, http_agent_connection_factory: ConnectionFactory
    ):
        """Test that a custom user agent can be set via constructor."""
        custom_agent = "my-app/1.0.0"
        conn = http_agent_connection_factory(http_user_agent=custom_agent)
        assert conn.http_user_agent == custom_agent
        # Verify the header is applied to the httpx client at construction time
        assert conn._get_flink_client().headers.get("User-Agent") == custom_agent

    def test_set_user_agent_via_property(self, invalid_credential_connection: Connection):
        """Test that user agent can be set via property setter."""
        new_agent = "updated-app/2.0"
        invalid_credential_connection.http_user_agent = new_agent
        assert invalid_credential_connection.http_user_agent == new_agent
        # Verify the header is updated in the httpx client when property is set
        client = invalid_credential_connection._get_flink_client()
        assert client.headers.get("User-Agent") == new_agent

    def test_set_user_agent_updates_already_built_client_in_place(
        self, invalid_credential_connection: Connection
    ):
        """Setting http_user_agent after the (lazily-built) Flink client already exists must
        update that same client's headers live, not just influence a future build."""
        client = invalid_credential_connection._get_flink_client()  # force lazy build now
        new_agent = "updated-app/3.0"
        invalid_credential_connection.http_user_agent = new_agent
        assert invalid_credential_connection._get_flink_client() is client
        assert client.headers.get("User-Agent") == new_agent

    def test_set_user_agent_updates_already_built_controlplane_clients_in_place(self):
        """Setting http_user_agent must also update the Tableflow and Connect control-plane
        clients' headers if they've already been lazily built -- not just the Flink client
        (caught by Copilot on #144: the docstring promises "all HTTP requests")."""
        conn = connect(
            environment_id="env-id",
            organization_id="org-id",
            cloud_provider="aws",
            cloud_region="us-east-1",
            flink_api_key="flink-key",
            flink_api_secret="flink-secret",
            tableflow_api_key="tableflow-key",
            tableflow_api_secret="tableflow-secret",
            connect_api_key="connect-key",
            connect_api_secret="connect-secret",
        )
        controlplane_client = conn._get_controlplane_client()  # force lazy build now
        connect_client = conn._get_connect_controlplane_client()  # force lazy build now

        new_agent = "updated-app/4.0"
        conn.http_user_agent = new_agent

        assert controlplane_client.headers.get("User-Agent") == new_agent
        assert connect_client.headers.get("User-Agent") == new_agent

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


@pytest.mark.unit
class TestHttpTimeoutSecs:
    """Tests for the http_timeout_secs constructor parameter and property."""

    def test_default_reports_confluent_sql_default(
        self, invalid_credential_connection: Connection
    ):
        """When not provided, the property reports the effective default, not None."""
        assert invalid_credential_connection.http_timeout_secs == DEFAULT_HTTP_TIMEOUT_SECS
        assert invalid_credential_connection._get_flink_client().timeout == httpx.Timeout(
            DEFAULT_HTTP_TIMEOUT_SECS
        )

    def test_default_http_timeout_secs_is_ten_seconds(self):
        """Pin the exact default value so a silent change here doesn't slip by unnoticed."""
        assert DEFAULT_HTTP_TIMEOUT_SECS == 10.0

    @pytest.mark.parametrize("timeout_value", [0.5, 1, 10, 30.0, 120])
    def test_custom_timeout_via_constructor(
        self, connection_factory: ConnectionFactory, timeout_value
    ):
        """A custom timeout is stored on the connection and applied to the httpx client."""
        conn = connection_factory(
            environment_id="env-id",
            organization_id="org-id",
            compute_pool_id="cp-id",
            cloud_provider="aws",
            cloud_region="us-east-1",
            flink_api_key="key",
            flink_api_secret="secret",
            http_timeout_secs=timeout_value,
        )
        assert conn.http_timeout_secs == timeout_value
        # httpx wraps the timeout into a Timeout(connect, read, write, pool) object.
        assert conn._get_flink_client().timeout == httpx.Timeout(timeout_value)

    @pytest.mark.parametrize(
        "invalid_value,expected_error",
        [
            (0, "http_timeout_secs must be positive, got 0"),
            (-1, "http_timeout_secs must be positive, got -1"),
            (-0.5, "http_timeout_secs must be positive, got -0.5"),
        ],
    )
    def test_rejects_non_positive(
        self,
        connection_factory: ConnectionFactory,
        invalid_value,
        expected_error,
    ):
        """Zero and negative values are rejected."""
        with pytest.raises(InterfaceError, match=expected_error):
            connection_factory(
                environment_id="env-id",
                organization_id="org-id",
                compute_pool_id="cp-id",
                cloud_provider="aws",
                cloud_region="us-east-1",
                flink_api_key="key",
                flink_api_secret="secret",
                http_timeout_secs=invalid_value,
            )

    @pytest.mark.parametrize(
        "invalid_value,expected_error",
        [
            ("5", "http_timeout_secs must be a number, got str"),
            ([5], "http_timeout_secs must be a number, got list"),
            ({"secs": 5}, "http_timeout_secs must be a number, got dict"),
            (True, "http_timeout_secs must be a number, got bool"),
            (False, "http_timeout_secs must be a number, got bool"),
        ],
    )
    def test_rejects_non_numeric(
        self,
        connection_factory: ConnectionFactory,
        invalid_value,
        expected_error,
    ):
        """Non-numeric types (including bool) are rejected."""
        with pytest.raises(InterfaceError, match=expected_error):
            connection_factory(
                environment_id="env-id",
                organization_id="org-id",
                compute_pool_id="cp-id",
                cloud_provider="aws",
                cloud_region="us-east-1",
                flink_api_key="key",
                flink_api_secret="secret",
                http_timeout_secs=invalid_value,
            )


@pytest.mark.unit
class TestComputePoolIdParameter:
    """Test the compute_pool_id parameter for Connection methods."""

    def test_execute_statement_with_compute_pool_id(
        self, invalid_credential_connection, mocker
    ):
        """Test that _execute_statement uses provided compute_pool_id and logs override."""
        # Mock the HTTP request
        mock_response = mocker.Mock()
        mock_response.json.return_value = {
            "name": "test-statement",
            "spec": {"compute_pool_id": "lfcp-custom-pool"},
        }
        mocker.patch.object(
            invalid_credential_connection._get_flink_client(),
            'request',
            return_value=mock_response
        )

        # Patch logger.info to verify override message
        mock_log_info = mocker.patch("confluent_sql.connection.logger.info")

        # Call _execute_statement with a custom compute_pool_id
        invalid_credential_connection._execute_statement(
            "SELECT 1",
            ExecutionMode.SNAPSHOT,
            compute_pool_id="lfcp-custom-pool"
        )

        # Verify the request was made
        invalid_credential_connection._get_flink_client().request.assert_called_once()
        call_kwargs = invalid_credential_connection._get_flink_client().request.call_args
        payload = call_kwargs.kwargs['json']

        # Verify the compute_pool_id in the payload matches the provided one
        assert payload['spec']['compute_pool_id'] == "lfcp-custom-pool"

        # Verify logger.info was called with the override message
        assert mock_log_info.called, "Expected logger.info to be called"
        log_messages = [str(call) for call in mock_log_info.call_args_list]
        assert any(
            "Overriding connection compute_pool_id" in msg
            and "lfcp-custom-pool" in msg
            for msg in log_messages
        ), f"Expected override log message not found in: {log_messages}"

    def test_execute_statement_defaults_to_connection_pool(
        self, invalid_credential_connection, mocker
    ):
        """Test that _execute_statement uses Connection's pool when not provided."""
        # Mock the HTTP request
        mock_response = mocker.Mock()
        mock_response.json.return_value = {
            "name": "test-statement",
            "spec": {"compute_pool_id": "cp-id"},
        }
        mocker.patch.object(
            invalid_credential_connection._get_flink_client(),
            'request',
            return_value=mock_response
        )

        # Call _execute_statement without compute_pool_id
        invalid_credential_connection._execute_statement(
            "SELECT 1",
            ExecutionMode.SNAPSHOT
        )

        # Verify the payload uses the connection's compute_pool_id
        call_kwargs = invalid_credential_connection._get_flink_client().request.call_args
        payload = call_kwargs.kwargs['json']
        assert payload['spec']['compute_pool_id'] == "cp-id"  # from fixture

    def test_execute_statement_omits_pool_when_none_resolved(
        self, poolless_connection, mocker
    ):
        """With no per-call argument and no connection default, spec carries no pool."""
        mock_response = mocker.Mock()
        mock_response.json.return_value = {"name": "test-statement", "spec": {}}
        mocker.patch.object(
            poolless_connection._get_flink_client(), "request", return_value=mock_response
        )
        mock_log_info = mocker.patch("confluent_sql.connection.logger.info")

        poolless_connection._execute_statement("SELECT 1", ExecutionMode.SNAPSHOT)

        payload = poolless_connection._get_flink_client().request.call_args.kwargs["json"]
        assert "compute_pool_id" not in payload["spec"]

        # No connection default means there is nothing to "override" -- stay silent.
        log_messages = [str(call) for call in mock_log_info.call_args_list]
        assert not any(
            "Overriding connection compute_pool_id" in msg for msg in log_messages
        ), f"Override log should not fire without a connection default: {log_messages}"

    def test_execute_statement_per_call_pool_without_connection_default(
        self, poolless_connection, mocker
    ):
        """A per-call pool is used even with no connection default, without an override log."""
        mock_response = mocker.Mock()
        mock_response.json.return_value = {
            "name": "test-statement",
            "spec": {"compute_pool_id": "lfcp-explicit"},
        }
        mocker.patch.object(
            poolless_connection._get_flink_client(), "request", return_value=mock_response
        )
        mock_log_info = mocker.patch("confluent_sql.connection.logger.info")

        poolless_connection._execute_statement(
            "SELECT 1", ExecutionMode.SNAPSHOT, compute_pool_id="lfcp-explicit"
        )

        payload = poolless_connection._get_flink_client().request.call_args.kwargs["json"]
        assert payload["spec"]["compute_pool_id"] == "lfcp-explicit"

        log_messages = [str(call) for call in mock_log_info.call_args_list]
        assert not any(
            "Overriding connection compute_pool_id" in msg for msg in log_messages
        ), f"Override log should not fire without a connection default: {log_messages}"

    @pytest.mark.parametrize("falsy_per_call_pool", [None, ""])
    def test_execute_statement_falsy_per_call_pool_defers_to_default(
        self, invalid_credential_connection, mocker, falsy_per_call_pool
    ):
        """A falsy per-call pool defers to the connection default, silently.

        Empty string must behave identically to None -- both mean "unspecified" -- matching
        how connect() folds "" into no-pool. Neither variant overrides anything, so the
        override log must stay silent.
        """
        mock_response = mocker.Mock()
        mock_response.json.return_value = {
            "name": "test-statement",
            "spec": {"compute_pool_id": "cp-id"},
        }
        mocker.patch.object(
            invalid_credential_connection._get_flink_client(), "request", return_value=mock_response
        )
        mock_log_info = mocker.patch("confluent_sql.connection.logger.info")

        invalid_credential_connection._execute_statement(
            "SELECT 1", ExecutionMode.SNAPSHOT, compute_pool_id=falsy_per_call_pool
        )

        payload = invalid_credential_connection._get_flink_client().request.call_args.kwargs["json"]
        assert payload["spec"]["compute_pool_id"] == "cp-id"  # the connection default

        log_messages = [str(call) for call in mock_log_info.call_args_list]
        assert not any(
            "Overriding connection compute_pool_id" in msg for msg in log_messages
        ), f"Falsy per-call pool should not fire the override log: {log_messages}"

    def test_execute_statement_validates_compute_pool_type(
        self, invalid_credential_connection
    ):
        """Test that invalid compute_pool_id type raises InterfaceError."""
        with pytest.raises(InterfaceError, match="compute_pool_id must be a string"):
            invalid_credential_connection._execute_statement(
                "SELECT 1",
                ExecutionMode.SNAPSHOT,
                compute_pool_id=12345  # Invalid: int instead of str
            )

    def test_execute_snapshot_ddl_with_compute_pool_id(
        self, invalid_credential_connection, mocker
    ):
        """Test that execute_snapshot_ddl passes compute_pool_id to cursor.execute."""
        # Mock the cursor and its execute method
        mock_cursor = mocker.Mock()
        mocker.patch.object(
            invalid_credential_connection,
            'closing_cursor',
            return_value=mocker.MagicMock(__enter__=mocker.Mock(return_value=mock_cursor))
        )

        invalid_credential_connection.execute_snapshot_ddl(
            "CREATE TABLE foo (id INT)",
            compute_pool_id="lfcp-custom-pool"
        )

        # Verify cursor.execute was called with compute_pool_id
        mock_cursor.execute.assert_called_once()
        call_kwargs = mock_cursor.execute.call_args.kwargs
        assert call_kwargs['compute_pool_id'] == "lfcp-custom-pool"

    def test_execute_streaming_ddl_with_compute_pool_id(
        self, invalid_credential_connection, mocker
    ):
        """Test that execute_streaming_ddl passes compute_pool_id to cursor.execute."""
        # Mock the cursor and its execute method
        mock_cursor = mocker.Mock()
        mocker.patch.object(
            invalid_credential_connection,
            'closing_cursor',
            return_value=mocker.MagicMock(__enter__=mocker.Mock(return_value=mock_cursor))
        )

        invalid_credential_connection.execute_streaming_ddl(
            "CREATE MATERIALIZED TABLE foo AS SELECT * FROM bar",
            compute_pool_id="lfcp-streaming-pool"
        )

        # Verify cursor.execute was called with compute_pool_id
        mock_cursor.execute.assert_called_once()
        call_kwargs = mock_cursor.execute.call_args.kwargs
        assert call_kwargs['compute_pool_id'] == "lfcp-streaming-pool"
