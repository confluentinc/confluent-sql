"""Pytest configuration + fixtures for unit tests."""

from __future__ import annotations

import os
import types
from collections.abc import Callable
from typing import Any, TypeAlias

import httpx
import pytest

from confluent_sql import DEFAULT_TIMEOUTS, Connection, Cursor, oauth
from confluent_sql.connection import RowTypeRegistry
from confluent_sql.statement import ChangelogRow, Op, Statement


@pytest.fixture(autouse=True)
def _reset_oauth_holder():
    """Every unit test starts and ends with a pristine process-wide OAuth holder.

    The holder is a module-level singleton by design (one interactive-OAuth identity per
    process), so a login established by one `connect(auth="oauth")` test would otherwise leak
    into every test that runs after it in the same pytest process. `discard_login()` is
    idempotent, so this is a no-op for the (overwhelming majority of) tests that never touch
    oauth mode at all.
    """
    oauth.discard_login()
    yield
    oauth.discard_login()


@pytest.fixture(autouse=True)
def _scrub_confluent_env():
    """Unit tests never see the developer's `CONFLUENT_*` integration-test environment.

    `connection_factory` fills every argument a test leaves unset from these variables, so a
    populated shell (the README tells you to export them for the integration suite) silently
    changed what a unit test did: a database name plus a global key made `connect()` infer the
    cloud via a live CMK lookup instead of raising the `InterfaceError` the test expected. A test
    that wants one of these supplies it itself (or via `monkeypatch.setenv`).

    Uses its own `MonkeyPatch` context, not the test's `monkeypatch` fixture: a test calling
    `monkeypatch.undo()` must not restore the developer's variables mid-test. Being set up before
    the test's own fixtures, it is also torn down after them.
    """
    with pytest.MonkeyPatch.context() as mp:
        for name in [name for name in os.environ if name.startswith("CONFLUENT_")]:
            mp.delenv(name)
        yield


class NetworkAttemptedInUnitTestError(AssertionError):
    """A unit test tried to send a real HTTP request to a non-loopback host."""


_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})


@pytest.fixture(autouse=True)
def _forbid_outbound_http():
    """Fail any unit test that would put a real HTTP request on the wire.

    Patched at the transport every `httpx.Client` ends up on (including the bare clients for
    artifact uploads and the OAuth provider), so no caller can route around it. Loopback is
    allowed: the OAuth callback-server tests drive a real in-process listener on 127.0.0.1. Tests
    that inject an `httpx.MockTransport` or patch `request` never reach this transport.
    """
    real_sync = httpx.HTTPTransport.handle_request
    real_async = httpx.AsyncHTTPTransport.handle_async_request

    def _check(request: httpx.Request) -> None:
        if request.url.host not in _LOOPBACK_HOSTS:
            raise NetworkAttemptedInUnitTestError(
                f"unit test attempted a real HTTP request: {request.method} {request.url}"
            )

    def sync_guard(self, request, *args, **kwargs):
        _check(request)
        return real_sync(self, request, *args, **kwargs)

    async def async_guard(self, request, *args, **kwargs):
        _check(request)
        return await real_async(self, request, *args, **kwargs)

    # Own `MonkeyPatch` context (not the test's `monkeypatch`), so a test's `monkeypatch.undo()`
    # can't remove the guard mid-test.
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(httpx.HTTPTransport, "handle_request", sync_guard)
        mp.setattr(httpx.AsyncHTTPTransport, "handle_async_request", async_guard)
        yield


ResultRowFactory: TypeAlias = Callable[[list[Any], Op | None], ChangelogRow]


@pytest.fixture
def result_row_maker() -> ResultRowFactory:
    """A fixture that returns a helper function to create result row dictionaries
    in changelog format.

    These are helpful for overriding the return value of `mock_connection._get_statement_results`.
    """

    def _maker(row_values: list[Any], op: Op | None = None) -> ChangelogRow:
        """Helper to create a ChangelogRow for mocking statement results.

        Args:
            row_values: The list of values for the row.
            op: The changelog operation code (0=INSERT, 1=DELETE, etc). Default is 0 (INSERT).
        Returns:
            A ChangelogRow instance.
        """
        return ChangelogRow(op.value if op is not None else 0, row_values)

    return _maker


StatementResponse: TypeAlias = dict[str, Any]
"""A type alias for statement v1 JSON dictionaries."""

StatementResponseFactory: TypeAlias = Callable[..., StatementResponse]
"""A factory type alias for creating statement v1 JSON dictionaries."""


def as_dry_run(response: dict[str, Any], value: Any = "true") -> dict[str, Any]:
    """Reshape a statement response like a live `sql.dry-run` POST reply: the server echoes the
    flag in spec.properties, and metadata.uid and resource_version are empty because the
    statement is never stored."""
    response["spec"]["properties"]["sql.dry-run"] = value
    response["metadata"]["uid"] = ""
    response["metadata"]["resource_version"] = ""
    return response


@pytest.fixture()
def statement_response_factory() -> StatementResponseFactory:
    """A fixture that returns a factory function to create statement v1 JSON dictionaries
    with various overrides."""

    def _factory(  # noqa: PLR0913
        *,
        sql_statement: str = "SELECT TRUE AS VALUE",
        sql_kind: str = "SELECT",
        is_bounded: bool = True,
        is_append_only: bool = True,
        phase: str = "COMPLETED",
        status_detail: str = "",
        compute_pool_id: str = "lfcp-01x6d2",
        principal: str = "u-0xw9v9p",
        schema_columns: list[dict[str, Any]] | None = None,
        null_schema: bool = False,
        name: str = "dbapi-d4c685ef-befe-4091-9548-05d5ccb52d4a",
        labels: dict[str, str] | None = None,
        stopped: bool = False,
        scaling_state: str = "OK",
        warnings: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Construct a statement v1 as from JSON dictionary."""

        if not null_schema:
            if schema_columns is None:
                # Default to a simple schema with one BOOLEAN column
                schema_columns = [{"name": "value", "type": {"nullable": False, "type": "BOOLEAN"}}]
            maybe_schema = {"columns": schema_columns}
        else:
            # Caller asked to simulate no schema at all (for as best we are aware of how Flink might
            # do it at this time).
            maybe_schema = None

        response = {
            "api_version": "sql/v1",
            "environment_id": "env-asdf",
            "kind": "Statement",
            "metadata": {
                "created_at": "2025-11-07T18:45:05.102478Z",
                "labels": {},
                "resource_version": "5",
                "self": "https://flink.us-east-1.aws.confluent.cloud/dbapi-d4c685ef-befe-4091-9548-05d5ccb52d4a",
                "uid": "77826b1d-314e-4c6a-b33c-46e879930160",
                "updated_at": "2025-11-07T18:45:05.989814Z",
            },
            "name": name,
            "organization_id": "7c210ed4-6e1e-4355-abf9-b25e25a8b25a",
            "spec": {
                "compute_pool_id": compute_pool_id,
                "execution_mode": "BATCH",
                "principal": principal,
                "properties": {
                    "sql.current-catalog": "env-d0v2k7",
                    "sql.current-database": "python_udf",
                    "sql.snapshot.mode": "now",
                },
                "statement": sql_statement,
                "stopped": stopped,
            },
            "status": {
                "detail": status_detail,
                "network_kind": "PUBLIC",
                "phase": phase,
                "scaling_status": {
                    "last_updated": "2025-11-07T18:45:05Z",
                    "scaling_state": scaling_state,
                },
                "traits": {
                    "connection_refs": [],
                    "is_append_only": is_append_only,
                    "is_bounded": is_bounded,
                    "schema": maybe_schema,
                    "sql_kind": sql_kind,
                    "upsert_columns": None,
                },
                "warnings": warnings if warnings is not None else [],
            },
        }

        if phase == "FAILED":
            # Failed statements also do not have traits.
            response["status"]["traits"] = None

        if labels is not None:
            response["metadata"]["labels"] = labels

        return response

    return _factory


@pytest.fixture()
def statement_factory(
    mock_connection: Connection,
    statement_response_factory: StatementResponseFactory,
) -> Callable[..., Statement]:
    """Returns a factory function that creates Statement instances from mock_connection,
    using statement_response_factory to create the underlying JSON
    dictionaries.
    """

    def _factory(**kwargs: Any) -> Statement:
        """Construct a Statement instance from JSON dictionary.

        Args:
            **kwargs: Arguments to pass to statement_response_factory to create
                the statement JSON dictionary.
        Returns:
            A Statement instance.
        """
        statement_json = statement_response_factory(**kwargs)
        return Statement.from_response(mock_connection, statement_json)

    return _factory


StatementFactory: TypeAlias = Callable[..., Statement]
"""A factory type alias for creating Statement instances."""

CursorWithStatementFactory: TypeAlias = Callable[..., Cursor]
"""A factory type alias for creating cursors pre-loaded with a tracked statement."""


@pytest.fixture()
def cursor_with_statement_factory(
    mock_connection_factory: MockConnectionFactory,
    statement_factory: StatementFactory,
) -> CursorWithStatementFactory:
    """Returns a factory that builds a cursor whose tracked statement is set from the given
    statement_factory kwargs, so a test can declare the statement state under inspection inline
    and exercise cursor methods (e.g. _raise_if_statement_is_broken) against it.
    """

    def _factory(**statement_kwargs: Any) -> Cursor:
        cursor = mock_connection_factory(None, None).cursor()
        cursor._statement = statement_factory(**statement_kwargs)
        return cursor

    return _factory


GetStatementsResultsValue: TypeAlias = tuple[list[ChangelogRow], str | None]
"""A type alias for the return value of _get_statement_results()."""

MockConnectionFactory: TypeAlias = Callable[
    [StatementResponse | None, GetStatementsResultsValue | None], Connection
]
"""A factory type alias for creating mock Connection instances."""


@pytest.fixture
def mock_connection_factory(mocker, statement_response_factory) -> MockConnectionFactory:
    """
    A factory fixture that creates mock Connection instances for testing cursor behavior
    with mocked client responses.

    Any executed statement will appear to complete immediately with no results.

    Can call the returned factory with an optional statement_response to override
    the statement dict returned from any executed statement (use statement_response_factory()
    to help create such dicts).
    """

    def _factory(
        statement_response: StatementResponse | None,
        get_statement_results_value: GetStatementsResultsValue | None,
    ) -> Connection:
        # Create a mock instance with spec so attribute/method names are validated
        mock_conn = mocker.create_autospec(Connection, instance=True)
        mock_conn._closed = False
        mock_conn.is_closed = False

        # Make any executed statement return a completed statement with no results
        executed_statement_from_json = statement_response or statement_response_factory()
        mock_conn._execute_statement.return_value = executed_statement_from_json
        mock_conn._get_statement.return_value = executed_statement_from_json

        # Set up default statement results to empty list.
        # (Returns a tuple of (list of result rows, possible next_page URL))
        mock_conn._get_statement_results.return_value = get_statement_results_value or ([], None)

        # Replace the MagicMock cursor with the real implementation bound to the mock instance
        mock_conn.cursor = types.MethodType(Connection.cursor, mock_conn)
        # Likewise for closing_cursor
        mock_conn.closing_cursor = types.MethodType(Connection.closing_cursor, mock_conn)

        mock_conn._row_type_registry = RowTypeRegistry()
        # Only a class-level annotation, so the autospec would not have it.
        mock_conn.timeouts = DEFAULT_TIMEOUTS

        return mock_conn

    return _factory


@pytest.fixture
def mock_connection(mock_connection_factory: MockConnectionFactory) -> Connection:
    """A fixture that returns a mock connection for testing type conversion.

    The connection has no statement response or results by default, but will
    be sufficient for testing type conversion logic (will have a row type registry).
    """
    return mock_connection_factory(None, None)
