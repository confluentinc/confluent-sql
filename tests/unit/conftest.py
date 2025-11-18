"""Pytest configuration + fixtures for unit tests."""

from __future__ import annotations

import types
from collections.abc import Callable
from typing import Any, TypeAlias

import pytest

from confluent_sql import Connection
from confluent_sql.statement import Op


def pytest_runtest_setup(item):
    # Ensure that all tests within unit/ are marked as unit tests
    is_unit = any(item.iter_markers(name="unit"))
    if not is_unit:
        pytest.fail("Tests within 'unit/' must be marked with @pytest.mark.unit.")


ResultRowFactory: TypeAlias = Callable[[list[Any], Op | None], dict[str, Any]]


@pytest.fixture
def result_row_maker() -> ResultRowFactory:
    """A fixture that returns a helper function to create result row dictionaries
    in changelog format.

    These are helpful for overriding the return value of `mock_connection._get_statement_results`.
    """

    def _maker(row_values: list[Any], op: Op | None = None) -> dict[str, Any]:
        """Helper to create a result row dictionary for mocking statement results.

        Args:
            row_values: The list of values for the row.
            op: The changelog operation code (0=INSERT, 1=DELETE, etc). Default is 0 (INSERT).
        Returns:
            A dictionary representing the result row.
        """
        if op is None:
            # If no op is specified, code will assume it was an INSERT (op=0)
            return {"row": row_values}

        return {"row": row_values, "op": op.value}

    return _maker


StatementResponse: TypeAlias = dict[str, Any]
"""A type alias for statement v1 JSON dictionaries."""

StatementResponseFactory: TypeAlias = Callable[..., StatementResponse]
"""A factory type alias for creating statement v1 JSON dictionaries."""


@pytest.fixture()
def statement_response_factory() -> StatementResponseFactory:
    """A fixture that returns a factory function to create statement v1 JSON dictionaries
    with various overrides."""

    def _factory(  # noqa: PLR0913
        *,
        sql_statement: str = "SELECT TRUE AS VALUE",
        is_bounded: bool = True,
        is_append_only: bool = True,
        phase: str = "COMPLETED",
        status_detail: str = "",
        compute_pool_id: str = "lfcp-01x6d2",
        principal: str = "u-0xw9v9p",
        schema_columns: list[dict[str, Any]] | None = None,
        null_schema: bool = False,
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
            "name": "dbapi-d4c685ef-befe-4091-9548-05d5ccb52d4a",
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
                "stopped": False,
            },
            "status": {
                "detail": status_detail,
                "network_kind": "PUBLIC",
                "phase": phase,
                "scaling_status": {
                    "last_updated": "2025-11-07T18:45:05Z",
                    "scaling_state": "OK",
                },
                "traits": {
                    "connection_refs": [],
                    "is_append_only": is_append_only,
                    "is_bounded": is_bounded,
                    "schema": maybe_schema,
                    "sql_kind": "SELECT",
                    "upsert_columns": None,
                },
            },
        }

        if null_schema:
            response["status"]["traits"]["schema"] = None

        return response

    return _factory


GetStatementsResultsValue: TypeAlias = tuple[list[dict[str, Any]], str | None]
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

        return mock_conn

    return _factory
