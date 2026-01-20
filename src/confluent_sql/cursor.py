"""
Cursor module for Confluent SQL DB-API driver.

This module provides the Cursor class for executing SQL statements and
retrieving results from Confluent SQL services.
"""

from __future__ import annotations

import logging
import random
import time
import warnings
from itertools import islice
from typing import TYPE_CHECKING, Any

from .exceptions import (
    InterfaceError,
    OperationalError,
    ProgrammingError,
)
from .execution_mode import ExecutionMode
from .statement import Op, Schema, Statement
from .types import convert_statement_parameters

if TYPE_CHECKING:
    from .connection import Connection


logger = logging.getLogger(__name__)


class Cursor:
    """
    A cursor for executing SQL statements and retrieving results.

    This class provides methods for executing SQL statements and fetching
    results from a Confluent SQL service connection.
    """

    def __init__(
        self,
        connection: Connection,
        *,
        as_dict: bool = False,
        execution_mode: ExecutionMode,
    ):
        """
        Initialize a new cursor.

        Args:
            connection: The Connection object this cursor is associated with.
            as_dict: If True, fetch results as dictionaries; otherwise, as tuples.
        """
        self.rowcount = -1
        self.arraysize = 1

        # Cursor state
        self._connection = connection
        self._closed = False
        self._index = 0
        self._next_page = None
        self._results_as_dicts = as_dict
        self._execution_mode = execution_mode
        self._fetch_next_page_called = False

        # Statement execution state
        self._statement: Statement | None = None

        # TODO -- simplify to get rid of the dict-ness, stop storing the changelog operation,
        # no need.
        self._results: list[dict[str, tuple | Op]] = []

    @property
    def description(self) -> list[tuple] | None:
        self._raise_if_closed()

        # Required by DB-API: https://peps.python.org/pep-0249/#description
        if self._statement is None:
            return None
        else:
            return self._statement.description

    @property
    def statement(self) -> Statement:
        """
        Get the current statement associated with the cursor.

        Returns:
            The current Statement object.

        Raises:
            InterfaceError: If no statement has been executed yet.
        """
        if self._statement is None:
            raise InterfaceError("No statement has been executed yet.")
        return self._statement

    def execute(
        self,
        statement_text: str,
        parameters: tuple | list | None = None,
        *,
        timeout: int = 3000,
        statement_name: str | None = None,
    ) -> None:
        """
        Execute a SQL statement.

        Args:
            statement: The SQL statement to execute
            parameters: Parameters for the SQL statement (optional)
            timeout: Maximum time to wait for statement completion in seconds (default: 3000)
            statement_name: Optional name for the statement (defaults to DB-API UUID
                            if not provided)

        Raises:
            InterfaceError: If the cursor is closed
            ProgrammingError: If the SQL statement is invalid
            OperationalError: If the statement cannot be executed
        """
        self._raise_if_closed()

        if not statement_text.strip():
            raise ProgrammingError("SQL statement cannot be empty")

        # Delete any previous statement if present and in a deletable state
        if self._statement is not None and not self._statement.is_deleted:
            if self._statement.is_deletable:
                self.delete_statement()
            else:
                warnings.warn(
                    "Executing a new statement on a cursor with an existing active"
                    f" statement. The previous statement {self._statement.name} will not be deleted"
                    " automatically.",
                    stacklevel=2,
                )

        # Reset internal state
        self._statement = None
        self._statement_handle = None
        self._results = []
        self._index = 0
        self.rowcount = -1
        self._fetch_next_page_called = False
        self._next_page = None

        # Now submit the statement and wait for it to be ready
        self._statement = self._submit_statement(statement_text, parameters, statement_name)
        self._wait_for_statement_ready(timeout)

    def executemany(self, operation: str, seq_of_parameters: list[tuple | list | dict]):
        # Implement this if needed.
        # XXX: We need to handle multiple statements with a single cursor here,
        #      the logic currently implies each cursor handles a single statement at a time
        raise NotImplementedError("executemany not implemented")

    def _raise_if_ddl_mode(self):
        """Raise if cursor is in a DDL mode that doesn't support result fetching."""
        if self._execution_mode.is_ddl:
            raise InterfaceError(
                f"Cannot fetch results in {self._execution_mode.value} mode. "
                "DDL statements don't return result sets."
            )

    def fetchone(self) -> dict | tuple | None:
        self._raise_if_closed()
        self._raise_if_ddl_mode()

        res = self._get_next_results(1)
        assert len(res) <= 1, "fetchone returned more than one result, this is probably a bug"
        # If no results are available, `res` is an empty list,
        # but we want to return None in this case: https://peps.python.org/pep-0249/#fetchone
        return res[0] if res else None

    def fetchmany(self, size: int | None = None) -> list[dict | tuple]:
        self._raise_if_closed()
        self._raise_if_ddl_mode()

        if size is None:
            size = self.arraysize
        if size <= 0:
            raise InterfaceError(f"size must be a non-negative integer, got {size}")

        return self._get_next_results(size)

    def fetchall(self) -> list[dict | tuple]:
        """
        Fetch all the results from the current cursor.
        Beware that this will download and put into memory all the available results.
        Make sure the result set can fit in memory, and that you are not making too many calls
        at once to fetch the whole result set.
        If you want more control, use the cursor as an iterator, or use `fetchone`/`fetchmany`
        to fetch results one-by-one or in batches.
        """
        self._raise_if_closed()
        self._raise_if_ddl_mode()

        return self._get_next_results()

    def close(self) -> None:
        """
        Close the cursor and free associated resources.

        This method marks the cursor as closed and releases any
        local resources associated with it.

        If the statement is in a deletable state, it will also attempt to
        delete the statement from the server to free server-side resources.

        Active statements (e.g., running streaming queries) will not be deleted.
        """
        if not self._closed:
            if self._statement is not None and self._statement.is_deletable:
                try:
                    # Keep the statement around for posterity, but mark it as deleted.
                    self.delete_statement()
                except Exception as e:
                    logger.error(
                        f"Error deleting statement {self._statement.name} during cursorclose: {e}"
                    )

            self.rowcount = -1
            self._closed = True
            self._results = []
            self._index = 0

    def setinputsizes(self, sizes) -> None:
        """
        Set the sizes of input parameters.

        This method is a no-op for this implementation, as input sizes
        are not explicitly handled.
        """
        pass  # pragma: no cover

    def setoutputsize(self, size, column: int | None = None) -> None:
        """
        Set the size of output columns.

        This method is a no-op for this implementation, as output sizes
        are not explicitly handled.
        """
        pass  # pragma: no cover

    def delete_statement(self) -> None:
        """
        Delete any possible CCloud Flink-side statement to prevent orphaned jobs / statement
        records.

        If no statement was executed, or if the statement was already deleted, this is a no-op.

        Raises:
            OperationalError: If statement deletion fails.
            InterfaceError: If the cursor or connection is closed.
        """
        self._raise_if_closed()

        if self._statement is None or self._statement.is_deleted:
            return

        self._connection.delete_statement(self._statement.name)
        self._statement.set_deleted()

    @property
    def is_closed(self) -> bool:
        """
        Check if the cursor is closed.

        Returns:
            True if the cursor is closed, False otherwise
        """
        return self._closed

    @property
    def may_have_results(self) -> bool:
        """
        Return true if a statement has been submitted and may have results to fetch (or have
        cached some results already).
        """
        return (
            self._statement is not None
            and self._statement.schema is not None
            and (
                # We haven't fetched any pages yet to know about results or next page token
                (not self._fetch_next_page_called)
                # Or we have some remaining results in the local cache
                or self._remaining > 0
                # Or we know there are more pages to fetch.
                or self._next_page is not None
            )
        )

    def _raise_if_closed(self) -> None:
        """Raise InterfaceError if the cursor or connection is closed."""
        if self._closed:
            raise InterfaceError("Cursor is closed")
        if self._connection.is_closed:
            raise InterfaceError("Connection is closed")

    def _fetch_next_page(self):
        self._raise_if_closed()

        if not self._statement:
            raise InterfaceError("No statement was used. Call execute() first.")

        if not self._statement.is_ready:
            raise InterfaceError("Statement is not ready for result fetching.")

        if not self._statement.schema:
            raise InterfaceError("Trying to fetch results for a non-query statement")

        self._fetch_next_page_called = True

        if not self._results or self._next_page is not None:
            logger.info(f"Fetching next page of results for statement {self._statement.name}")
            results, next_page = self._connection._get_statement_results(
                self._statement.name, self._next_page
            )
            self._next_page = next_page
            self.rowcount += len(results)

            # Use the statement's type converter to decode rows from API JSON to Python values
            type_converter = self._statement.type_converter
            for res in results:
                decoded_row = type_converter.to_python_row(res.get("row", []))

                row: dict[str, tuple | Op] = {"row": decoded_row}

                # op is not mandatory
                op_id = res.get("op", None)
                if op_id is not None:
                    op = Op(op_id)

                    # Temporary until smarter changelog parsing.
                    if op != Op.INSERT:
                        logger.error(
                            f"""Received non-INSERT op {op} in results, not smart enough to handle
                            this yet."""
                        )
                        raise NotImplementedError("Only INSERT op is supported in results for now.")

                    row["op"] = op

                self._results.append(row)

    def __iter__(self):
        return self

    @property
    def _remaining(self):
        remaining = len(self._results) - self._index
        if remaining < 0:
            raise InterfaceError(
                "Internal index bigger than results list. This is probably a bug."
            )  # pragma: no cover
        return remaining

    def __next__(self) -> tuple[Any] | dict[str, Any]:
        """
        Return the next row from the result set.

        Will be a tuple if as_dict is False, or a dict based on the statement schema if as_dict
        is True
        """
        assert self._statement is not None, "Trying to fetch results with null statement"
        if self._remaining == 0:
            if self._results and not self._next_page:
                raise StopIteration
            self._fetch_next_page()
            # Check again, as we might not have new results for any reason
            if self._remaining == 0:
                raise StopIteration

        # By default, we return the raw row as a tuple.
        res: Any = self._results[self._index]["row"]
        self._index += 1

        # If this is a dict cursor, map the tuple to a dict using the statement schema
        if self._results_as_dicts and self._statement.schema is not None:
            res = self._map_row_to_dict(res)

        return res

    def _get_next_results(self, limit: int | None = None) -> list[dict[str, Any] | tuple[Any]]:
        """
        Get the next results from the cursor, up to the specified limit.
        Returns either tuples or dicts based on the `as_dict` flag the cursor was created with.
        """
        if limit is None:
            return list(self)
        else:
            return list(islice(self, limit))

    def _wait_for_statement_ready(self, timeout: int) -> None:
        """
        Wait for self._statement to be ready (not in PENDING status).
        Uses exponential backoff with jitter to prevent thundering herd problems.

        Reassigns to self._statement with updated status on each poll.

        Args:
            timeout: Maximum time to wait in seconds

        Raises:
            OperationalError: If polling times out or fails
        """
        if self._statement is None:
            raise InterfaceError(
                "Calling _wait_for_statement_ready but _statement is None, this is probably a bug"
            )

        start_time = time.time()
        base_delay = 1.0  # Start with 1 second
        max_delay = 30.0  # Maximum delay between polls
        current_delay = base_delay

        while time.time() - start_time < timeout:
            logger.info(f"Checking statement '{self._statement.name}' status...")
            response = self._connection._get_statement(self._statement.name)

            # Will raise if the phase is FAILED
            self._statement = Statement.from_response(self._connection, response)

            # We only support append-only statements for now. Our changelog
            # parsing is not smart enough to handle updates/deletes from streaming statements.
            # (This is different from bounded vs unbounded: even if we relax and start to
            #  return pages of results from unbounded statements, we still can't handle
            #  non-append-only changelogs).
            if not self._statement.is_append_only:
                raise NotImplementedError("Only append-only statements are supported for now.")

            if self._statement.is_ready or (
                # Handle the current (Jan 2026) bug state where streaming DDL statements like CTAS
                # are erroneously marked as being bounded, see
                # https://confluent.slack.com/archives/C044A8FNSJ0/p1768575045244419
                self._execution_mode == ExecutionMode.STREAMING_DDL and self._statement.is_running
            ):
                # Ready to possibly fetch results!
                return

            # If the statement is degraded (unbounded and in a bad state), hmm.
            # For now, treat it as an error.
            if self._statement.is_degraded:
                raise OperationalError(
                    f"Statement '{self._statement.name}' is in DEGRADED state: "
                    f"{self._statement.status['detail']}"
                )

            # Exponential backoff with jitter to prevent thundering herd
            jitter = random.uniform(0.75, 1.25)  # ±25% randomness
            actual_delay = current_delay * jitter
            time.sleep(actual_delay)
            current_delay = min(current_delay * 1.5, max_delay)
        raise OperationalError(f"Statement submission timed out after {timeout} seconds")

    def _submit_statement(
        self,
        statement_text: str,
        parameters: tuple | list | None = None,
        statement_name: str | None = None,
    ) -> Statement:
        """
        Submit a SQL statement for execution.

        Args:
            operation: The SQL statement to execute
            parameters: Parameters for the SQL statement (optional)
            statement_name: Optional name for the statement (defaults to DB-API UUID if
                            not provided)

        Returns:
            The submitted Statement object

        Raises:
            OperationalError: If statement submission fails
            ProgrammingError: If template parameter interpolation fails
        """
        logger.info(f"Submitting statement {statement_text}")

        interpolated_statement = self._interpolate_parameters(statement_text, parameters)

        logger.debug(f"Interpolated statement: {interpolated_statement}")

        response = self._connection._execute_statement(
            interpolated_statement, statement_name, self._execution_mode
        )
        return Statement.from_response(self._connection, response)

    def _interpolate_parameters(
        self,
        statement_template: str,
        parameters: tuple | list | None = None,
    ) -> str:
        """Interpolate parameters (if any) into the statement template, returning
        the final statement.

        Raises ProgrammingError if wrong number of parameters provided.
        """
        if parameters is None or len(parameters) == 0:
            return statement_template

        if not isinstance(parameters, (list, tuple)):
            raise TypeError(f"Parameters must be a tuple or list, got {type(parameters)}")

        # May raise InterfaceError if unsupported parameter type found
        converted_params = convert_statement_parameters(parameters)

        # Interpolate parameters using the %s placeholders in statement_template.
        try:
            interpolated_statement = statement_template % converted_params
        except TypeError as e:
            raise ProgrammingError(f"Error interpolating parameters into statement: {e}") from e

        return interpolated_statement

    def _map_row_to_dict(self, values) -> dict:
        assert self._statement is not None, "statement is none, this is probably a bug"
        if self._statement.schema is None:
            raise InterfaceError("schema not present, can't map values to keys")
        return _map_tuple_to_dict(self._statement.schema, values)


def _map_tuple_to_dict(schema: Schema, values: tuple) -> dict:
    """
    Recursively transforms a tuple of data values into a
    dictionary based on the provided schema.
    """
    result_dict = {}

    for column, value in zip(schema.columns, values, strict=True):
        field_name = column.name
        # Skip recursive mapping for nonatomic types for now.
        result_dict[field_name] = value
    return result_dict
