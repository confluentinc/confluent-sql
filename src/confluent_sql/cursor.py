"""
Cursor module for Confluent SQL DB-API driver.

This module provides the Cursor class for executing SQL statements and
retrieving results from Confluent SQL services.
"""

import logging
import random
import time
from itertools import islice
from typing import Optional, List, Tuple, Union, Dict, TYPE_CHECKING

from .exceptions import (
    InterfaceError,
    ProgrammingError,
    OperationalError,
)
from .statement import Statement, Op

if TYPE_CHECKING:
    from .connection import Connection


logger = logging.getLogger(__name__)


class Cursor:
    """
    A cursor for executing SQL statements and retrieving results.

    This class provides methods for executing SQL statements and fetching
    results from a Confluent SQL service connection.
    """

    def __init__(self, connection: "Connection", with_schema: bool = False):
        """
        Initialize a new cursor.

        Args:
            connection: The Connection object this cursor is associated with
        """
        self.rowcount = -1
        self.arraysize = 1

        # Cursor state
        self._connection = connection
        self._closed = False
        self._index = 0
        self._next_page = None
        self._with_schema = with_schema

        # Statement execution state
        self._statement: Optional[Statement] = None
        self._results: list[dict] = []

    @property
    def description(self) -> Optional[list[tuple]]:
        self._raise_if_closed()

        # Required by DB-API: https://peps.python.org/pep-0249/#description
        if self._statement is None:
            return None
        else:
            return self._statement.description

    def execute(
        self,
        operation: str,
        parameters: Optional[Union[tuple, list, dict]] = None,
        timeout: int = 3000,
        statement_name: Optional[str] = None,
        bounded: bool = True,
    ):
        """
        Execute a SQL statement.

        Args:
            operation: The SQL statement to execute
            parameters: Parameters for the SQL statement (optional)
            timeout: Maximum time to wait for statement completion in seconds (default: 3000)
            statement_name: Optional name for the statement (defaults to DB-API UUID if not provided)

        Raises:
            InterfaceError: If the cursor is closed
            ProgrammingError: If the SQL statement is invalid
            OperationalError: If the statement cannot be executed
        """
        # TODO: Handle parameters, see self._submit_statement

        self._raise_if_closed()

        if not operation.strip():
            raise ProgrammingError("SQL statement cannot be empty")

        # Reset internal state
        self._statement = None
        self._results = []
        self._index = 0
        self.rowcount = -1

        # Now submit the statement and wait for it to be ready
        self._submit_statement(operation, parameters, statement_name, bounded)
        self._wait_for_statement_ready(timeout)

    def executemany(
        self, operation: str, seq_of_parameters: list[Union[tuple, list, dict]]
    ):
        # Implement this if needed.
        # XXX: We need to handle multiple statements with a single cursor here,
        #      the logic currently implies each cursor handles a single statement at a time
        raise NotImplementedError("executemany not implemented")

    def fetchone(self) -> Optional[dict]:
        self._raise_if_closed()

        res = self._get_next_results(1)
        assert (
            len(res) <= 1
        ), "fetchone returned more than one result, this is probably a bug"
        # If no results are available, `res` is an empty list,
        # but we want to return None in this case: https://peps.python.org/pep-0249/#fetchone
        return res[0] if res else None

    def fetchmany(self, size: Optional[int] = None) -> list[dict]:
        self._raise_if_closed()

        if size is None:
            size = self.arraysize
        if size <= 0:
            raise InterfaceError("fetchmany must be called with size > 0, aborting")

        return self._get_next_results(size)

    def fetchall(self) -> list[dict]:
        """
        Fetch all the results from the current cursor.
        Beware that this will download and put into memory all the available results.
        Make sure the result set can fit in memory, and that you are not making too many calls
        at once to fetch the whole result set.
        If you want more control, use the cursor as an iterator, or use `fetchone`/`fetchmany`
        to fetch results one-by-one or in batches.
        """
        self._raise_if_closed()

        return self._get_next_results()

    def close(self) -> None:
        """
        Close the cursor and free associated resources.

        This method marks the cursor as closed and releases any
        resources associated with it. It also attempts to delete
        any running statement to prevent orphaned jobs.
        """
        if not self._closed:
            if self._statement is not None:
                self._statement = None

            self.rowcount = -1
            self._closed = True
            self._results = []
            self._index = 0

    def delete_statement(self) -> None:
        """
        Delete the CCloud Flink-side statement to prevent orphaned jobs / statement records.

        Raises:
            OperationalError: If statement deletion fails.
            InterfaceError: If the cursor is closed or if there is no statement to delete.
        """
        self._raise_if_closed()

        if self._statement is None:
            raise InterfaceError("No statement to delete")

        if not self._statement.is_deleted:
            self._connection._delete_statement(self._statement.name)
            self._statement.set_deleted()

    @property
    def is_closed(self) -> bool:
        """
        Check if the cursor is closed.

        Returns:
            True if the cursor is closed, False otherwise
        """
        return self._closed

    def _raise_if_closed(self) -> None:
        """Raise InterfaceError if the cursor or connection is closed."""
        if self._closed:
            raise InterfaceError("Cursor is closed")
        if self._connection.is_closed:
            raise InterfaceError("Connection is closed")

    def _fetch_next_page(self):
        if self._closed:
            raise InterfaceError("Cursor is closed")

        if not self._statement:
            raise InterfaceError("No statement was used. Call execute() first.")

        if not self._statement.is_ready:
            raise InterfaceError("Statement is not ready for result fetching.")

        if not self._statement.schema:
            raise InterfaceError("Trying to fetch results for a non-query statement")

        if not self._results or self._next_page is not None:
            logger.info(
                f"Fetching next page of results for statement {self._statement.name}"
            )
            results, next_page = self._connection._get_statement_results(
                self._statement.name, self._next_page
            )
            self._next_page = next_page
            self.rowcount += len(results)

            for res in results:
                row = {"row": tuple(res.get("row", []))}

                # op is not mandatory
                op_id = res.get("op", None)
                if op_id is not None:
                    row["op"] = Op(op_id)
                self._results.append(row)

    def __iter__(self):
        return self

    @property
    def _remaining(self):
        remaining = len(self._results) - self._index
        if remaining < 0:
            raise InterfaceError(
                "Internal index bigger than results list. This is probably a bug."
            )
        return remaining

    def __next__(self) -> dict:
        assert (
            self._statement is not None
        ), "Trying to fetch results with null statement"
        if self._remaining == 0:
            if self._results and not self._next_page:
                raise StopIteration
            self._fetch_next_page()
            # Check again, as we might not have new results for any reason
            if self._remaining == 0:
                raise StopIteration

        res = self._results[self._index]
        self._index += 1
        if self._with_schema and self._statement.schema is not None:
            res["row"] = self._map_row_to_schema(res["row"])
        return res

    def _get_next_results(self, limit: Optional[int] = None) -> list[dict]:
        if limit is None:
            return list(self)
        else:
            return list(islice(self, limit))

    def _wait_for_statement_ready(self, timeout: int) -> None:
        """
        Wait for statement to be ready (not in PENDING status).
        Uses exponential backoff with jitter to prevent thundering herd problems.

        Args:
            timeout: Maximum time to wait in seconds

        Raises:
            OperationalError: If polling times out or fails
        """
        assert (
            self._statement is not None
        ), "Calling _wait_for_statement_ready but _statement is None, this is probably a bug"
        start_time = time.time()
        base_delay = 1.0  # Start with 1 second
        max_delay = 30.0  # Maximum delay between polls
        current_delay = base_delay

        while time.time() - start_time < timeout:
            logger.info(f"Checking statement '{self._statement.name}' status...")
            response = self._connection._get_statement_status(self._statement.name)
            self._statement = Statement.from_response(response)

            # Statement is ready when it's not in PENDING status.
            # We first check if it failed, and return early if so:
            if self._statement.is_failed or self._statement.is_degraded:
                # TODO: Do something here
                return

            # Any kind of query in the completed phase is ready
            if self._statement.is_completed:
                return

            # For unbounded queries, RUNNING is enough though
            if not self._statement.is_bounded and self._statement.is_running:
                return

            # Exponential backoff with jitter to prevent thundering herd
            jitter = random.uniform(0.75, 1.25)  # ±25% randomness
            actual_delay = current_delay * jitter
            time.sleep(actual_delay)
            current_delay = min(current_delay * 1.5, max_delay)
        raise OperationalError(
            f"Statement submission timed out after {timeout} seconds"
        )

    def _submit_statement(
        self,
        operation: str,
        parameters: Optional[Union[Tuple, List, Dict]] = None,
        statement_name: Optional[str] = None,
        bounded: bool = True,
    ) -> None:
        """
        Submit a SQL statement for execution.

        Args:
            operation: The SQL statement to execute
            parameters: Parameters for the SQL statement (optional)
            statement_name: Optional name for the statement (defaults to DB-API UUID if not provided)

        Raises:
            OperationalError: If statement submission fails
        """
        # TODO: Handle parameters, see Connection.execute_statement
        logger.info(f"Submitting statement {operation}")
        response = self._connection._execute_statement(
            operation, parameters, statement_name, bounded
        )
        self._statement = Statement.from_response(response)

    def _map_row_to_schema(self, values) -> dict:
        assert self._statement is not None, "statement is none, this is probably a bug"
        if self._statement.schema is None:
            raise InterfaceError("schema not present, can't map values to keys")
        return _map_tuple_to_schema(self._statement.schema, values)


def _map_tuple_to_schema(schema: dict, values: tuple) -> dict:
    """
    Recursively transforms a tuple or list of data values into a
    dictionary based on the provided schema.
    """
    result_dict = {}

    for field_schema, value in zip(schema, values):
        field_name = field_schema["name"]
        type_info = field_schema.get("type")
        if type_info is None:
            type_info = field_schema.get("field_type")
        if isinstance(type_info, dict) and type_info.get("type") == "ROW":
            sub_schema_fields = type_info["fields"]
            result_dict[field_name] = _map_tuple_to_schema(sub_schema_fields, value)
        else:
            result_dict[field_name] = value
    return result_dict
