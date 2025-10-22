"""
Cursor module for Confluent SQL DB-API driver.

This module provides the Cursor class for executing SQL statements and
retrieving results from Confluent SQL services.
"""

import random
import time
from dataclasses import dataclass
from typing import Optional, List, Tuple, Union, Dict

from .connection import Connection
from .exceptions import (
    InterfaceError,
    DatabaseError,
    ProgrammingError,
    OperationalError,
)


@dataclass
class Statement:
    statement_id: str
    name: str
    spec: dict
    status: dict
    results: list[dict]
    traits: dict
    schema: Optional[dict]
    description: Optional[list[tuple]]
    _deleted: bool = False

    @property
    def is_running(self) -> bool:
        return self.phase not in ["COMPLETED", "STOPPED", "FAILED"]

    @property
    def phase(self) -> str:
        if self._deleted:
            return "DELETED"
        return self.status["phase"]

    @property
    def compute_pool_id(self) -> str:
        return self.spec["compute_pool_id"]

    @property
    def principal(self) -> str:
        return self.spec["principal"]

    @property
    def is_bounded(self) -> bool:
        return self.traits["is_bounded"]

    @property
    def sql_kind(self) -> str:
        return self.traits["sql_kind"]

    @property
    def is_append_only(self) -> bool:
        return self.traits["is_append_only"]

    @property
    def connection_refs(self) -> list:
        return self.traits.get("connection_refs", [])

    def set_deleted(self):
        self._deleted = True

    @classmethod
    def from_response(cls, response: dict) -> "Statement":
        try:
            # Mandatory fields
            statement_id = response["metadata"]["uid"]
            name = response["name"]
            spec = response["spec"]
            status = response["status"]
            # TODO: We should probably set a flag and avoid erroring out here.
            if status['phase'] == "FAILED":
                raise OperationalError(status['detail'])
            # XXX: Should this be optional?
            # traits = status["traits"]
            traits = status.get("traits", {})

            # Optional fields with defaults
            results = response.get("results", [])

            # Optional fields
            schema = traits.get("schema", {}).get("columns")
            # XXX: Is this used anywhere?
            description = (
                [(col["name"], col["type"]["type"]) for col in schema]
                if schema is not None
                else None
            )
        except KeyError as e:
            raise OperationalError(f"Error parsing statement response, missing {e}.") from e

        return cls(
            statement_id,
            name,
            spec,
            status,
            results,
            traits,
            schema,
            description,
        )


class Cursor:
    """
    A cursor for executing SQL statements and retrieving results.

    This class provides methods for executing SQL statements and fetching
    results from a Confluent SQL service connection.
    """

    def __init__(self, connection: Connection):
        """
        Initialize a new cursor.

        Args:
            connection: The Connection object this cursor is associated with
        """
        self.connection = connection
        self._closed = False
        self._rowcount = -1
        self._arraysize = 1
        self._last_executed: Optional[str] = None

        # Statement execution state
        self._statement: Optional[Statement] = None
        self._all_results: list[dict] = []
        self._current_result_index = 0

    def execute(
        self,
        operation: str,
        parameters: Optional[Union[Tuple, List, Dict]] = None,
        timeout: int = 3000,
        statement_name: Optional[str] = None,
    ) -> "Cursor":
        """
        Execute a SQL statement.

        Args:
            operation: The SQL statement to execute
            parameters: Parameters for the SQL statement (optional)
            timeout: Maximum time to wait for statement completion in seconds (default: 3000)
            statement_name: Optional name for the statement (defaults to DB-API UUID if not provided)

        Returns:
            Self for method chaining

        Raises:
            InterfaceError: If the cursor is closed
            ProgrammingError: If the SQL statement is invalid
            OperationalError: If the statement cannot be executed
        """
        if self._closed:
            raise InterfaceError("Cursor is closed")

        if not operation or not operation.strip():
            raise ProgrammingError("SQL statement cannot be empty")

        self._last_executed = operation
        self._all_results = []
        self._current_result_index = 0
        # XXX: Should we check if a statement is present?
        # XXX: Should we do anything else if it is?
        self._statement = None

        try:
            # Step 1: Submit the SQL statement
            self._submit_statement(operation, parameters, statement_name)

            # Step 2: Wait for statement to be ready (not in PENDING status)
            # in this loop, if FAILED or DEGRADED, raise an error
            self._wait_for_statement_ready(timeout)

            # Step 3: Set up cursor state for all successful statements
            # All queries are treated as streaming - database server handles result management
            # TODO: handle snapshot queries
            self._setup_cursor_state()
        except Exception as e:
            raise OperationalError("Failed to execute statement") from e

        return self

    def _wait_for_statement_ready(self, timeout: int) -> None:
        """
        Wait for statement to be ready (not in PENDING status).
        Uses exponential backoff with jitter to prevent thundering herd problems.

        Args:
            timeout: Maximum time to wait in seconds

        Raises:
            OperationalError: If polling times out or fails
        """
        assert self._statement is not None, (
            "Calling _wait_for_statement_ready but _statement is None, this is probably a bug"
        )
        start_time = time.time()
        base_delay = 1.0  # Start with 1 second
        max_delay = 30.0  # Maximum delay between polls
        current_delay = base_delay

        while time.time() - start_time < timeout:
            try:
                # Check statement status - this will raise OperationalError if status/phase is missing
                self._update_statement_status()

                # Statement is ready when it's not in PENDING status
                # Check for failure states first - exit immediately
                if self._statement.phase in ["FAILED", "DEGRADED"]:
                    return

                # Wait for COMPLETED status
                if self._statement.phase == "COMPLETED":
                    return

                # For unbounded queries, RUNNING is acceptable
                if not self._statement.is_bounded and self._statement.phase == "RUNNING":
                    return

                # Exponential backoff with jitter to prevent thundering herd
                jitter = random.uniform(0.75, 1.25)  # ±25% randomness
                actual_delay = current_delay * jitter
                time.sleep(actual_delay)
                current_delay = min(current_delay * 1.5, max_delay)
            except Exception as e:
                raise OperationalError("Failed to poll statement status") from e
        raise OperationalError(f"Statement submission timed out after {timeout} seconds")

    def _setup_cursor_state(self) -> None:
        """
        Set up cursor state after successful statement execution.
        This sets description and rowcount but does not parse results.
        All queries are treated as streaming - database server handles result management.
        """
        assert self._statement is not None, (
            "Calling _setup_cursor_state but _statement is None, this is probably a bug"
        )

        # Set rowcount for non-query statements
        if not self._statement.schema:
            self._rowcount = 0
        else:
            # Will be set when results are fetched
            self._rowcount = -1

    def _submit_statement(
        self,
        operation: str,
        parameters: Optional[Union[Tuple, List, Dict]] = None,
        statement_name: Optional[str] = None,
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
        try:
            # Delegate to connection object for statement execution
            response = self.connection.execute_statement(operation, parameters, statement_name)
            self._statement = Statement.from_response(response)
        except Exception as e:
            raise OperationalError(f"Failed to submit {operation}") from e

    def _parse_results_from_response(self, data: List[Dict]) -> None:
        """
        Parse results from the API response data and append to existing results.
        Returns raw results without processing changelog operations.

        Args:
            data: List of result data from the API response
        """
        assert self._statement is not None, (
            "Calling _parse_results_from_response but _statement is None, this is probably a bug"
        )
        for item in data:
            # Extract row data only - no operation processing
            row_data = item.get("row", [])

            # Create result row with just the data
            result_row = {}

            # Add column data if we have schema
            if self._statement.schema and len(row_data) == len(self._statement.schema):
                for i, col in enumerate(self._statement.schema):
                    result_row[col["name"]] = row_data[i]
            else:
                # Fallback: use generic column names
                for i, value in enumerate(row_data):
                    result_row[f"col_{i}"] = value

            self._all_results.append(result_row)

    def _update_statement_status(self) -> None:
        """
        Check the current status of the statement.

        Raises:
            OperationalError: If status check fails
        """
        assert self._statement is not None, (
            "Calling _update_statement_status but _statement is None, this is probably a bug"
        )
        try:
            response = self.connection.get_statement_status(self._statement.name)
            self._statement = Statement.from_response(response)
        except Exception as e:
            raise OperationalError("Error checking statement status") from e

    def _handle_failed_statement(self) -> None:
        """
        Handle a failed statement by retrieving detailed error information.

        Raises:
            DatabaseError: With detailed error message from the API
        """
        assert self._statement is not None, (
            "Calling _handle_failed_statement but _statement is None, this is probably a bug"
        )
        try:
            # Delegate to connection object to get statement details
            response = self.connection.get_statement_status(self._statement.name)

            # Extract error details from the response
            error_message = "Statement execution failed"
            if response.get("status") and response["status"].get("detail"):
                error_message = response["status"]["detail"]

            raise DatabaseError(f"Statement failed: {error_message}")

        except DatabaseError:
            # Re-raise DatabaseError as-is
            raise
        except Exception as e:
            raise DatabaseError(f"Statement failed: {e}")

    def fetchall(self) -> List[Tuple]:
        """
        Fetch all remaining rows from the result set.

        Returns:
            A list of tuples, where each tuple represents a row

        Raises:
            InterfaceError: If the cursor is closed, no result set is available, or query is unbounded
        """
        if self._closed:
            raise InterfaceError("Cursor is closed")

        if not self._statement:
            raise InterfaceError("No result set available. Call execute() first.")

        # Check if this is an unbounded streaming query
        if not self._statement.is_bounded:
            raise InterfaceError(
                "fetchall() is not supported for unbounded streaming queries. "
                "Use fetchone() or fetchmany() in a loop for streaming data, "
                "or implement a polling pattern for continuous updates."
            )

        # All queries are treated as streaming - check if statement is ready
        if self._statement.phase == "RUNNING":
            raise InterfaceError("Statement is not ready for result fetching.")
        elif self._statement.phase not in ["COMPLETED", "STOPPED", "RUNNING"]:
            raise InterfaceError("Statement is not ready for result fetching.")

        if not self._statement.schema:
            # Non-query statement (INSERT, UPDATE, DELETE, etc.)
            return []

        # If we haven't fetched results yet, fetch them now
        if not self._all_results:
            self._fetch_all_pages()

        # Convert results to tuples - return raw data without changelog operations
        result_tuples = []
        for row in self._all_results:
            # Return raw data as tuples
            result_tuples.append(tuple(row.values()))

        self._rowcount = len(result_tuples)
        return result_tuples

    def _fetch_all_pages(self) -> None:
        """
        Fetch all pages of results using pagination.
        This is where result parsing actually happens (lazy evaluation).
        All queries are treated as streaming - database server handles result management.

        Raises:
            OperationalError: If fetching results fails
        """
        # If results are already parsed, don't fetch again
        if self._all_results:
            return

        assert self._statement is not None, (
            "Calling _fetch_all_pages but _statement is None, this is probably a bug"
        )

        try:
            # Start with the first page
            response = self.connection.get_statement_results(self._statement.name)
            self._parse_results_from_response(response.get("results", {}).get("data", []))

            # Check for pagination and fetch all remaining pages
            next_url = response.get("metadata", {}).get("next", "")
            while next_url and len(next_url) > 0:
                # Fetch the next page using the Connection method
                next_response = self.connection.get_statement_results_from_url(next_url)
                self._parse_results_from_response(next_response.get("results", {}).get("data", []))

                # Get the next URL for the next iteration
                next_url = next_response.get("metadata", {}).get("next", "")

        except Exception as e:
            print(f"Failed to fetch results from API: {e}")
            # If results API fails, return empty results
            self._all_results = []

    def fetchone(self) -> Optional[Tuple]:
        """
        Fetch the next row from the result set.
        For unbounded streaming queries, this acts as a yielding iterator.

        Returns:
            A tuple representing the next row, or None if no more rows

        Raises:
            InterfaceError: If the cursor is closed or no result set is available
        """
        if self._closed:
            raise InterfaceError("Cursor is closed")

        if not self._statement:
            raise InterfaceError("No result set available. Call execute() first.")

        # All queries are treated as streaming - check if statement is ready
        if self._statement.phase not in ["COMPLETED", "STOPPED", "RUNNING"]:
            raise InterfaceError("Statement is not ready for result fetching.")

        if not self._statement.schema:
            # Non-query statement
            return None

        # For unbounded streaming queries, fetch results incrementally
        if not self._statement.is_bounded:
            return self._fetch_next_streaming_row()

        # For bounded queries, use the existing logic
        # If we haven't fetched results yet, fetch them now
        if not self._all_results:
            self._fetch_all_pages()

        # Get the current result and advance the index
        if self._current_result_index < len(self._all_results):
            row = self._all_results[self._current_result_index]
            self._current_result_index += 1
            return tuple(row.values())

        return None

    def _fetch_next_streaming_row(self) -> Optional[Tuple]:
        """
        Fetch the next row from an unbounded streaming query.
        This implements a yielding iterator pattern for streaming data.

        Returns:
            A tuple representing the next row, or None if no more rows available
        """
        # For streaming queries, we need to fetch results incrementally
        # This is a simplified implementation - in practice, you might want to
        # implement more sophisticated streaming logic

        # Check if we have cached results to return
        if self._current_result_index < len(self._all_results):
            row = self._all_results[self._current_result_index]
            self._current_result_index += 1

            # Return raw data as tuple
            return tuple(row.values())

        # For streaming queries, we would typically:
        # 1. Poll the API for new results
        # 2. Parse and cache new results
        # 3. Return the next available row
        #
        # For now, we'll return None to indicate no more data
        # In a full implementation, this would involve:
        # - Polling the results API endpoint
        # - Parsing new results as they arrive
        # - Managing connection state for continuous streaming

        return None

    def _fetch_streaming_batch(self, size: int) -> List[Tuple]:
        """
        Fetch a batch of rows from an unbounded streaming query.
        This implements a yielding iterator pattern for streaming data.

        Args:
            size: Number of rows to fetch

        Returns:
            A list of tuples representing the rows
        """
        results = []

        # For streaming queries, we need to fetch results incrementally
        # This is a simplified implementation - in practice, you might want to
        # implement more sophisticated streaming logic

        # Check if we have cached results to return
        remaining = len(self._all_results) - self._current_result_index
        batch_size = min(size, remaining)

        for i in range(batch_size):
            row = self._all_results[self._current_result_index + i]

            # Return raw data as tuple
            results.append(tuple(row.values()))

        self._current_result_index += batch_size

        # For streaming queries, we would typically:
        # 1. Poll the API for new results if we need more data
        # 2. Parse and cache new results as they arrive
        # 3. Return available results (might be fewer than requested)
        #
        # For now, we'll return what we have available
        # In a full implementation, this would involve:
        # - Polling the results API endpoint for new data
        # - Parsing new results as they arrive
        # - Managing connection state for continuous streaming

        return results

    def fetchmany(self, size: Optional[int] = None) -> List[Tuple]:
        """
        Fetch the next set of rows from the result set.
        For unbounded streaming queries, this acts as a yielding iterator.

        Args:
            size: Number of rows to fetch (defaults to arraysize)

        Returns:
            A list of tuples, where each tuple represents a row

        Raises:
            InterfaceError: If the cursor is closed or no result set is available
        """
        if self._closed:
            raise InterfaceError("Cursor is closed")

        if not self._statement:
            raise InterfaceError("No result set available. Call execute() first.")

        # All queries are treated as streaming - check if statement is ready
        if self._statement.phase not in ["COMPLETED", "STOPPED", "RUNNING"]:
            raise InterfaceError("Statement is not ready for result fetching.")

        if not self._statement.schema:
            # Non-query statement
            return []

        fetch_size = size if size is not None else self._arraysize
        results = []

        # For unbounded streaming queries, fetch results incrementally
        if not self._statement.is_bounded:
            return self._fetch_streaming_batch(fetch_size)

        # For bounded queries, use the existing logic
        # If we haven't fetched results yet, fetch them now
        if not self._all_results:
            self._fetch_all_pages()

        # Get the next batch of results
        remaining = len(self._all_results) - self._current_result_index
        batch_size = min(fetch_size, remaining)

        for i in range(batch_size):
            row = self._all_results[self._current_result_index + i]

            # Return raw data as tuple
            results.append(tuple(row.values()))

        self._current_result_index += batch_size
        return results

    def close(self) -> None:
        """
        Close the cursor and free associated resources.

        This method marks the cursor as closed and releases any
        resources associated with it. It also attempts to delete
        any running statement to prevent orphaned jobs.
        """
        if not self._closed:
            # Try to delete any running statement
            # XXX: Do we need to also cleanup completed statements?
            if self._statement is not None and self._statement.is_running:
                self._delete_statement()

            self._closed = True
            self._rowcount = -1
            self._all_results = []
            self._current_result_index = 0
            self._statement = None

    def _delete_statement(self) -> None:
        """
        Delete a running statement to prevent orphaned jobs.

        Raises:
            OperationalError: If statement deletion fails
        """
        assert self._statement is not None, (
            "Calling _delete_statement but _statement is None, this is probably a bug"
        )
        try:
            self.connection.delete_statement(self._statement.statement_id)
            print(self._statement)
            self._statement.set_deleted()
        except Exception as e:
            raise OperationalError(f"Failed to delete statement: {e}")
