"""
Cursor module for Confluent SQL DB-API driver.

This module provides the Cursor class for executing SQL statements and
retrieving results from Confluent SQL services.
"""

import logging
import random
import time
from dataclasses import dataclass
from enum import Enum
from typing import Optional, List, Tuple, Union, Dict

import httpx

from .connection import Connection
from .exceptions import (
    InterfaceError,
    DatabaseError,
    ProgrammingError,
    OperationalError,
)


logger = logging.getLogger(__name__)


class Op(Enum):
    INSERT = 0
    UPDATE_BEFORE = 1
    UPDATE_AFTER = 2
    DELETE = 3

    def __str__(self):
        if self is self.INSERT:
            return "+I"
        elif self is self.UPDATE_BEFORE:
            return "-U"
        elif self is self.UPDATE_AFTER:
            return "+U"
        elif self is self.DELETE:
            return "-D"
        else:
            raise ValueError(f"Unknown value for Op: '{self.value}'. This is probably a bug")


class Phase(Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    DELETING = "DELETING"
    FAILED = "FAILED"
    # This is not documented in the rest api docs, but mentioned here:
    # https://docs.confluent.io/cloud/current/flink/concepts/statements.html#flink-sql-statements
    DEGRADED = "DEGRADED"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"

    # This is only used internally,
    # never returned by the api.
    DELETED = "DELETED"


@dataclass
class Statement:
    statement_id: str
    name: str
    spec: dict
    status: dict
    traits: dict
    # Internal state
    _phase: Phase
    _deleted: bool = False

    @property
    def is_ready(self) -> bool:
        if self.is_bounded:
            return self.phase in [Phase.COMPLETED, Phase.STOPPED]
        else:
            return self.phase in [Phase.COMPLETED, Phase.STOPPED, Phase.RUNNING]

    @property
    def is_running(self) -> bool:
        return self.phase == Phase.RUNNING

    @property
    def is_completed(self) -> bool:
        return self.phase is Phase.COMPLETED

    @property
    def is_failed(self) -> bool:
        return self.phase is Phase.FAILED

    @property
    def is_degraded(self) -> bool:
        return self.phase is Phase.DEGRADED

    @property
    def phase(self) -> Phase:
        if self._deleted:
            return Phase.DELETED
        return self._phase

    @property
    def compute_pool_id(self) -> str:
        return self.spec["compute_pool_id"]

    @property
    def principal(self) -> str:
        return self.spec["principal"]

    @property
    def sql_kind(self) -> str:
        return self.traits["sql_kind"]

    @property
    def is_bounded(self) -> bool:
        return self.traits["is_bounded"]

    @property
    def is_append_only(self) -> bool:
        return self.traits["is_append_only"]

    @property
    def schema(self) -> Optional[dict]:
        return self.traits.get("schema", {}).get("columns")

    @property
    def connection_refs(self) -> list:
        return self.traits.get("connection_refs", [])

    @property
    def description(self) -> Optional[list[tuple]]:
        # This is required by the cursor object, see https://peps.python.org/pep-0249/#description
        # It's a list of 7-item tuples, the items represent:
        # (name, type_code, display_size, internal_size, precision, scale, null_ok)
        if self.schema is not None:
            return [
                (col["name"], col["type"]["type"], None, None, None, None, None)
                for col in self.schema
            ]
        return None

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

            # Check the phase first.
            try:
                phase = Phase(status["phase"])
            except ValueError:
                raise OperationalError(
                    f"Received an unknown phase for statement from the server: {status['phase']}. "
                    "This is probably a bug"
                )

            # If it's failed, we won't get 'traits', and it's probably good to raise an error.
            # TODO: Should we instead set the phase and avoid erroring out here?
            if phase is Phase.FAILED:
                raise OperationalError(status["detail"])

            traits = status["traits"]
        except KeyError as e:
            raise OperationalError(f"Error parsing statement response, missing {e}.") from e

        return cls(statement_id, name, spec, status, traits, phase)


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
        self.rowcount = -1
        self.arraysize = 1

        # Cursor state
        self._connection = connection
        self._closed = False

        # Statement execution state
        self._statement: Optional[Statement] = None
        self._results: list[dict] = []
        self._index = 0

    @property
    def description(self) -> Optional[list[tuple]]:
        # Required by DB-API: https://peps.python.org/pep-0249/#description
        if self._statement is None:
            return None
        else:
            return self._statement.description

    def close(self) -> None:
        """
        Close the cursor and free associated resources.

        This method marks the cursor as closed and releases any
        resources associated with it. It also attempts to delete
        any running statement to prevent orphaned jobs.
        """
        if not self._closed:
            if self._statement is not None:
                self._delete_statement()
                self._statement = None

            self.rowcount = -1
            self._closed = True
            self._results = []
            self._index = 0

    def execute(
        self,
        operation: str,
        parameters: Optional[Union[tuple, list, dict]] = None,
        timeout: int = 3000,
        statement_name: Optional[str] = None,
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
        if self._closed:
            raise InterfaceError("Cursor is closed")

        if not operation.strip():
            raise ProgrammingError("SQL statement cannot be empty")

        # If a statement is present, delete it first
        if self._statement is not None:
            self._delete_statement()
            self._statement = None

        # Then reset the state
        self._results = []
        self._index = 0
        self.rowcount = -1

        # Now submit the statement and wait for it to be ready
        self._submit_statement(operation, parameters, statement_name)
        self._wait_for_statement_ready(timeout)

    def executemany(self, operation: str, seq_of_parameters: list[Union[tuple, list, dict]]):
        # Implement this if needed.
        # XXX: We'd need to handle multiple statements with a single cursor here,
        #      all the logic currently implies each cursor handles a single statement at a time
        raise NotImplementedError("executemany not implemented")

    def _fetch_results(self):
        if self._closed:
            raise InterfaceError("Cursor is closed")

        if not self._statement:
            raise InterfaceError("No statement was used. Call execute() first.")

        if not self._statement.is_ready:
            raise InterfaceError("Statement is not ready for result fetching.")

        if not self._statement.schema:
            raise InterfaceError("Trying to fetch results for a non-query statement")

        if not self._statement.is_bounded:
            self._delete_statement()
            raise NotImplementedError("Support for unbounded queries not implemented")

        if not self._results:
            results = self._connection.get_statement_results(self._statement.name)
            self.rowcount = len(results)

            for row in results:
                # Extract row data only - no operation processing
                op = Op(row.get("op"))
                data = row.get("row", [])
                result = {"op": f"{op}"}

                # Add column data if we have schema
                if self._statement.schema and len(data) == len(self._statement.schema):
                    for i, col in enumerate(self._statement.schema):
                        result[col["name"]] = data[i]
                else:
                    # Fallback: use generic column names
                    for i, value in enumerate(data):
                        result[f"col_{i}"] = value

                self._results.append(result)

    def _get_next_results(self, limit: Optional[int] = None) -> list[tuple]:
        remaining = len(self._results) - self._index

        if remaining == 0:
            return []

        if remaining < 0:
            raise InterfaceError("Trying fetch a negative number of elements")

        if limit is None:
            limit = remaining
        else:
            limit = min(remaining, limit)

        start = self._index
        end = self._index + limit
        self._index += limit
        return [tuple(row.values()) for row in self._results[start:end]]

    def fetchone(self) -> Optional[Tuple]:
        self._fetch_results()
        res = self._get_next_results(1)
        assert len(res) <= 1, "fetchone returned more than one result, this is probably a bug"
        # Here we want to return None if no results are available,
        # not an empty list: https://peps.python.org/pep-0249/#fetchone
        if not res:
            return None
        return res[0]

    def fetchmany(self, size: Optional[int] = None) -> List[Tuple]:
        self._fetch_results()

        # Get the next batch of results
        if size is None:
            size = self.arraysize

        return self._get_next_results(size)

    def fetchall(self) -> List[tuple]:
        self._fetch_results()
        return self._get_next_results()

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
            response = self._connection.get_statement_status(self._statement.name)
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
        raise OperationalError(f"Statement submission timed out after {timeout} seconds")

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
        response = self._connection.execute_statement(operation, parameters, statement_name)
        self._statement = Statement.from_response(response)

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
            response = self._connection.get_statement_status(self._statement.name)

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

    def _delete_statement(self) -> None:
        """
        Delete a running statement to prevent orphaned jobs.

        Raises:
            OperationalError: If statement deletion fails
        """
        assert self._statement is not None, (
            "Calling _delete_statement but _statement is None, this is probably a bug"
        )
        self._connection.delete_statement(self._statement.statement_id)
        self._statement.set_deleted()
