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
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, TypeAlias

from .changelog import (
    AppendOnlyChangelogProcessor,
    ChangeloggedRow,
    ChangelogProcessor,
    FetchMetrics,
    RawChangelogProcessor,
    ResultTupleOrDict,
)
from .changelog_compressor import ChangelogCompressor, create_changelog_compressor
from .exceptions import (
    ComputePoolExhaustedError,
    InterfaceError,
    OperationalError,
    ProgrammingError,
)
from .execution_mode import ExecutionMode
from .statement import Phase, Statement
from .types import convert_statement_parameters

if TYPE_CHECKING:
    from .connection import Connection

logger = logging.getLogger(__name__)

ResultRow: TypeAlias = ResultTupleOrDict | ChangeloggedRow
"""A single row of results returned from a cursor fetch or iteration.
Can be a dict or tuple depending on cursor configuration and statement schema, or a ChangeloggedRow
containing the changelog operation and a dict or tuple if the statement is not append-only."""


class Cursor:
    """
    A cursor for executing SQL statements and retrieving results.

    This class provides methods for executing SQL statements and fetching
    results from a Confluent SQL service connection.

    Result Consumption Methods
    --------------------------
    Two approaches are available for consuming query results, with different
    blocking behaviors in streaming mode:

    1. **Iteration (for row in cursor):**
       - Always blocking, waits for data to become available
       - Suitable for consuming complete result sets
       - Will retry fetching until data arrives or stream ends

    2. **Fetch methods (fetchone/fetchmany/fetchall):**
       - Snapshot mode: Blocking (traditional DB-API behavior)
       - Streaming mode: Non-blocking for fetchone/fetchmany (single request max)
       - Use cursor.may_have_results to check if more data might come

    For streaming queries, choose based on your use case:
    - Continuous consumption: Use iteration
    - Polling/async patterns: Use fetch methods with may_have_results

    Result Type Determination
    -------------------------
    The cursor exposes properties to help determine what type of results will be returned:

    - `cursor.as_dict`: True if rows are dicts, False if tuples
    - `cursor.execution_mode`: SNAPSHOT or STREAMING_QUERY
    - `cursor.is_streaming`: True if in streaming mode (convenience for execution_mode check)
    - `cursor.returns_changelog`: True if results are ChangeloggedRow namedtuples
    - `cursor.statement.is_append_only`: True if only INSERT operations (after execute)

    Example:
        cursor.execute("SELECT * FROM orders")
        if cursor.returns_changelog:
            # Results are ChangeloggedRow(op=..., row=...)
            row = cursor.fetchone()
            if row:
                op, data = row  # Unpack operation and row data
        else:
            # Results are plain tuples or dicts
            row = cursor.fetchone()  # tuple or dict based on cursor.as_dict
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

        # Statement execution state
        self._statement: Statement | None = None
        self._changelog_processor: AppendOnlyChangelogProcessor | RawChangelogProcessor | None = (
            None
        )

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
        statement_label: str | None = None,
    ) -> None:
        """
        Execute a SQL statement.

        Args:
            statement_text: The SQL statement to execute
            parameters: Parameters for the SQL statement (optional)
            timeout: Maximum time to wait for statement completion in seconds (default: 3000)
            statement_name: Optional name for the statement (defaults to DB-API UUID
                            if not provided)
            statement_label: Optional label for the statement. Labels can be used to
                            group and manage related statements. The label will be
                            prefixed with "user.confluent.io/" when stored but you only
                            need to provide the label value itself (e.g., "my-batch-job").
                            Use Connection.list_statements(label=...) to retrieve statements
                            by label.

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
        self._changelog_processor = None
        self._results = []
        self._index = 0
        self.rowcount = -1
        self._next_page = None

        # Now submit the statement and wait for it to be ready
        self._statement = self._submit_statement(
            statement_text, parameters, statement_name, statement_label
        )

        if self._statement.is_failed:
            raise OperationalError(
                f"Statement submission failed: {self._statement.status.get('detail', '')}"
            )  # pragma: no cover

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
                f"Cannot fetch results in {self._execution_mode}. "
                "DDL statements do not produce result sets."
            )

    def _get_changelog_processor(self) -> ChangelogProcessor[Any]:
        """Raise if changelog processor is not initialized, which should be the case if the
        statement is not append-only or if we haven't successfully waited for the statement to
        be ready."""
        if self._changelog_processor is None:
            raise InterfaceError(
                "Changelog processor not initialized. This likely means the statement"
                " is not ready for fetching results yet."
            )  # pragma: no cover

        return self._changelog_processor

    def fetchone(self) -> ResultRow | None:
        """
        Fetch the next row of a query result set.

        Behavior depends on execution mode:
        - Snapshot mode with bounded queries: Uses traditional blocking behavior,
          fetching additional pages if needed to return a row.
        - Streaming mode or changelog queries: Non-blocking, makes at most one
          server request and returns None if no data is immediately available.

        Important for streaming mode:
            When fetchone() returns None, check cursor.may_have_results to determine:
            - If may_have_results is True: No data currently available, but more may
              arrive later. Continue polling.
            - If may_have_results is False: End of results reached. No more data will
              arrive.

        Example (streaming mode):
            while True:
                row = cursor.fetchone()
                if row is not None:
                    process_row(row)
                elif not cursor.may_have_results:
                    break  # End of results
                else:
                    time.sleep(0.1)  # Wait before polling again

        Returns:
            A single row (tuple or dict based on cursor settings) or None if no
            rows are available.

        Raises:
            InterfaceError: If cursor is closed or in DDL mode.
        """
        self._raise_if_closed()
        self._raise_if_ddl_mode()

        return self._get_changelog_processor().fetchone()

    def fetchmany(self, size: int | None = None) -> list[ResultRow]:
        """
        Fetch up to 'size' rows from the query result set.

        Behavior depends on execution mode:
        - Snapshot mode with bounded queries: Uses traditional blocking behavior,
          fetching multiple pages if needed to return up to 'size' rows.
        - Streaming mode or changelog queries: Non-blocking, makes at most one
          server request and may return fewer rows than requested.

        Important for streaming mode:
            When fetchmany() returns an empty list, check cursor.may_have_results to
            determine:
            - If may_have_results is True: No data currently available, but more may
              arrive later. Continue polling.
            - If may_have_results is False: End of results reached. No more data will
              arrive.

        Example (streaming mode):
            while cursor.may_have_results:
                rows = cursor.fetchmany(10)
                if rows:
                    for row in rows:
                        process_row(row)
                else:
                    time.sleep(0.1)  # Wait before polling again

        Args:
            size: Maximum number of rows to return. If None, uses cursor.arraysize.

        Returns:
            List of 0 to 'size' rows. In snapshot mode, will attempt to return
            exactly 'size' rows if available. In streaming mode, returns whatever
            is immediately available (possibly empty list).
            Rows are returned as tuples or dicts based on cursor settings.

        Raises:
            InterfaceError: If cursor is closed or in DDL mode.
        """
        self._raise_if_closed()
        self._raise_if_ddl_mode()

        return self._get_changelog_processor().fetchmany(
            size if size is not None else self.arraysize
        )

    def fetchall(self) -> list[ResultRow]:
        """
        Fetch all remaining rows of a query result, blocking until complete.

        This method will fetch all remaining pages from the server and accumulate
        them in memory. Unlike fetchone() and fetchmany(), this method blocks and
        makes multiple server requests as needed to retrieve all results.

        Warning:
            This downloads the entire remaining result set into memory.
            For large result sets, consider using iteration or fetchmany()
            to process results in batches.

        Returns:
            A list of all remaining rows (tuples or dicts based on cursor settings).
            Returns an empty list if no rows are available.

        Raises:
            InterfaceError: If cursor is closed or in DDL mode.
            NotSupportedError: If called on an unbounded streaming statement,
                since fetchall() would never complete.
        """
        self._raise_if_closed()
        self._raise_if_ddl_mode()

        return self._get_changelog_processor().fetchall()

    def __iter__(self) -> Iterator[ResultRow]:
        """Return the cursor as an iterator, so that our __next__ can ensure .close() checks."""
        self._raise_if_closed()
        self._raise_if_ddl_mode()
        return self

    def __next__(self) -> ResultRow:
        """Defer to the changelog processor's iterator after proving
        the cursor is not yet closed."""
        self._raise_if_closed()
        return self._get_changelog_processor().__next__()

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
                    # Delete the statement server-side. Our handle on it will then smell
                    # `.is_deleted` as true.
                    self.delete_statement()
                except Exception as e:
                    logger.error(
                        f"Error deleting statement {self._statement.name} during cursor close: {e}"
                    )

            self.rowcount = -1
            self._closed = True
            self._results = []
            self._index = 0
            self._changelog_processor = None

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
        Check if there may be results available to fetch.

        This property is essential for streaming mode to distinguish between:
        - Temporary emptiness: fetchone() returns None but may_have_results is True
          (more data might arrive later)
        - Permanent end: fetchone() returns None and may_have_results is False
          (no more data will ever arrive)

        Returns:
            True if:
            - Results are buffered locally, OR
            - More pages might be available from the server, OR
            - We haven't fetched any pages yet (initial state)
            False if we've fetched at least once and know no more results exist.
        """
        return (
            self._statement is not None
            and self._statement.schema is not None
            and self._get_changelog_processor().may_have_results
        )

    @property
    def metrics(self) -> FetchMetrics:
        """Return the current fetch metrics from the changelog processor, if available."""
        if self._changelog_processor is None:
            raise InterfaceError(
                "No changelog processor initialized, cannot get metrics."
            )  # pragma: no cover
        return self._changelog_processor.metrics

    @property
    def as_dict(self) -> bool:
        """
        Check if the cursor returns row data as dictionaries.

        Returns:
            True if rows are returned as dicts, False if as tuples.
            This applies to the row data portion whether in plain rows or
            within ChangeloggedRow namedtuples.

        Example:
            cursor = conn.cursor(as_dict=True)
            if cursor.as_dict:
                print("Results will have column names as dict keys")
        """
        return self._results_as_dicts

    @property
    def execution_mode(self) -> ExecutionMode:
        """
        Get the execution mode of this cursor.

        Returns:
            ExecutionMode.SNAPSHOT for bounded queries or
            ExecutionMode.STREAMING_QUERY for continuous/unbounded queries.

        Example:
            cursor = conn.cursor(mode=ExecutionMode.STREAMING_QUERY)
            if cursor.execution_mode == ExecutionMode.STREAMING_QUERY:
                print("This is a streaming cursor")
        """
        return self._execution_mode

    @property
    def is_streaming(self) -> bool:
        """
        Check if this cursor is in streaming mode.

        This is a convenience property equivalent to checking:
        `cursor.execution_mode == ExecutionMode.STREAMING_QUERY`

        Returns:
            True if the cursor is in streaming mode, False if in snapshot mode.

        Example:
            cursor = conn.streaming_cursor()
            if cursor.is_streaming:
                # Use non-blocking fetch pattern for streaming
                while cursor.may_have_results:
                    row = cursor.fetchone()
                    if row:
                        process(row)
                    else:
                        time.sleep(0.1)
            else:
                # Use standard blocking fetch for snapshot
                for row in cursor:
                    process(row)
        """
        return self._execution_mode == ExecutionMode.STREAMING_QUERY

    @property
    def returns_changelog(self) -> bool:
        """
        Check if the cursor returns changelog events with row data.

        This property helps determine the result structure:
        - True: Results are ChangeloggedRow namedtuples with (op, row)
        - False: Results are plain rows (tuples or dicts)

        For streaming non-append-only queries, this will be True,
        meaning each result includes an operation type (INSERT, DELETE,
        UPDATE_BEFORE, UPDATE_AFTER) along with the row data.

        Returns:
            True if results include changelog operations, False otherwise.
            Returns False if no statement has been executed yet.

        Example:
            cursor.execute("SELECT user_id, COUNT(*) FROM orders GROUP BY user_id")
            if cursor.returns_changelog:
                row = cursor.fetchone()
                if row:
                    op, data = row  # ChangeloggedRow unpacks to (operation, row_data)
                    print(f"Operation: {op}, Data: {data}")
            else:
                row = cursor.fetchone()  # Plain tuple or dict
        """
        if self._statement is None:
            return False
        # Changelog results occur when:
        # 1. Query is streaming (not snapshot), AND
        # 2. Query is not append-only (has updates/deletes)
        return self.is_streaming and not self._statement.is_append_only

    def changelog_compressor(self) -> ChangelogCompressor:
        """Create a changelog compressor for streaming non-append-only results.

        The compressor accumulates and applies changelog operations to maintain
        a logical result set that changes over time.

        Returns:
            A ChangelogCompressor instance appropriate for this cursor's configuration.

        Raises:
            InterfaceError: If the cursor is not configured for changelog results or
                           if there is no statement.
        """
        if not self._statement:
            raise InterfaceError("Cannot create changelog compressor without a statement")

        return create_changelog_compressor(self, self._statement)

    def _raise_if_closed(self) -> None:
        """Raise InterfaceError if the cursor or connection is closed."""
        if self._closed:
            raise InterfaceError("Cursor is closed")
        if self._connection.is_closed:
            raise InterfaceError("Connection is closed")

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
            )  # pragma: no cover

        start_time = time.monotonic()
        base_delay = 1.0  # Start with 1 second
        max_delay = 30.0  # Maximum delay between polls
        current_delay = base_delay

        while time.monotonic() - start_time < timeout:
            logger.info(f"Checking statement '{self._statement.name}' status...")
            response = self._connection._get_statement(self._statement.name)

            self._statement = statement = Statement.from_response(self._connection, response)

            self._raise_if_statement_is_broken(statement)

            if statement.is_append_only:
                # Create changelog processor for append-only statements, will
                # return row tuples or dicts depending on self._results_as_dicts.
                self._changelog_processor = AppendOnlyChangelogProcessor(
                    self._connection,
                    self._statement,
                    self._execution_mode,
                    as_dict=self._results_as_dicts,
                )
            else:
                # Use a RawChangelogProcessor that will return pairs of the changelog
                # operation and the type-promoted row data as a dict or tuple depending
                # on self._results_as_dicts.
                self._changelog_processor = RawChangelogProcessor(
                    self._connection,
                    self._statement,
                    self._execution_mode,
                    as_dict=self._results_as_dicts,
                )

            # Determine if the statement is ready based on execution mode and statement type
            if self._execution_mode.is_streaming:
                # In streaming mode, statements are ready when RUNNING (or terminal states)
                # This handles:
                # - Regular streaming queries
                # - Streaming DDL (CTAS in streaming mode)
                # Note: CTAS is currently misreported as bounded due to a server bug
                statement_ready = statement.is_running or statement.phase in [
                    Phase.COMPLETED,
                    Phase.STOPPED,
                    Phase.FAILED,
                ]
            else:
                # In snapshot mode, use the statement's is_ready property
                # which waits for completion for bounded statements
                statement_ready = statement.is_ready

            if statement_ready:
                # Ready to possibly fetch results!
                return

            # Exponential backoff with jitter to prevent thundering herd
            jitter = random.uniform(0.75, 1.25)  # ±25% randomness
            actual_delay = current_delay * jitter
            time.sleep(actual_delay)
            current_delay = min(current_delay * 1.5, max_delay)

        raise OperationalError(f"Statement submission timed out after {timeout} seconds")

    def _raise_if_statement_is_broken(self, statement: Statement) -> None:
        """Raise an exception if the statement is in a failed, degraded, or pool-exhausted state.

        Raises:
            OperationalError: If the statement is failed or degraded.
            ComputePoolExhaustedError: If the statement is pool-exhausted (a subclass of
                OperationalError).
        """

        if statement.is_failed:
            raise OperationalError(
                f"Statement '{statement.name}' failed: {statement.status.get('detail', '')}"
            )

        # If the statement is degraded (unbounded and in a bad state), hmm.
        # For now, treat it as an error.
        if statement.is_degraded:
            raise OperationalError(
                f"Statement '{statement.name}' is in DEGRADED state: {statement.status['detail']}"
            )

        # If was submitted to an overloaded compute pool, then at this
        # time we choose to both _delete_ the statement and raise a specific
        # exception.
        if statement.is_pool_exhausted:
            statement_deleted = False
            try:
                self.delete_statement()
                statement_deleted = True
            except Exception as e:
                logger.error(f"Error deleting pool-exhausted statement {statement.name}: {e}")

            # Build message based on whether deletion succeeded
            if statement_deleted:
                deletion_msg = "The statement has been deleted."
            else:
                deletion_msg = "The statement could not be deleted and may need manual cleanup."

            # Subclass of OperationalError....
            raise ComputePoolExhaustedError(
                f"Statement '{statement.name}' was not accepted for execution due to compute"
                f" pool exhaustion. {deletion_msg} Please retry your query.",
                statement_name=statement.name,
                statement_deleted=statement_deleted,
            )

    def _submit_statement(
        self,
        statement_text: str,
        parameters: tuple | list | None = None,
        statement_name: str | None = None,
        statement_label: str | None = None,
    ) -> Statement:
        """
        Submit a SQL statement for execution.

        Args:
            operation: The SQL statement to execute
            parameters: Parameters for the SQL statement (optional)
            statement_name: Optional name for the statement (defaults to DB-API UUID if
                            not provided)
            statement_label: Optional label for the statement for easier identification in
                            server logs and UIs (defaults to None)

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
            interpolated_statement,
            self._execution_mode,
            statement_name,
            statement_label,
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
