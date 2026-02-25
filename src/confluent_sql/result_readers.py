"""
Result reading and buffering for Confluent SQL DB-API driver.

This module provides low-level result readers that fetch statement results
from the server, handle paging, and convert row data from JSON to Python types.

- AppendOnlyResultReader: For append-only streaming (or all snapshot mode)
  queries (returns row data only).
- ChangelogEventReader: For non-append-only queries -- a subset of streaming queries
  (returns ChangeloggedRow with operation + row).

These readers fetch and expose either result rows or changelog events including
result rows but do NOT apply or interpret them. These implementations back
the iteration, fetchone/fetchmany/fetchall methods of our Cursor class.

For stateful compression of changelog events into a logical result set, see
the `changelog_compressor` module, which makes use of the sequence of ChangeloggedRow
returned by ChangelogEventReader to produce a compressed result set ("interpreted changelog")
that applies the changelog operations to maintain the current state of each row in the result
and the logical result set at large over time. That functionality is exposed from the Cursor
class's `changelog_compressor()` method (only callable for non-append-only streaming statements).
"""

from __future__ import annotations

import abc
import logging
import time
from collections import deque
from dataclasses import dataclass
from itertools import islice
from typing import TYPE_CHECKING, Generic, NamedTuple, TypeAlias, TypeVar

from .exceptions import InterfaceError, NotSupportedError
from .execution_mode import ExecutionMode
from .statement import Op, Statement
from .types import StrAnyDict, SupportedPythonTypes

if TYPE_CHECKING:
    from .connection import Connection

logger = logging.getLogger(__name__)

ReaderOutput = TypeVar("ReaderOutput")
"""Type that a result reader produces as output from its __iter__ method."""


StatementResultTuple: TypeAlias = tuple[SupportedPythonTypes, ...]
"""The tuple representation of a row of Flink statement results
   after type conversion from Results API JSON to Python types."""


ResultTupleOrDict: TypeAlias = StatementResultTuple | StrAnyDict
"""Output type for AppendOnlyResultReader fetch methods and iteration."""


@dataclass()
class FetchMetrics:
    """Holds metrics related to fetch results route operations in ResultReader."""

    total_page_fetches: int = 0
    """Total number of times the reader fetched a page of results from the server."""

    total_changelog_rows_fetched: int = 0
    """Total number of changelog rows fetched from the server across all pages."""

    empty_page_fetches: int = 0
    """Number of times the reader fetched a page of results that contained no rows."""

    fetch_request_secs: float = 0.0
    """Total elapsed seconds spent on results fetch operations (excluding any pauses)."""

    paused_times: int = 0
    """Number of times the reader paused before fetching the next page of results."""

    paused_secs: float = 0.0
    """Total number of seconds the reader spent paused before fetching pages."""

    _before_fetch_timestamp: float | None = None
    """Internal timestamp to track when a fetch operation started, used for metrics calculation."""

    @property
    def avg_rows_per_page(self) -> float:
        """Average number of changelog rows fetched per page."""
        if self.total_page_fetches == 0:
            return 0.0
        return self.total_changelog_rows_fetched / self.total_page_fetches

    def paused_before_fetch(self, pause_secs: float) -> None:
        """Record that the reader paused for a some time before fetching results."""
        self.paused_times += 1
        self.paused_secs += pause_secs

    def prep_for_fetch(self) -> None:
        """Call before starting a fetch operation to record the start time."""
        self._before_fetch_timestamp = time.monotonic()

    def record_fetch_completion(self, rows_fetched: int) -> None:
        """Call after completing a fetch operation to update metrics.

        Args:
            rows_fetched: The number of changelog rows fetched in the completed operation.
        """
        if self._before_fetch_timestamp is None:
            raise InterfaceError(
                "prep_for_fetch() must be called before recording fetch completion."
            )  # pragma: no cover
        elapsed_secs = time.monotonic() - self._before_fetch_timestamp
        self.fetch_request_secs += elapsed_secs
        self.total_changelog_rows_fetched += rows_fetched
        self.total_page_fetches += 1
        if rows_fetched == 0:
            self.empty_page_fetches += 1
        self._before_fetch_timestamp = None


class ResultReader(Generic[ReaderOutput], abc.ABC):
    """Abstract base class for result readers.

    Important: Iteration vs Fetch Methods Behavior
    ------------------------------------------------
    This reader provides two ways to consume results, with different
    blocking behaviors in streaming mode:

    1. **Iteration (for row in reader):**
       - Always blocking in both snapshot and streaming modes
       - Waits for data to become available or until definitively complete
       - Suitable for consuming complete result sets
       - Will retry fetching pages until data arrives

    2. **Fetch methods (fetchone/fetchmany):**
       - Blocking in snapshot mode (traditional DB-API behavior)
       - Non-blocking in streaming mode (at most one server request)
       - Suitable for polling patterns in streaming applications
       - Use may_have_results to distinguish temporary vs permanent emptiness

    Recommendations:
    - For snapshot queries: Use either approach (both block until complete)
    - For streaming queries: Use fetch methods for polling, iteration for continuous consumption
    """

    _connection: Connection
    """The connection associated with this result reader."""

    _statement: Statement
    """The statement associated with this result reader."""

    _as_dict: bool
    """Whether to return the row portion of results as dicts or tuples."""

    _fetch_next_page_called: bool
    """Whether _fetch_next_page has been called at least once.
        TODO: discard in favor of _most_recent_results_fetch_time
    """

    _next_page: str | None
    """The URL of the next page of results, if any. 

       Initial state is None, but may also be set to None after fetching a page if there are
       no more pages to fetch (distinguished from the initial state by
       `_fetch_next_page_called` being set to `True`)."""

    _results: deque[ReaderOutput]
    """Deque of unconsumed results. Rows are removed via popleft() as they
    are consumed, freeing memory incrementally."""

    _most_recent_results_fetch_time: float | None
    """Timestamp of the most recent results fetch operation, in seconds since epoch.
        Used for result page fetch pacing."""

    _metrics: FetchMetrics
    """Metrics related to results fetching operations"""

    _execution_mode: ExecutionMode
    """The execution mode for this reader (snapshot vs streaming)."""

    def __init__(
        self,
        connection: Connection,
        statement: Statement,
        execution_mode: ExecutionMode,
        as_dict: bool = False,
    ):
        self._connection = connection
        self._statement = statement
        self._execution_mode = execution_mode
        self._as_dict = as_dict

        self._next_page = None
        self._fetch_next_page_called = False
        self._most_recent_results_fetch_time = None

        self._results: deque[ReaderOutput] = deque()
        self._metrics = FetchMetrics()

    @abc.abstractmethod
    def _retain(self, op: Op, decoded: ResultTupleOrDict) -> None:
        """Retain the changelog row in the reader's internal state.

        This is used by ChangelogEventReader to retain the full changelog result,
        and by AppendOnlyResultReader to retain just the row data (after validating
        that the operation is an INSERT if provided by the server).

        The exact retention logic is left to the concrete implementations since it may differ
        based on whether we are retaining full changelog results or just row data.
        """
        raise NotImplementedError("Abstract method")  # pragma: no cover

    def __iter__(self) -> ResultReader[ReaderOutput]:
        """Returns an iterator over the result reader results.

        Important: Iteration always uses blocking behavior, even in streaming mode.
        When the buffer is empty, iteration will fetch and wait for more data to
        become available. This differs from fetchone/fetchmany which are non-blocking
        in streaming mode.

        For streaming queries where non-blocking behavior is desired, use fetchone()
        or fetchmany() in a polling loop instead of iteration.
        """
        return self

    def fetchone(self) -> ReaderOutput | None:
        """
        Fetch the next row from the query result.

        Behavior depends on execution mode:
        - Snapshot mode: Blocking behavior, may fetch multiple pages to return a row
        - Streaming mode: Non-blocking, fetches at most one page per call

        Returns:
            A single row or None if no rows are available.
            In streaming mode, use may_have_results property to distinguish between
            temporary emptiness (more data may come) and end of results.

        Note: This non-blocking behavior in streaming mode differs from iteration.
        When iterating (for row in reader), the reader will block waiting
        for data. Use fetchone() in a polling loop for non-blocking streaming:

        Example:
            # Streaming mode polling pattern
            while reader.may_have_results:
                row = reader.fetchone()
                if row is not None:
                    process(row)
                else:
                    # No data right now, could sleep/yield control
                    time.sleep(0.1)
        """
        res = self._get_next_results(1)
        assert len(res) <= 1, "fetchone returned more than one result, this is probably a bug"
        # If no results are available, `res` is an empty list,
        # but we want to return None in this case: https://peps.python.org/pep-0249/#fetchone
        return res[0] if res else None

    @property
    def metrics(self) -> FetchMetrics:
        """Return the current metrics over results fetching activity."""
        return self._metrics

    def _map_row_to_dict(self, row: StatementResultTuple) -> StrAnyDict:
        """Map tuple row to dict using statement schema."""
        if not self._statement.schema:
            raise InterfaceError("Cannot map row to dict without schema")  # pragma: no cover
        return dict(zip([col.name for col in self._statement.schema.columns], row, strict=True))

    def _consume_from_buffer(self, limit: int) -> list[ReaderOutput]:
        """
        Consume up to 'limit' results from the deque buffer.

        Uses popleft() to remove rows from the front of the deque. This is destructive
        consumption: once removed, rows cannot be re-accessed. Memory is released
        incrementally as deque blocks become empty (approximately every 64 popleft()
        calls in CPython). This automatic freeing eliminates the need for manual
        buffer clearing and enables O(page_size) memory usage instead of O(total_rows).

        Args:
            limit: Maximum number of results to consume from buffer.

        Returns:
            List of up to 'limit' results from the buffer.
        """
        actual_limit = min(limit, len(self._results))
        consumed = []
        for _ in range(actual_limit):
            consumed.append(self._results.popleft())
        return consumed

    def _get_next_results(self, limit: int | None) -> list[ReaderOutput]:
        """
        Retrieve up to `limit` results, with behavior depending on execution mode.

        This is the core result-fetching method that all public fetch methods
        delegate to. The behavior differs based on execution mode:

        - Snapshot mode: Uses blocking behavior for bounded result sets, fetching
          multiple pages if needed to satisfy the requested limit (traditional
          DB-API behavior).
        - Streaming mode: Non-blocking, makes at most one request to the server,
          returning whatever is available (suitable for polling).
        - Unlimited fetches (fetchall): Always blocking.

        Args:
            limit: Maximum number of results to return, or None for all remaining.

        Returns:
            A list of result rows (as tuples or dicts based on `as_dict` flag).
            Behavior depends on mode and reader type.

        Raises:
            InterfaceError: If limit is None and the statement is unbounded (streaming),
                since iteration would never complete.
        """
        if limit is None:
            # fetchall() - maintain blocking behavior to fetch everything
            return list(self)

        # Determine if we should use blocking behavior
        # Snapshot mode uses traditional blocking behavior for bounded result sets
        # Streaming mode uses non-blocking for efficient polling
        if self._execution_mode.is_snapshot:
            # Traditional blocking behavior for snapshot mode with append-only
            # Use iteration to fetch as many pages as needed to satisfy limit
            return list(islice(self, limit))

        # Non-blocking behavior for streaming mode or changelog processing
        # Check if we have buffered results (unconsumed rows in deque)
        if len(self._results) > 0:
            return self._consume_from_buffer(limit)

        # Buffer is empty - check if we can fetch more
        if self._fetch_next_page_called and self._next_page is None:
            # We've already fetched before and there are no more pages
            return []

        # Try to fetch one page of results
        self._fetch_next_page()

        # Return up to 'limit' results from what we just fetched (check unconsumed rows in deque)
        if len(self._results) > 0:
            return self._consume_from_buffer(limit)

        return []  # Fetched but got no results

    @property
    def may_have_results(self) -> bool:
        """Whether there may be results to fetch."""
        return (
            # We haven't fetched any pages yet to know about results or next page token
            (not self._fetch_next_page_called)
            # Or we have unconsumed results in the local buffer (deque)
            or len(self._results) > 0
            # Or we know there are more pages to fetch.
            or self._next_page is not None
        )

    def fetchmany(self, size: int) -> list[ReaderOutput]:
        """
        Fetch up to 'size' rows from the query result.

        Behavior depends on execution mode:
        - Snapshot mode: Blocking behavior, may fetch multiple pages to satisfy count
        - Streaming mode: Non-blocking, fetches at most one page per call and may
          return fewer rows than requested (including an empty list)

        Use may_have_results property to distinguish between temporary emptiness
        and end of results in streaming mode.

        Note: This non-blocking behavior in streaming mode differs from iteration.
        Direct iteration will block waiting for data to fill the requested size.

        Example:
            # Streaming mode batch polling pattern
            while reader.may_have_results:
                batch = reader.fetchmany(100)
                if batch:
                    for row in batch:
                        process(row)
                else:
                    # No data available right now
                    time.sleep(0.1)

        Args:
            size: Maximum number of rows to return. Must be positive.

        Returns:
            List of 0 to 'size' rows, depending on what's available.
            In streaming mode, may return empty list even if more data will come.

        Raises:
            InterfaceError: If size is not positive.
        """
        if size <= 0:
            raise InterfaceError(f"size must be a positive integer, got {size}")

        return self._get_next_results(size)

    def fetchall(self) -> list[ReaderOutput]:
        """
        Fetch all remaining rows of a query result.

        This method will fetch all remaining pages from the server
        and accumulate them in memory.

        Warning:
            This downloads the entire remaining result set into memory.
            For large result sets, consider using iteration or `fetchmany()`
            to process results in batches.

        Returns:
            A list of tuples or dicts (based on `as_dict` flag).
            Returns an empty list if no rows are available.

        Raises:
            NotSupportedError: If called on an unbounded streaming statement,
                since fetchall() would never complete.

        See Also:
            https://peps.python.org/pep-0249/#fetchall
        """

        if self._statement.is_bounded is False:
            raise NotSupportedError(
                "Cannot call fetchall() on an unbounded streaming statement. "
                "Use fetchone(), fetchmany(), or iterate with a limit instead."
            )

        return self._get_next_results(None)

    def __next__(self) -> ReaderOutput:
        """Implementation of iterator protocol.

        This method implements blocking iteration behavior:
        - When the buffer is empty, it calls _fetch_next_page() to get more data
        - Raises StopIteration only when no more results will ever be available
        - In streaming mode, this means iteration will block/wait for new data

        Note: This blocking behavior differs from fetchone/fetchmany in streaming
        mode, which return immediately with None/empty list if no data is buffered.
        """
        if len(self._results) == 0:
            # Detect iteration exhaustion: we've fetched before AND there's nowhere
            # else to fetch from. Using _fetch_next_page_called flag rather than
            # buffer state because deque's destructive popleft() consumption leaves
            # an empty deque indistinguishable from "never fetched yet".
            if self._fetch_next_page_called and not self._next_page:
                raise StopIteration
            self._fetch_next_page()
            if len(self._results) == 0:
                raise StopIteration

        return self._results.popleft()

    def _fetch_next_page(self) -> None:
        """Fetch and process the next page of results."""

        if not self._statement.can_fetch_results(self._execution_mode):
            raise InterfaceError("Statement is not ready for result fetching.")

        if not self._statement.schema:
            raise InterfaceError("Trying to fetch results for a non-query statement")

        if not self._results or self._next_page is not None:
            if self._most_recent_results_fetch_time is not None:
                # Should we pause before fetching the next page?
                elapsed_secs = time.monotonic() - self._most_recent_results_fetch_time
                if elapsed_secs < self._connection.statement_results_page_fetch_pause_secs:
                    # Sleep the difference between when we last fetched results
                    # and the configured pause time so that we ensure to not
                    # hit the endpoint for this statement more often than
                    # the configured pause time.
                    pause_secs = (
                        self._connection.statement_results_page_fetch_pause_secs - elapsed_secs
                    )
                    time.sleep(pause_secs)
                    self._metrics.paused_before_fetch(pause_secs)

            self._metrics.prep_for_fetch()

            # Get raw ChangelogRow results from connection
            results, next_page = self._connection._get_statement_results(
                self._statement.name, self._next_page
            )

            self._metrics.record_fetch_completion(len(results))

            self._next_page = next_page

            # Process each changelog row just fetched
            type_converter = self._statement.type_converter
            for res in results:
                # Convert row to Python types
                decoded_row = type_converter.to_python_row(res.row)

                # Promote from tuple -> dict if requested
                if self._as_dict:
                    decoded_row = self._map_row_to_dict(decoded_row)

                # Retain the row (and perhaps also the operation) in the reader's internal state
                self._retain(res.op, decoded_row)

        self._fetch_next_page_called = True
        self._most_recent_results_fetch_time = time.monotonic()


class AppendOnlyResultReader(ResultReader[ResultTupleOrDict]):
    """Append-only result reader implementation.

    Returns statement result rows as either tuples or dicts based on the `as_dict` flag.
    """

    def _retain(self, op: Op, decoded: ResultTupleOrDict) -> None:
        """Retain the changelog row in the reader's internal state.

        For AppendOnlyResultReader, we only retain the row data (after validating
        that the operation is an INSERT if provided by the server), since we only return
        the row data in fetch and iteration methods.

        Raise NotSupportedError if a non-INSERT operation is encountered.

        Args:
            op: The changelog operation type.
            decoded: The row data as either a tuple or dict based on the `as_dict` flag,
                     after type conversion from Results API JSON to Python types.
        """
        if op is not None and op != Op.INSERT:
            # Only expect INSERT operations for append-only
            logger.error(f"Received non-INSERT op {op} in results for append-only statement.")
            raise NotSupportedError(
                f"Non-INSERT op was received by AppendOnlyResultReader: {op}. "
            )

        self._results.append(decoded)


class ChangeloggedRow(NamedTuple):
    """Changelog operation and corresponding row data after type conversion from Results API JSON
    to Python types. Returned by cursors using ChangelogEventReader for non-append-only statements.
    """

    op: Op
    """The changelog operation type."""
    row: ResultTupleOrDict
    """The row data as either a tuple or dict based on the `as_dict` flag, after type conversion
    from Results API JSON to Python types."""


class ChangelogEventReader(ResultReader[ChangeloggedRow]):
    """Non-append-only changelog event reader implementation.

    Returns changelog results as `ChangeloggedRow` namedtuples containing both the operation
    type (op) and the tuple or dict row data (`row`).

    Used for the subset of streaming statements that are not append-only, where we need to return
    the changelog operation type along with each row.

    No changelog interpretation is done at this level.
    """

    def _retain(self, op: Op, decoded: ResultTupleOrDict) -> None:
        """Retain the changelog row in the reader's internal state.

        For ChangelogEventReader, we retain both the operation type and the row data

        Args:
            op: The changelog operation type.
            decoded: The row data as either a tuple or dict based on the `as_dict` flag,
                     after type conversion from Results API JSON to Python types.
        """

        self._results.append(ChangeloggedRow(op, decoded))
