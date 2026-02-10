"""
Changelog processor module for Confluent SQL DB-API driver.

This module provides implementations of changelog processing for streaming
queries in Confluent SQL.
"""

from __future__ import annotations

import abc
import logging
import time
from itertools import islice
from typing import TYPE_CHECKING, Generic, NamedTuple, TypeAlias, TypeVar

from .exceptions import InterfaceError, NotSupportedError
from .statement import Op, Statement
from .types import StrAnyDict, SupportedPythonTypes

if TYPE_CHECKING:
    from .connection import Connection

logger = logging.getLogger(__name__)

ProcessorOutput = TypeVar("ProcessorOutput")
"""Type that a changelog processor produces as output from its __iter__ method."""


StatementResultTuple: TypeAlias = tuple[SupportedPythonTypes, ...]
"""The tuple representation of a row of Flink statement results
   after type conversion from Results API JSON to Python types."""


ResultTupleOrDict: TypeAlias = StatementResultTuple | StrAnyDict
"""Output type for AppendOnlyChangelogProcessor fetch methods and iteration."""


class ChangelogProcessor(Generic[ProcessorOutput], abc.ABC):
    """Abstract base class for changelog processors."""

    _connection: Connection
    """The connection associated with this changelog processor."""

    _statement: Statement
    """The statement associated with this changelog processor."""

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

    _results: list[ProcessorOutput]
    """The accumulated post-from-response-api to Python and processor
    return type conversion results."""

    _index: int
    """Index of the next result to return from the local buffer.
    
    Starts at 0 and increments after each result is returned. Used to track
    position in `_results` list during iteration and fetch operations.
    """

    _most_recent_results_fetch_time: float | None
    """Timestamp of the most recent results fetch operation, in seconds since epoch.
        Used for result page fetch pacing."""

    def __init__(self, connection: Connection, statement: Statement, as_dict: bool = False):
        self._connection = connection
        self._statement = statement
        self._as_dict = as_dict

        self._next_page = None
        self._fetch_next_page_called = False
        self._most_recent_results_fetch_time = None

        self._results = []
        self._index = 0

    @abc.abstractmethod
    def _retain(self, op: Op, decoded: ResultTupleOrDict) -> None:
        """Retain the changelog row in the processor's internal state.

        This is used by RawChangelogProcessor to retain the full changelog result,
        and by AppendOnlyChangelogProcessor to retain just the row data (after validating
        that the operation is an INSERT if provided by the server).

        The exact retention logic is left to the concrete implementations since it may differ
        based on whether we are retaining full changelog results or just row data.
        """
        raise NotImplementedError("Abstract method")  # pragma: no cover

    def __iter__(self) -> ChangelogProcessor[ProcessorOutput]:
        """Returns an iterator over the processed changelog results."""
        return self

    def fetchone(self) -> ProcessorOutput | None:
        res = self._get_next_results(1)
        assert len(res) <= 1, "fetchone returned more than one result, this is probably a bug"
        # If no results are available, `res` is an empty list,
        # but we want to return None in this case: https://peps.python.org/pep-0249/#fetchone
        return res[0] if res else None

    def _map_row_to_dict(self, row: StatementResultTuple) -> StrAnyDict:
        """Map tuple row to dict using statement schema."""
        if not self._statement.schema:
            raise InterfaceError("Cannot map row to dict without schema")  # pragma: no cover
        return dict(zip([col.name for col in self._statement.schema.columns], row, strict=True))

    def _get_next_results(self, limit: int | None) -> list[ProcessorOutput]:
        """
        Retrieve up to `limit` results, fetching additional pages as needed.

        This is the core result-fetching method that all public fetch methods
        delegate to. It drives the iterator protocol (`__next__`), which in turn
        calls `_fetch_next_page()` when the local buffer is exhausted.

        Design note:
            By funneling all fetch methods through iteration, we maintain a single
            code path for buffer management and page fetching. This trades some
            per-row function call overhead for correctness and simplicity. The
            overhead is negligible compared to network I/O for fetching pages.

        Args:
            limit: Maximum number of results to return, or None for all remaining.

        Returns:
            A list of result rows (as tuples or dicts based on `as_dict` flag).

        Raises:
            InterfaceError: If limit is None and the statement is unbounded (streaming),
                since iteration would never complete.
        """
        if limit is None:
            return list(self)
        else:
            return list(islice(self, limit))

    @property
    def may_have_results(self) -> bool:
        """Whether there may be results to fetch."""
        return (
            # We haven't fetched any pages yet to know about results or next page token
            (not self._fetch_next_page_called)
            # Or we have some remaining results in the local cache
            or self._remaining > 0
            # Or we know there are more pages to fetch.
            or self._next_page is not None
        )

    def fetchmany(self, size: int) -> list[ProcessorOutput]:
        if size <= 0:
            raise InterfaceError(f"size must be a positive integer, got {size}")

        return self._get_next_results(size)

    def fetchall(self) -> list[ProcessorOutput]:
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

    def __next__(self) -> ProcessorOutput:
        """Implementation of iterator protocol."""
        if self._remaining == 0:
            if self._results and not self._next_page:
                raise StopIteration
            self._fetch_next_page()
            if self._remaining == 0:
                raise StopIteration

        # Get the row tuple from results
        res = self._results[self._index]
        self._index += 1

        return res

    @property
    def _remaining(self) -> int:
        """Number of results remaining in current buffer."""
        remaining = len(self._results) - self._index
        if remaining < 0:
            raise InterfaceError("Internal index bigger than results list.")  # pragma: no cover
        return remaining

    def _fetch_next_page(self) -> None:
        """Fetch and process the next page of results."""

        if not self._statement.is_ready:
            raise InterfaceError("Statement is not ready for result fetching.")

        if not self._statement.schema:
            raise InterfaceError("Trying to fetch results for a non-query statement")

        if not self._results or self._next_page is not None:
            if self._most_recent_results_fetch_time is not None:
                # Should we pause before fetching the next page?
                elapsed_secs = time.time() - self._most_recent_results_fetch_time
                if elapsed_secs < self._connection.statement_results_page_fetch_pause_secs:
                    # Sleep the difference between when we last fetched results
                    # and the configured pause time so that we ensure to not
                    # hit the endpoint for this statement more often than
                    # the configured pause time.
                    pause_secs = (
                        self._connection.statement_results_page_fetch_pause_secs - elapsed_secs
                    )
                    time.sleep(pause_secs)

            # Get raw ChangelogRow results from connection
            results, next_page = self._connection._get_statement_results(
                self._statement.name, self._next_page
            )
            self._next_page = next_page

            # Process each changelog row just fetched
            type_converter = self._statement.type_converter
            for res in results:
                # Convert row to Python types
                decoded_row = type_converter.to_python_row(res.row)

                # Promote from tuple -> dict if requested
                if self._as_dict:
                    decoded_row = self._map_row_to_dict(decoded_row)

                # Retain the row (and perhaps also the operation) in the processor's internal state
                self._retain(res.op, decoded_row)

        self._fetch_next_page_called = True
        self._most_recent_results_fetch_time = time.time()


class AppendOnlyChangelogProcessor(ChangelogProcessor[ResultTupleOrDict]):
    """Append-only changelog processor implementation.

    Returns statement result rows as either tuples or dicts based on the `as_dict` flag.
    """

    def _retain(self, op: Op, decoded: ResultTupleOrDict) -> None:
        """Retain the changelog row in the processor's internal state.

        For AppendOnlyChangelogProcessor, we only retain the row data (after validating
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
                f"Non-INSERT op was received by AppendOnlyChangelogProcessor: {op}. "
            )

        self._results.append(decoded)


class ChangeloggedRow(NamedTuple):
    """Changelog operation and corresponding row data after type conversion from Results API JSON
    to Python types. Returned by cursors using RawChangelogProcessor for non-append-only statements.
    """

    op: Op
    """The changelog operation type."""
    row: ResultTupleOrDict
    """The row data as either a tuple or dict based on the `as_dict` flag, after type conversion
    from Results API JSON to Python types."""


class RawChangelogProcessor(ChangelogProcessor[ChangeloggedRow]):
    """Non-append-only changelog processor implementation.

    Returns changelog results as `ChangeloggedRow` namedtuples containing both the operation
    type (op) and the either tuple-or-dict row data (`row`).

    Used for the subset of streaming statements that are not append-only, where we need to return
    the changelog operation type along with each row.

    No changelog interpretation is done at this level.
    """

    def _retain(self, op: Op, decoded: ResultTupleOrDict) -> None:
        """Retain the changelog row in the processor's internal state.

        For RawChangelogProcessor, we retain both the operation type and the row data

        Args:
            op: The changelog operation type.
            decoded: The row data as either a tuple or dict based on the `as_dict` flag,
                     after type conversion from Results API JSON to Python types.
        """

        self._results.append(ChangeloggedRow(op, decoded))
