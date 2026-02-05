"""
Changelog processor module for Confluent SQL DB-API driver.

This module provides implementations of changelog processing for streaming
queries in Confluent SQL.
"""

from __future__ import annotations

import abc
import logging
from itertools import islice
from typing import TYPE_CHECKING, Generic, TypeAlias, TypeVar

from .exceptions import InterfaceError, NotSupportedError
from .statement import Op, Statement
from .types import StrAnyDict, SupportedPythonTypes

if TYPE_CHECKING:
    from .connection import Connection

logger = logging.getLogger(__name__)

ProcessorOutput = TypeVar("ProcessorOutput")
"""Type that a changelog processor produces as output from its __iter__ method."""


class ChangelogProcessor(Generic[ProcessorOutput], abc.ABC):
    """Abstract base class for changelog processors."""

    _connection: Connection
    """The connection associated with this changelog processor."""

    def __init__(self, connection: Connection):
        self._connection = connection

    def __iter__(self) -> ChangelogProcessor[ProcessorOutput]:
        """Returns an iterator over the processed changelog results."""
        return self

    @abc.abstractmethod
    def __next__(self) -> ProcessorOutput:
        """Returns an iterator over the processed changelog results."""
        pass  # pragma: no cover

    @abc.abstractmethod
    def fetchone(self) -> ProcessorOutput | None:
        """Returns the next result, or None if there are no more results."""
        pass  # pragma: no cover

    @abc.abstractmethod
    def fetchmany(self, size: int) -> list[ProcessorOutput]:
        """Returns the next size results. Cursor will always be calling with
        size > 0.
        """
        pass  # pragma: no cover

    @abc.abstractmethod
    def fetchall(self) -> list[ProcessorOutput]:
        """Returns all remaining results. Use with caution, as this may consume a lot of memory."""
        pass  # pragma: no cover

    @property
    @abc.abstractmethod
    def may_have_results(self) -> bool:
        """Whether there may be more results to fetch."""
        pass  # pragma: no cover


StatementResultTuple: TypeAlias = tuple[SupportedPythonTypes]
"""__next__ output type for AppendOnlyChangelogProcessor."""


class AppendOnlyChangelogProcessor(ChangelogProcessor[StatementResultTuple | StrAnyDict]):
    """Append-only changelog processor implementation.

    Returns statement result rows as either tuples or dicts based on the `as_dict` flag.
    """

    _rows: list[StatementResultTuple]
    """The accumulated post-python type conversion results."""

    _as_dict: bool
    """Whether to return results as dicts or tuples."""

    _fetch_next_page_called: bool
    """Whether _fetch_next_page has been called at least once."""

    _next_page: str | None
    """The URL of the next page of results, if any. 

       Initial state is None, but may also be set to None after fetching a page if there are
       no more pages to fetch (distinguished from the initial state by
       `_fetch_next_page_called` being set to `True`)."""

    def __init__(self, connection: Connection, statement: Statement, as_dict: bool = False):
        super().__init__(connection)
        self._statement = statement
        self._statement_name = statement.name
        self._as_dict = as_dict
        self._results: list[StatementResultTuple] = []
        self._index = 0
        self._next_page = None
        self._fetch_next_page_called = False

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

    def fetchone(self) -> StrAnyDict | StatementResultTuple | None:
        res = self._get_next_results(1)
        assert len(res) <= 1, "fetchone returned more than one result, this is probably a bug"
        # If no results are available, `res` is an empty list,
        # but we want to return None in this case: https://peps.python.org/pep-0249/#fetchone
        return res[0] if res else None

    def fetchmany(self, size: int) -> list[StrAnyDict | StatementResultTuple]:
        if size <= 0:
            raise InterfaceError(f"size must be a non-negative integer, got {size}")

        return self._get_next_results(size)

    def fetchall(self) -> list[StrAnyDict | StatementResultTuple]:
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

    def __next__(self) -> StrAnyDict | StatementResultTuple:
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

        # Return as dict if requested
        if self._as_dict and self._statement.schema:
            return self._map_row_to_dict(res)
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
            # Get raw ChangelogRow results from connection
            results, next_page = self._connection._get_statement_results(
                self._statement_name, self._next_page
            )
            self._next_page = next_page

            # Process each result
            type_converter = self._statement.type_converter
            for res in results:
                # Convert row to Python types
                decoded_row = type_converter.to_python_row(res.row)

                # Validate that the operation is INSERT if provided, since here in
                # AppendOnlyChangelogProcessor we only expect INSERT operations.
                if res.op is not None and res.op != Op.INSERT:
                    # Only expect INSERT operations for append-only
                    logger.error(
                        f"Received non-INSERT op {res.op} in results for append-only statement."
                    )
                    raise NotImplementedError(
                        f"Non-INSERT op was received by AppendOnlyChangelogProcessor: {res.op}. "
                    )

                self._results.append(decoded_row)

        self._fetch_next_page_called = True

    def _get_next_results(self, limit: int | None) -> list[StrAnyDict | StatementResultTuple]:
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

    def _map_row_to_dict(self, row: tuple[SupportedPythonTypes]) -> StrAnyDict:
        """Map tuple row to dict using statement schema."""
        if not self._statement.schema:
            raise InterfaceError("Cannot map row to dict without schema")  # pragma: no cover
        return dict(zip([col.name for col in self._statement.schema.columns], row, strict=True))
