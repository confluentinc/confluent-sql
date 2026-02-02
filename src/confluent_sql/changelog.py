"""
Changelog processor module for Confluent SQL DB-API driver.

This module provides implementations of changelog processing for streaming
queries in Confluent SQL.
"""

from __future__ import annotations

import abc
import logging
from itertools import islice
from typing import Generic, TypeAlias, TypeVar

from .exceptions import InterfaceError

from .connection import Connection
from .statement import ChangelogRow, Op, Statement
from .types import StrAnyDict, SupportedPythonTypes

logger = logging.getLogger(__name__)

ProcessorOutput = TypeVar("ProcessorOutput")
"""Type that a changelog processor produces as output from its __iter__ method."""


class ChangelogProcessor(Generic[ProcessorOutput], abc.ABC):
    """Abstract base class for changelog processors."""

    _connection: Connection
    """The connection associated with this changelog processor."""
    _next_page: str | None
    """The URL next page of results, if any."""

    def __init__(self, connection: Connection):
        self._connection = connection

    def __iter__(self) -> ChangelogProcessor[ProcessorOutput]:
        """Returns an iterator over the processed changelog results."""
        return self
    
    @abc.abstractmethod
    def __next__(self) -> ProcessorOutput:
        """Returns an iterator over the processed changelog results."""
        pass


StatementResultTuple: TypeAlias = tuple[SupportedPythonTypes]
"""__next__ output type for AppendOnlyChangelogProcessor."""


class AppendOnlyChangelogProcessor(ChangelogProcessor[StatementResultTuple]):
    _rows: list[StatementResultTuple]
    """The accumulated post-python type conversion results."""

    _as_dict: bool
    """Whether to return results as dicts or tuples."""

    def __init__(self, connection: Connection, statement: Statement, as_dict: bool = False):
        super().__init__(connection)
        self._statement = statement
        self._statement_name = statement.name
        self._as_dict = as_dict
        self._results: list[dict[str, tuple[SupportedPythonTypes] | Op]] = []
        self._index = 0
        self._next_page = None  # Already declared in base class

    def fetchone(self) -> StrAnyDict | StatementResultTuple | None:
        res = self._get_next_results(1)
        assert len(res) <= 1, "fetchone returned more than one result, this is probably a bug"
        # If no results are available, `res` is an empty list,
        # but we want to return None in this case: https://peps.python.org/pep-0249/#fetchone
        return res[0] if res else None

    def fetchmany(self, size: int | None = None) -> list[StrAnyDict | StatementResultTuple]:
        if size is None:
            size = 1 # used to be self.arraysize, but it was never set to anything else
        if size <= 0:
            raise InterfaceError(f"size must be a non-negative integer, got {size}")

        return self._get_next_results(size)

    def fetchall(self) -> list[StrAnyDict | StatementResultTuple]:
        """
        Fetch all the results from the current cursor.
        Beware that this will download and put into memory all the available results.
        Make sure the result set can fit in memory, and that you are not making too many calls
        at once to fetch the whole result set.
        If you want more control, use the cursor as an iterator, or use `fetchone`/`fetchmany`
        to fetch results one-by-one or in batches.
        """

        return self._get_next_results()

    def __next__(self) -> StrAnyDict | StatementResultTuple:
        """Implementation of iterator protocol."""
        if self._remaining == 0:
            if self._results and not self._next_page:
                raise StopIteration
            self._fetch_next_page()
            if self._remaining == 0:
                raise StopIteration

        # Get the row tuple from results
        res = self._results[self._index]["row"]
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
            raise InterfaceError("Internal index bigger than results list.")
        return remaining

    def _fetch_next_page(self) -> None:
        """Fetch and process the next page of results."""
        if not self._results or self._next_page is not None:
            # Get raw ChangelogRow results from connection
            results, next_page = self._connection._get_statement_results(
                self._statement_name, self._next_page
            )
            self._next_page = next_page

            # Process each result - this is the logic from cursor.py lines 330-348
            type_converter = self._statement.type_converter
            for res in results:
                # Convert row to Python types
                decoded_row = type_converter.to_python_row(res.row)

                # Build result dict with row and optional op
                row: dict[str, tuple[SupportedPythonTypes] | Op] = {"row": decoded_row}

                # Handle changelog operation metadata
                if res.op is not None:
                    # Only allow INSERT operations for append-only
                    if res.op != Op.INSERT:
                        logger.error(
                            f"Received non-INSERT op {res.op} in results, not smart enough "
                            f"to handle this yet."
                        )
                        raise NotImplementedError("Only INSERT op is supported in results for now.")
                    row["op"] = res.op

                self._results.append(row)

    def _get_next_results(self, limit: int | None = None) -> list[StrAnyDict | StatementResultTuple]:
        """Get up to limit results, or all if limit is None."""
        if limit is None:
            return list(self)
        else:
            return list(islice(self, limit))

    def _map_row_to_dict(self, row: tuple[SupportedPythonTypes]) -> StrAnyDict:
        """Map tuple row to dict using statement schema."""
        if not self._statement.schema:
            raise InterfaceError("Cannot map row to dict without schema")
        return dict(zip([col.name for col in self._statement.schema.columns], row))