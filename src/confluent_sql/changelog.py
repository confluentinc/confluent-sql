"""
Changelog processor module for Confluent SQL DB-API driver.

This module provides implementations of changelog processing for streaming
queries in Confluent SQL.
"""

from __future__ import annotations

import abc
import logging
from typing import Generic, TypeAlias, TypeVar

from .exceptions import InterfaceError

from .connection import Connection
from .statement import ChangelogRow, Op
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

    def __init__(self, connection: Connection, as_dict: bool = False):
        self._connection = connection
        self._as_dict = as_dict
        self._rows = []

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