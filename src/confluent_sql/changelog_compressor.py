"""
Changelog compressor module for streaming non-append-only Flink statement results.

This module provides compressors that accumulate and apply changelog operations
to maintain a logical result set that changes over time based on changelog events.
"""

from __future__ import annotations

import abc
import copy
import logging
from typing import TYPE_CHECKING, Generic, TypeVar, cast

from .changelog import ChangeloggedRow, ResultTupleOrDict, StatementResultTuple
from .exceptions import InterfaceError
from .statement import Op, Schema, Statement
from .types import StrAnyDict

if TYPE_CHECKING:
    from .cursor import Cursor

logger = logging.getLogger(__name__)

# Type variable for the output type (either tuple or dict)
T = TypeVar("T", bound=ResultTupleOrDict)


def create_changelog_compressor(cursor: Cursor, statement: Statement) -> ChangelogCompressor:
    """Factory function to create the appropriate changelog compressor.

    This function determines which concrete compressor class to instantiate based on:
    - Whether the statement has upsert columns
    - Whether the cursor is configured for dict or tuple results

    Args:
        cursor: The cursor to fetch changelog data from.
        statement: The statement associated with the cursor.

    Returns:
        An appropriate ChangelogCompressor instance.

    Raises:
        InterfaceError: If the cursor is not configured for changelog results.
    """
    if not cursor.returns_changelog:
        raise InterfaceError(
            "Changelog compressor can only be created for streaming non-append-only queries. "
            "This query does not return changelog results."
        )

    # Determine if we have upsert columns
    has_upsert_columns = bool(statement.traits and statement.traits.upsert_columns)

    # Select the appropriate concrete compressor class
    if has_upsert_columns:
        if cursor.as_dict:
            return UpsertColumnsDictCompressor(cursor, statement)
        else:
            return UpsertColumnsTupleCompressor(cursor, statement)
    elif cursor.as_dict:
        return NoUpsertColumnsDictCompressor(cursor, statement)
    else:
        return NoUpsertColumnsTupleCompressor(cursor, statement)


class ChangelogCompressor(abc.ABC, Generic[T]):
    """Abstract base class for changelog compressors.

    Compressors accumulate changelog operations and maintain a logical result set
    that changes over time based on INSERT, UPDATE_BEFORE/AFTER, and DELETE operations.
    """

    _cursor: Cursor
    """The cursor to fetch changelog data from."""

    _statement: Statement
    """The statement associated with the cursor."""

    _as_dict: bool
    """Whether the cursor returns results as dicts (True) or tuples (False)."""

    _upsert_columns: list[int] | None
    """Zero-based indices of upsert columns from the statement traits, if any."""

    _schema: Schema
    """The schema of the result set."""

    def __init__(self, cursor: Cursor, statement: Statement):
        """Initialize the compressor with a cursor and statement.

        Args:
            cursor: The cursor to fetch changelog data from.
            statement: The statement associated with the cursor.

        Raises:
            InterfaceError: If the cursor does not return changelog results or
                           if the statement does not have a schema.
        """
        self._cursor = cursor
        self._statement = statement

        # Validate this is a changelog query
        if not cursor.returns_changelog:
            raise InterfaceError(
                "ChangelogCompressor can only be created for streaming non-append-only queries"
            )

        # Validate statement has a schema
        if not statement.schema:
            raise InterfaceError(
                "ChangelogCompressor requires a statement with a schema"
            )

        # Get statement info we need
        self._as_dict = cursor.as_dict
        self._upsert_columns = statement.traits.upsert_columns if statement.traits else None
        self._schema = statement.schema

    @abc.abstractmethod
    def get_snapshot(self, fetch_batchsize: int | None = None) -> list[T]:
        """Fetch and accumulate changelog operations, returning the current logical result set.

        Args:
            fetch_batchsize: The batch size to use for fetching, or None to use cursor.arraysize.

        Returns:
            A deep copy of the accumulated logical result set.
        """
        ...

    def close(self) -> None:
        """Close the compressor and release resources.

        This method closes the underlying cursor and clears any internal state.
        After calling close(), the compressor should not be used anymore.
        """
        self._cursor.close()


class UpsertColumnsCompressor(ChangelogCompressor[T], abc.ABC):
    """Abstract base class for compressors handling statements with upsert columns.

    Uses dict-based storage for fast O(1) key-based lookups.
    """

    _upsert_column_indices: list[int]
    """Zero-based indices of columns that form the upsert key."""

    _rows: dict[tuple, T]
    """Dictionary mapping key tuples to row data. Dict maintains insertion order in Python 3.7+."""

    _pending_update_positions: dict[tuple, tuple]
    """Mapping of keys to keys, tracking UPDATE_BEFORE operations awaiting UPDATE_AFTER."""

    def __init__(self, cursor: Cursor, statement: Statement):
        """Initialize the compressor with upsert column indices.

        Args:
            cursor: The cursor to fetch changelog data from.
            statement: The statement associated with the cursor.

        Raises:
            InterfaceError: If the statement does not have upsert columns.
        """
        super().__init__(cursor, statement)

        if not statement.traits or not statement.traits.upsert_columns:
            raise InterfaceError(
                "UpsertColumnsCompressor requires a statement with upsert columns"
            )

        self._upsert_column_indices = statement.traits.upsert_columns
        self._rows = {}
        self._pending_update_positions = {}

    def _extract_key(self, row: T) -> tuple:
        """Extract the key tuple from a row based on upsert columns.

        Args:
            row: The row data (tuple or dict).

        Returns:
            A tuple of the key values.
        """
        if isinstance(row, dict):
            # For dicts, we need to convert to list first to use indices
            col_names = [col.name for col in self._schema.columns]
            row_list = [row.get(col_names[i]) for i in range(len(col_names))]
            return tuple(row_list[i] for i in self._upsert_column_indices)
        else:
            # For tuples, direct index access
            return tuple(row[i] for i in self._upsert_column_indices)

    def _apply_operation(self, op: Op, row: T) -> None:
        """Apply a changelog operation to the internal state.

        Args:
            op: The changelog operation.
            row: The row data.
        """
        key = self._extract_key(row)

        if op == Op.INSERT:
            self._rows[key] = row

        elif op == Op.UPDATE_BEFORE:
            # Mark the position for UPDATE_AFTER
            if key in self._rows:
                self._pending_update_positions[key] = key

        elif op == Op.UPDATE_AFTER:
            # Use marked position if available
            old_key = self._pending_update_positions.get(key)
            if old_key:
                # Remove old key if different
                if old_key != key and old_key in self._rows:
                    del self._rows[old_key]
                # Don't clear pending position - keep it for potential future updates

            # Add/update with new key
            self._rows[key] = row

        elif op == Op.DELETE:
            if key in self._rows:
                del self._rows[key]

    def get_snapshot(self, fetch_batchsize: int | None = None) -> list[T]:
        """Fetch and accumulate changelog operations, returning the current logical result set.

        Args:
            fetch_batchsize: The batch size to use for fetching, or None to use cursor.arraysize.

        Returns:
            A deep copy of the accumulated logical result set in insertion order.
        """
        batchsize = fetch_batchsize or self._cursor.arraysize

        # Keep fetching until no more results
        while True:
            batch = self._cursor.fetchmany(batchsize)
            if not batch:
                break

            for changelogged_row in batch:
                if isinstance(changelogged_row, ChangeloggedRow):
                    op, row = changelogged_row
                    self._apply_operation(op, cast(T, row))

        # Return deep copy in insertion order (dict maintains order)
        return [copy.deepcopy(row) for row in self._rows.values()]

    def close(self) -> None:
        """Close the compressor and release resources.

        Clears internal row storage and pending update tracking before
        delegating to parent class to close the cursor.
        """
        self._rows.clear()
        self._pending_update_positions.clear()
        super().close()


class NoUpsertColumnsCompressor(ChangelogCompressor[T], abc.ABC):
    """Abstract base class for compressors handling statements without upsert columns.

    Uses list-based storage with linear scan for row matching.
    """

    _rows: list[T]
    """List of rows maintaining insertion order. Scanned linearly for matching."""

    _pending_update_positions: dict[int, T]
    """Mapping of list positions to rows, tracking UPDATE_BEFORE operations awaiting
    UPDATE_AFTER."""

    def __init__(self, cursor: Cursor, statement: Statement):
        """Initialize the compressor.

        Args:
            cursor: The cursor to fetch changelog data from.
            statement: The statement associated with the cursor.
        """
        super().__init__(cursor, statement)
        self._rows = []
        self._pending_update_positions = {}

    def _find_row_position(self, row: T) -> int | None:
        """Find the position of a matching row by scanning backwards.

        Args:
            row: The row to find.

        Returns:
            The position index if found, None otherwise.
        """
        # Scan backwards to find most recent matching row
        for i in range(len(self._rows) - 1, -1, -1):
            if self._rows[i] == row:
                return i
        return None

    def _apply_operation(self, op: Op, row: T) -> None:
        """Apply a changelog operation to the internal state.

        Args:
            op: The changelog operation.
            row: The row data.
        """
        if op == Op.INSERT:
            self._rows.append(row)

        elif op == Op.UPDATE_BEFORE:
            # Find and mark position
            pos = self._find_row_position(row)
            if pos is not None:
                # Store position mapped to row for UPDATE_AFTER
                self._pending_update_positions[pos] = row

        elif op == Op.UPDATE_AFTER:
            # Look for a pending update position
            update_pos = None
            for pos, _ in self._pending_update_positions.items():
                # Check if this UPDATE_AFTER matches a pending UPDATE_BEFORE
                # Since we don't have keys, we rely on the position
                if pos < len(self._rows):
                    update_pos = pos
                    # Keep the pending position for potential future updates
                    break

            if update_pos is not None and update_pos < len(self._rows):
                self._rows[update_pos] = row
            else:
                # No matching UPDATE_BEFORE, treat as INSERT
                self._rows.append(row)

        elif op == Op.DELETE:
            pos = self._find_row_position(row)
            if pos is not None:
                del self._rows[pos]
                # Adjust pending positions after this deletion
                new_pending = {}
                for p, r in self._pending_update_positions.items():
                    if p > pos:
                        new_pending[p - 1] = r
                    elif p < pos:
                        new_pending[p] = r
                    # Skip position that was deleted
                self._pending_update_positions = new_pending

    def get_snapshot(self, fetch_batchsize: int | None = None) -> list[T]:
        """Fetch and accumulate changelog operations, returning the current logical result set.

        Args:
            fetch_batchsize: The batch size to use for fetching, or None to use cursor.arraysize.

        Returns:
            A deep copy of the accumulated logical result set.
        """
        batchsize = fetch_batchsize or self._cursor.arraysize

        # Keep fetching until no more results
        while True:
            batch = self._cursor.fetchmany(batchsize)
            if not batch:
                break

            for changelogged_row in batch:
                if isinstance(changelogged_row, ChangeloggedRow):
                    op, row = changelogged_row
                    self._apply_operation(op, cast(T, row))

        # Return deep copy
        return copy.deepcopy(self._rows)

    def close(self) -> None:
        """Close the compressor and release resources.

        Clears internal row storage and pending update tracking before
        delegating to parent class to close the cursor.
        """
        self._rows.clear()
        self._pending_update_positions.clear()
        super().close()


class UpsertColumnsTupleCompressor(UpsertColumnsCompressor[StatementResultTuple]):
    """Concrete compressor for statements with upsert columns returning tuples."""
    pass


class UpsertColumnsDictCompressor(UpsertColumnsCompressor[StrAnyDict]):
    """Concrete compressor for statements with upsert columns returning dicts."""
    pass


class NoUpsertColumnsTupleCompressor(NoUpsertColumnsCompressor[StatementResultTuple]):
    """Concrete compressor for statements without upsert columns returning tuples."""
    pass


class NoUpsertColumnsDictCompressor(NoUpsertColumnsCompressor[StrAnyDict]):
    """Concrete compressor for statements without upsert columns returning dicts."""
    pass