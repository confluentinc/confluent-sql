"""
Changelog state management and compression for streaming non-append-only queries.

This module provides the RECOMMENDED high-level interface for client code working with
streaming non-append-only Flink statements (e.g., GROUP BY, JOIN). Instead of manually
processing raw changelog events (INSERT, UPDATE_BEFORE/AFTER, DELETE), clients should use
a ChangelogCompressor to automatically maintain a logical result set that reflects the
current state over time.

Usage:
    cursor = conn.streaming_cursor()
    cursor.execute("SELECT first_letter, COUNT(*) FROM users GROUP BY first_letter")
    compressor = cursor.changelog_compressor()

    # Get current snapshot of accumulated results
    snapshot = compressor.get_snapshot()

Compressors consume raw changelog events from RawChangelogProcessor (via the cursor)
and apply operations to maintain the compressed result set. Storage strategies are
automatically selected based on whether the statement has upsert columns (dict-based
keyed lookup vs list-based scanning) and whether results are tuples or dicts.

For low-level changelog fetching without state management, see the `changelog` module.
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
            raise InterfaceError("ChangelogCompressor requires a statement with a schema")

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

    Expects UPDATE_BEFORE to be immediately followed by its matching UPDATE_AFTER event.
    Only one pending update is tracked at a time.
    """

    _upsert_column_indices: list[int]
    """Zero-based indices of columns that form the upsert key."""

    _rows_by_key: dict[tuple, T]
    """Dictionary mapping key tuples to row data. Dict maintains insertion order in Python 3.7+."""

    _expecting_update_after: bool
    """True when UPDATE_BEFORE has been received and UPDATE_AFTER is expected next."""

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
            raise InterfaceError("UpsertColumnsCompressor requires a statement with upsert columns")

        self._upsert_column_indices = statement.traits.upsert_columns
        self._rows_by_key = {}
        self._expecting_update_after = False

    @abc.abstractmethod
    def _extract_key(self, row: T) -> tuple:
        """Extract the key tuple from a row based on upsert columns.

        Args:
            row: The row data (tuple or dict).

        Returns:
            A tuple of the key values.
        """
        ...

    def _apply_operation(self, op: Op, row: T) -> None:
        """Apply a changelog operation to the internal state.

        Args:
            op: The changelog operation.
            row: The row data.
        """
        # Check for unexpected pending update (except when processing UPDATE_AFTER).
        # (Basically, if we had gotten an UPDATE_BEFORE, we highly expect the next
        #  event to be its matching UPDATE_AFTER.)
        if op != Op.UPDATE_AFTER and self._expecting_update_after:
            raise InterfaceError(f"Received {op.name} while an UPDATE_BEFORE is pending: {row}")

        key = self._extract_key(row)

        if op == Op.INSERT:
            # When iterating _rows_by_keys in get_snapshot(), this newly inserted key will be
            # at the end of the dict, so insertion order will be maintained.
            self._rows_by_key[key] = row

        elif op == Op.UPDATE_BEFORE:
            # Raise if we receive an UPDATE_BEFORE for a key that doesn't exist in current state
            if key not in self._rows_by_key:
                raise InterfaceError(
                    f"Received UPDATE_BEFORE for a key that does not exist in current state: {key}"
                )
            # Mark that we're expecting UPDATE_AFTER next
            self._expecting_update_after = True

        elif op == Op.UPDATE_AFTER:
            # May or may not have gotten a preceding UPDATE_BEFORE.
            # In either case, verify the key exists in current state
            if key not in self._rows_by_key:
                raise InterfaceError(
                    f"Received UPDATE_AFTER for a key that does not exist in current state: {key}"
                )

            # Update the row
            self._rows_by_key[key] = row
            self._expecting_update_after = False

        elif op == Op.DELETE:
            if key not in self._rows_by_key:
                raise InterfaceError(
                    f"Received DELETE for a key that does not exist in current state: {key}"
                )
            del self._rows_by_key[key]

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
        return [copy.deepcopy(row) for row in self._rows_by_key.values()]

    def close(self) -> None:
        """Close the compressor and release resources.

        Clears internal row storage and pending update tracking before
        delegating to parent class to close the cursor.
        """
        self._rows_by_key.clear()
        self._expecting_update_after = False
        super().close()


class NoUpsertColumnsCompressor(ChangelogCompressor[T], abc.ABC):
    """Abstract base class for compressors handling statements without upsert columns.

    Uses list-based storage with linear scan for row matching.

    Expects UPDATE_BEFORE to be immediately followed by its matching UPDATE_AFTER event.
    Only one pending update is tracked at a time.
    """

    _rows: list[T]
    """List of rows maintaining insertion order. Scanned linearly for matching."""

    _pending_update_position: int | None
    """Position of the row marked by the most recent UPDATE_BEFORE, awaiting UPDATE_AFTER.
    None when no UPDATE_BEFORE is pending."""

    def __init__(self, cursor: Cursor, statement: Statement):
        """Initialize the compressor.

        Args:
            cursor: The cursor to fetch changelog data from.
            statement: The statement associated with the cursor.
        """
        super().__init__(cursor, statement)
        self._rows = []
        self._pending_update_position = None

    def _find_row_position(self, row: T, operation: Op) -> int:
        """Find the position of a matching row by scanning backwards.

        Args:
            row: The row to find.
            operation: The operation being performed (for error messaging).

        Returns:
            The position index of the row.

        Raises:
            InterfaceError: If the row is not found in current state.
        """
        # Scan backwards to find most recent matching row
        for i in range(len(self._rows) - 1, -1, -1):
            if self._rows[i] == row:
                return i

        # Row not found - raise error with operation-specific message
        raise InterfaceError(
            f"Received {operation.name} for a row that does not exist in current state: {row}"
        )

    def _apply_operation(self, op: Op, row: T) -> None:
        """Apply a changelog operation to the internal state.

        Args:
            op: The changelog operation.
            row: The row data.
        """
        # Check for unexpected pending update (except when processing UPDATE_AFTER).
        # (Basically, if we had gotten an UPDATE_BEFORE, we highly expect the next
        #  event to be its matching UPDATE_AFTER.)
        if op != Op.UPDATE_AFTER and self._pending_update_position is not None:
            raise InterfaceError(f"Received {op.name} while an UPDATE_BEFORE is pending: {row}")

        if op == Op.INSERT:
            self._rows.append(row)

        elif op == Op.UPDATE_BEFORE:
            # Find row position (raises InterfaceError if not found)
            pos = self._find_row_position(row, Op.UPDATE_BEFORE)
            # Record position for pending update (expecting matching UPDATE_AFTER next)
            self._pending_update_position = pos

        elif op == Op.UPDATE_AFTER:
            # MUST have gotten a preceding UPDATE_BEFORE.
            # (Without upsert columns, we can't identify the row to update from the
            #  UPDATE_AFTER row alone, since the row content has changed.)
            if self._pending_update_position is None:
                raise InterfaceError(
                    f"Received UPDATE_AFTER without a preceding UPDATE_BEFORE: {row}"
                )

            self._rows[self._pending_update_position] = row
            self._pending_update_position = None

        elif op == Op.DELETE:
            # Find row position (raises InterfaceError if not found)
            pos = self._find_row_position(row, Op.DELETE)
            del self._rows[pos]

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
        self._pending_update_position = None
        super().close()


class UpsertColumnsTupleCompressor(UpsertColumnsCompressor[StatementResultTuple]):
    """Concrete compressor for statements with upsert columns returning tuples.

    Optimized for tuple row access with direct indexing.
    """

    def _extract_key(self, row: StatementResultTuple) -> tuple:
        """Extract the key tuple from a tuple row using direct index access.

        Args:
            row: The row data as a tuple.

        Returns:
            A tuple of the key values.
        """
        return tuple(row[i] for i in self._upsert_column_indices)


class UpsertColumnsDictCompressor(UpsertColumnsCompressor[StrAnyDict]):
    """Concrete compressor for statements with upsert columns returning dicts.

    Optimized for dict row access with precomputed column names for upsert key fields.
    """

    _upsert_key_column_names: list[str]
    """Precomputed list of column names corresponding to upsert column indices."""

    def __init__(self, cursor: Cursor, statement: Statement):
        """Initialize the compressor with precomputed column names for upsert keys.

        Args:
            cursor: The cursor to fetch changelog data from.
            statement: The statement associated with the cursor.
        """
        super().__init__(cursor, statement)

        # Precompute the column names for upsert key fields
        self._upsert_key_column_names = [
            self._schema.columns[i].name for i in self._upsert_column_indices
        ]

    def _extract_key(self, row: StrAnyDict) -> tuple:
        """Extract the key tuple from a dict row using precomputed column names.

        Args:
            row: The row data as a dict.

        Returns:
            A tuple of the key values.
        """
        return tuple(row[col_name] for col_name in self._upsert_key_column_names)


class NoUpsertColumnsTupleCompressor(NoUpsertColumnsCompressor[StatementResultTuple]):
    """Concrete compressor for statements without upsert columns returning tuples."""

    pass


class NoUpsertColumnsDictCompressor(NoUpsertColumnsCompressor[StrAnyDict]):
    """Concrete compressor for statements without upsert columns returning dicts."""

    pass
