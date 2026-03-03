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

    # Iterate over snapshots until the query is stopped
    for snapshot in compressor.snapshots():
        process(snapshot)
        time.sleep(5)  # Optional: wait between polls

    # Generator exits when query is externally stopped/deleted or fails

Compressors consume raw changelog events from ChangelogEventReader (via the cursor)
and apply operations to maintain the compressed result set. Storage strategies are
automatically selected based on whether the statement has upsert columns (dict-based
keyed lookup vs list-based scanning).

For low-level changelog fetching without state management, see the `result_readers` module.
"""

from __future__ import annotations

import abc
import copy
import logging
from collections.abc import Generator
from typing import TYPE_CHECKING, cast

from .exceptions import InterfaceError, StatementStoppedError
from .result_readers import ChangeloggedRow, ResultTupleOrDict
from .statement import Op, Schema, Statement

if TYPE_CHECKING:
    from .cursor import Cursor

logger = logging.getLogger(__name__)


def create_changelog_compressor(cursor: Cursor, statement: Statement) -> ChangelogCompressor:
    """Factory function to create the appropriate changelog compressor.

    This function determines which concrete compressor class to instantiate based on
    whether the statement has upsert columns. The decision of whether to return tuple or
    dict rows is made by the result reader layer and is transparent to the compressor.

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

    # Select the appropriate concrete compressor class based on upsert columns only
    if has_upsert_columns:
        return UpsertColumnsCompressor(cursor, statement)
    else:
        return NoUpsertColumnsCompressor(cursor, statement)


class ChangelogCompressor(abc.ABC):
    """Abstract base class for changelog compressors.

    Compressors accumulate changelog operations and maintain a logical result set
    that changes over time based on INSERT, UPDATE_BEFORE/AFTER, and DELETE operations.
    """

    _cursor: Cursor
    """The cursor to fetch changelog data from."""

    _statement: Statement
    """The statement associated with the cursor."""

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
        self._upsert_columns = statement.traits.upsert_columns if statement.traits else None
        self._schema = statement.schema

    @abc.abstractmethod
    def _apply_operation(self, op: Op, row: ResultTupleOrDict) -> None:
        """Apply a changelog operation to the internal state.

        Args:
            op: The changelog operation.
            row: The row data.
        """
        ...

    @abc.abstractmethod
    def _copy_accumulated_rows(self) -> list[ResultTupleOrDict]:
        """Return a deep copy of the accumulated rows from internal storage.

        Returns:
            A deep copy list of the current logical result set.
        """
        ...

    @abc.abstractmethod
    def _has_pending_update(self) -> bool:
        """Check if there's a pending UPDATE_BEFORE awaiting UPDATE_AFTER.

        Returns:
            True if a pending update is in progress, False otherwise.
        """
        ...

    @abc.abstractmethod
    def _clear_storage(self) -> None:
        """Clear internal row storage."""
        ...

    @abc.abstractmethod
    def _clear_pending_update(self) -> None:
        """Clear pending update tracking state."""
        ...

    def _validate_no_pending_update(self, op: Op, row: ResultTupleOrDict) -> None:
        """Raise if we have a pending update and aren't processing UPDATE_AFTER.

        Args:
            op: The changelog operation being processed.
            row: The row data.

        Raises:
            InterfaceError: If a pending UPDATE_BEFORE exists while processing a
                           non-UPDATE_AFTER operation.
        """
        if op != Op.UPDATE_AFTER and self._has_pending_update():
            raise InterfaceError(f"Received {op.name} while an UPDATE_BEFORE is pending: {row}")

    def snapshots(
        self, fetch_batchsize: int | None = None
    ) -> Generator[list[ResultTupleOrDict], None, None]:
        """Generator that yields snapshots of the accumulated result set until the query stops.

        This generator continuously polls for new changelog events, applies them to the internal
        state, and yields self-consistent snapshots of the accumulated result set. It automatically
        terminates when the streaming query enters a terminal state and all results have been
        consumed.

        Each iteration fetches ALL currently available changelog events from the cursor (until
        fetchmany returns an empty list), applies them to the internal state, and yields a
        self-consistent snapshot.

        **Self-Consistency**: A snapshot is considered self-consistent when all currently
        available changelog events have been consumed and applied. This means the snapshot
        reflects a coherent state with no pending UPDATE_BEFORE operations awaiting their
        matching UPDATE_AFTER.

        **No Guarantee of Logical Changes**: There is NO guarantee that consecutive snapshots
        will differ. If no new changelog events arrived since the prior yield, the snapshot
        will be logically identical to the previous one. Additionally, even if events were
        processed, the logical result set may remain unchanged (e.g., an INSERT followed
        immediately by a DELETE of the same row).

        **Return Value**: Each yielded snapshot is a deep copy of the accumulated rows. This
        ensures that modifications to the snapshot will not affect the compressor's internal
        state. The caller is free to mutate the yielded snapshots.

        **Termination**: The generator raises exceptions when the statement stops:
        - StatementStoppedError: Raised when cursor.may_have_results becomes False,
          indicating the statement entered a terminal phase (STOPPED, FAILED, COMPLETED).
          The exception includes the Statement object for inspection of why it stopped.
        - StatementDeletedError: A subclass of StatementStoppedError raised specifically
          when the statement is deleted (404 response). This is a distinct error case
          from normal stopping.

        Since streaming queries run indefinitely, any termination is exceptional and
        warrants an exception rather than silent StopIteration.

        Args:
            fetch_batchsize: The batch size to use for fetching, or None to use cursor.arraysize.

        Yields:
            Deep copies of the accumulated logical result set after consuming all currently
            available changelog events.

        Example:
            >>> compressor = cursor.changelog_compressor()
            >>> for snapshot in compressor.snapshots():
            ...     process(snapshot)
            ...     time.sleep(5)  # Optional: wait between polls
            >>> # Generator exits when query is stopped/deleted or fails
            >>> print("Streaming query stopped")
        """
        # Validate explicit batch size parameter (don't validate arraysize - trust the driver)
        if fetch_batchsize is not None and fetch_batchsize <= 0:
            raise ValueError(f"fetch_batchsize must be positive, got {fetch_batchsize}")

        # Resolve batch size once to ensure consistent behavior across yields
        batchsize = self._cursor.arraysize if fetch_batchsize is None else fetch_batchsize

        while True:
            if not self._cursor.may_have_results:
                # Statement stopped unexpectedly - raise exception with context
                statement = self._cursor.statement
                statement_name = statement.name if statement else "unknown"
                phase_info = statement.phase if statement else None
                phase_suffix = (
                    f" (phase: {statement.phase})" if statement and statement.phase else ""
                )
                message = (
                    f"Streaming statement '{statement_name}' stopped unexpectedly{phase_suffix}"
                )
                raise StatementStoppedError(
                    message,
                    statement_name=statement_name,
                    statement=statement,
                    phase=phase_info,
                )

            # Fetch and apply all available events, then yield snapshot
            yield self.get_current_snapshot(batchsize)

    def get_current_snapshot(self, fetch_batchsize: int | None = None) -> list[ResultTupleOrDict]:
        """Fetch all currently available changelog events and return current snapshot.

        This method fetches ALL currently available changelog events from the cursor (until
        fetchmany returns an empty list), applies them to the internal state via
        _apply_operation(), and returns a deep copy of the accumulated result set.

        Unlike snapshots(), this method:
        - Does NOT check cursor.may_have_results (caller's responsibility)
        - Does NOT raise StatementStoppedError
        - Returns a single snapshot rather than yielding indefinitely
        - Is non-blocking - returns immediately after consuming available events

        **Self-Consistency**: The returned snapshot is self-consistent, meaning all currently
        available changelog events have been consumed and applied. No pending UPDATE_BEFORE
        operations remain without their matching UPDATE_AFTER.

        **Deep Copy**: The returned snapshot is a deep copy. Mutations will not affect the
        compressor's internal state.

        **Idempotency**: If called when no new events are available, returns the current
        state unchanged. Multiple consecutive calls with no new events will return
        logically identical snapshots.

        Args:
            fetch_batchsize: The batch size for fetching, or None to use cursor.arraysize.

        Returns:
            A deep copy of the accumulated result set after consuming all currently
            available changelog events.

        Example:
            >>> compressor = cursor.changelog_compressor()
            >>> while cursor.may_have_results:
            ...     snapshot = compressor.get_current_snapshot()
            ...     process(snapshot)
            ...     time.sleep(5)
        """
        # Validate explicit batch size parameter (don't validate arraysize - trust the driver)
        if fetch_batchsize is not None and fetch_batchsize <= 0:
            raise ValueError(f"fetch_batchsize must be positive, got {fetch_batchsize}")

        # Resolve batch size once using explicit None check
        batchsize = self._cursor.arraysize if fetch_batchsize is None else fetch_batchsize

        # Fetch all currently available events
        while True:
            batch = self._cursor.fetchmany(batchsize)
            if not batch:
                break

            for changelogged_row in batch:
                # Must cast because cursor.fetchmany() returns list[ResultRow],
                # but if using a ChangelogCompressor, we know the rows are actually
                # ChangeloggedRow consisting of (Op, ResultTupleOrDict).
                op, row = cast(ChangeloggedRow, changelogged_row)
                self._apply_operation(op, cast(ResultTupleOrDict, row))

        # Return current snapshot
        return self._copy_accumulated_rows()

    def close(self) -> None:
        """Close the compressor and release resources.

        This method closes the underlying cursor and clears any internal state.
        After calling close(), the compressor should not be used anymore.
        """
        self._clear_storage()
        self._clear_pending_update()
        self._cursor.close()


class UpsertColumnsCompressor(ChangelogCompressor):
    """Compressor for statements with upsert columns, handling both tuple and dict rows.

    Uses dict-based storage for fast O(1) key-based lookups.

    Expects UPDATE_BEFORE to be immediately followed by its matching UPDATE_AFTER event.
    Only one pending update is tracked at a time.

    Rows can be either tuples or dicts (as determined by cursor.as_dict). The row format
    decision is made by the result reader layer, and this compressor works transparently
    with either format.
    """

    _upsert_column_indices: list[int]
    """Zero-based indices of columns that form the upsert key."""

    _upsert_key_column_names: list[str]
    """Column names corresponding to upsert column indices, for dict row access."""

    _rows_by_key: dict[tuple, ResultTupleOrDict]
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

        # Precompute column names for dict access (used if rows are dicts)
        self._upsert_key_column_names = [
            self._schema.columns[i].name for i in self._upsert_column_indices
        ]

        self._rows_by_key = {}
        self._expecting_update_after = False

    def _extract_key(self, row: ResultTupleOrDict) -> tuple:
        """Extract the key tuple from a row based on upsert columns.

        Handles both tuple and dict row formats. The row format (tuple or dict) is determined
        by cursor.as_dict and guaranteed by the result reader layer.

        Args:
            row: The row data, either a tuple (if cursor.as_dict=False) or dict (if as_dict=True).

        Returns:
            A tuple of the key values in column order.
        """
        if isinstance(row, dict):
            # Dict case: use precomputed column names
            return tuple(row[col_name] for col_name in self._upsert_key_column_names)
        else:
            # Tuple case: use direct index access
            return tuple(row[i] for i in self._upsert_column_indices)

    def _has_pending_update(self) -> bool:
        """Check if there's a pending UPDATE_BEFORE awaiting UPDATE_AFTER.

        Returns:
            True if a pending update is in progress, False otherwise.
        """
        return self._expecting_update_after

    def _clear_storage(self) -> None:
        """Clear internal row storage."""
        self._rows_by_key.clear()

    def _clear_pending_update(self) -> None:
        """Clear pending update tracking state."""
        self._expecting_update_after = False

    def _apply_operation(self, op: Op, row: ResultTupleOrDict) -> None:
        """Apply a changelog operation to the internal state.

        Args:
            op: The changelog operation.
            row: The row data.
        """
        self._validate_no_pending_update(op, row)

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

    def _copy_accumulated_rows(self) -> list[ResultTupleOrDict]:
        """Return deep copy of rows from dict storage in insertion order.

        Returns:
            A deep copy list of rows from the dict storage.
        """
        return [copy.deepcopy(row) for row in self._rows_by_key.values()]


class NoUpsertColumnsCompressor(ChangelogCompressor):
    """Compressor for statements without upsert columns, handling both tuple and dict rows.

    Uses list-based storage with linear scan for row matching.

    Expects UPDATE_BEFORE to be immediately followed by its matching UPDATE_AFTER event.
    Only one pending update is tracked at a time.

    Rows can be either tuples or dicts (as determined by cursor.as_dict). Row matching
    is performed by equality comparison, which works identically for both tuple and dict.
    """

    _rows: list[ResultTupleOrDict]
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

    def _has_pending_update(self) -> bool:
        """Check if there's a pending UPDATE_BEFORE awaiting UPDATE_AFTER.

        Returns:
            True if a pending update is in progress, False otherwise.
        """
        return self._pending_update_position is not None

    def _clear_storage(self) -> None:
        """Clear internal row storage."""
        self._rows.clear()

    def _clear_pending_update(self) -> None:
        """Clear pending update tracking state."""
        self._pending_update_position = None

    def _find_row_position(self, row: ResultTupleOrDict, operation: Op) -> int:
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

    def _apply_operation(self, op: Op, row: ResultTupleOrDict) -> None:
        """Apply a changelog operation to the internal state.

        Args:
            op: The changelog operation.
            row: The row data.
        """
        self._validate_no_pending_update(op, row)

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

    def _copy_accumulated_rows(self) -> list[ResultTupleOrDict]:
        """Return deep copy of rows from list storage.

        Returns:
            A deep copy of the list of rows.
        """
        return copy.deepcopy(self._rows)
