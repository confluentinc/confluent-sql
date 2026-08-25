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
    def _clear_storage(self) -> None:
        """Clear internal row storage."""
        ...

    def _resolve_batchsize(self, fetch_batchsize: int | None) -> int:
        """Resolve and validate the batch size to use for fetching.

        Args:
            fetch_batchsize: Explicit batch size, or None to use cursor.arraysize.

        Returns:
            The resolved batch size as a positive integer.

        Raises:
            InterfaceError: If fetch_batchsize is not a positive int.
        """
        # Validate explicit batch size parameter if provided
        if fetch_batchsize is not None:
            # Reject non-int values (including bool) even if they happen to compare or cast
            if isinstance(fetch_batchsize, bool) or not isinstance(fetch_batchsize, int):
                raise InterfaceError(
                    f"fetch_batchsize must be an int, got {type(fetch_batchsize).__name__}"
                )
            if fetch_batchsize <= 0:
                raise InterfaceError(f"fetch_batchsize must be positive, got {fetch_batchsize}")
            return fetch_batchsize

        # Fall back to cursor.arraysize (which is guaranteed valid by its property setter)
        return self._cursor.arraysize

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
        reflects every event fetched so far; nothing is retained awaiting a match.

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
        # Resolve batch size once to ensure consistent behavior across yields
        batchsize = self._resolve_batchsize(fetch_batchsize)

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
            # Pass resolved batchsize (int) so get_current_snapshot() won't re-read cursor.arraysize
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
        available changelog events have been consumed and applied. It reflects every event
        fetched so far; nothing is retained awaiting a match.

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
        # Resolve batch size
        batchsize = self._resolve_batchsize(fetch_batchsize)

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
        self._cursor.close()


class UpsertColumnsCompressor(ChangelogCompressor):
    """Compressor for statements with upsert columns, handling both tuple and dict rows.

    Uses dict-based storage for fast O(1) key-based lookups.

    The Confluent-cloud-side Kafka consumer reading a keyed upsert topic may be draining multiple
    partitions per poll. Same-key events stay ordered (a key always hashes to the same partition),
    but a single fetchmany() batch can still interleave *different* keys' events, so a key's
    UPDATE_BEFORE need not be immediately followed by that same key's UPDATE_AFTER -- an unrelated
    key's event can land in between. See issue #185.

    To stay correct under that interleaving, this compressor tracks no pending-update state:
    UPDATE_BEFORE (-U) is treated as a pure no-op, since under key-based upsert semantics it
    carries no information the matching INSERT/UPDATE_AFTER doesn't already supply. UPDATE_AFTER
    (+U) is handled exactly like INSERT -- both simply upsert the row for its key, last write
    wins. DELETE is unaffected by this and still validates that the key exists: same-key ops stay
    ordered relative to each other, so a DELETE for an untracked key remains a genuine protocol
    violation worth surfacing.

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

    def _clear_storage(self) -> None:
        """Clear internal row storage."""
        self._rows_by_key.clear()

    def _apply_operation(self, op: Op, row: ResultTupleOrDict) -> None:
        """Apply a changelog operation to the internal state.

        Args:
            op: The changelog operation.
            row: The row data.
        """
        if op == Op.UPDATE_BEFORE:
            # No-op: under key-based upsert semantics a retraction carries no information the
            # matching INSERT/UPDATE_AFTER doesn't already supply. See issue #185.
            return

        key = self._extract_key(row)

        if op.treat_as_insert:  # INSERT or UPDATE_AFTER: last write for this key wins
            # When iterating _rows_by_key in get_snapshot(), an upsert for a brand-new key will
            # be at the end of the dict, so insertion order is maintained.
            self._rows_by_key[key] = row

        elif self._rows_by_key.pop(key, None) is None:  # DELETE
            # Wacky, the delete is for a key that doesn't exist in current state!
            raise InterfaceError(
                f"Received DELETE for a key that does not exist in current state: {key}"
            )

    def _copy_accumulated_rows(self) -> list[ResultTupleOrDict]:
        """Return deep copy of rows from dict storage in insertion order.

        Returns:
            A deep copy list of rows from the dict storage.
        """
        return [copy.deepcopy(row) for row in self._rows_by_key.values()]


class NoUpsertColumnsCompressor(ChangelogCompressor):
    """Compressor for statements without upsert columns, handling both tuple and dict rows.

    Uses list-based storage with linear scan for row matching.

    Without an upsert key, a row can only be identified by its full-row spelling, and the
    changelog reaches this client via a multi-partition keyless sink that partitions by
    whole-row hash. An updated row's +U/-D spelling therefore hashes to a potentially
    different partition than its original +I/-U spelling, and since Kafka only guarantees
    order within a partition, events can be observed in a surprising order *across* spellings
    (e.g. a +U before its logical -U, or a -D before a later +I). See issue #184.

    To stay correct under that skew this compressor makes no ordering assumptions *across
    different spellings* and holds no pending-update state (see below for the one ordering
    guarantee it does rely on). It collapses the four ops to two: the additive ops (+I, +U;
    Op.treat_as_insert) append a row spelling, and the retracting ops (-U, -D;
    Op.treat_as_delete) remove one occurrence of a row spelling. The result set is eventually
    consistent -- intermediate points in time may transiently show extra rows, but once every
    event has arrived the set converges to the correct contents.

    A retraction for a spelling never arrives before the matching insert for that same
    spelling: identical whole-row values always hash to the same partition, which preserves
    their relative order. `_find_row_position` therefore always finds its match on a
    well-formed stream; its raise is retained only to surface a genuine protocol violation.

    Rows can be either tuples or dicts (as determined by cursor.as_dict). Row matching
    is performed by equality comparison, which works identically for both tuple and dict.
    """

    _rows: list[ResultTupleOrDict]
    """List of row spellings, ordered by when each op was applied. Scanned linearly for
    matching. Not a stable per-row order: an update's new spelling is appended anew, not
    repositioned at its old spelling's spot."""

    def __init__(self, cursor: Cursor, statement: Statement):
        """Initialize the compressor.

        Args:
            cursor: The cursor to fetch changelog data from.
            statement: The statement associated with the cursor.
        """
        super().__init__(cursor, statement)
        self._rows = []

    def _clear_storage(self) -> None:
        """Clear internal row storage."""
        self._rows.clear()

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

        Additive ops (+I, +U) append the row; retracting ops (-U, -D) remove the most recent
        occurrence of a matching row. No ordering is assumed across *different* rows' ops --
        but a row's own retracting op is assumed to arrive after its own matching additive op
        (see class docstring); `_find_row_position` raises otherwise.

        Args:
            op: The changelog operation.
            row: The row data.
        """
        if op.treat_as_insert:
            self._rows.append(row)
        else:  # op.treat_as_delete
            del self._rows[self._find_row_position(row, op)]

    def _copy_accumulated_rows(self) -> list[ResultTupleOrDict]:
        """Return deep copy of rows from list storage.

        Returns:
            A deep copy of the list of rows.
        """
        return copy.deepcopy(self._rows)
