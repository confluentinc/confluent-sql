from __future__ import annotations

import logging
from collections.abc import Iterator
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

from .exceptions import InterfaceError, OperationalError
from .execution_mode import ExecutionMode
from .types import ColumnTypeDefinition, FromResponseTypes, StatementTypeConverter, StrAnyDict

if TYPE_CHECKING:
    from .connection import Connection

logger = logging.getLogger(__name__)


LABEL_PREFIX = "user.confluent.io/"
"""Required prefix for all end-user labels in statement metadata. When filtering statements by
label, users can provide just the end-user portion of the label (without the prefix) and this driver
will add the prefix before making API calls."""


class Op(Enum):
    """Row operation types for Flink SQL changelog streams.

    These operation types correspond to Apache Flink's RowKind enum and indicate
    the type of change for each row in a changelog stream. They are used when
    processing non-append-only streaming queries that can produce updates and deletions.

    For more information, see:
    https://nightlies.apache.org/flink/flink-docs-stable/api/java/org/apache/flink/types/RowKind.html
    """

    INSERT = 0
    """Insertion operation.

    Represents a new row being added to the result set.
    String representation: +I
    """

    UPDATE_BEFORE = 1
    """Update operation with the previous content of the updated row.

    This operation SHOULD occur together with UPDATE_AFTER for modelling
    an update that needs to retract the previous row first. Represents
    the "before" state of a row that is being updated.
    String representation: -U
    """

    UPDATE_AFTER = 2
    """Update operation with new content of the updated row.

    This operation CAN occur together with UPDATE_BEFORE for modelling
    an update that needs to retract the previous row first. Represents
    the "after" state of a row that has been updated.
    String representation: +U
    """

    DELETE = 3
    """Deletion operation.

    Represents a row being removed from the result set.
    String representation: -D
    """

    def __str__(self):
        if self is self.INSERT:
            return "+I"
        elif self is self.UPDATE_BEFORE:
            return "-U"
        elif self is self.UPDATE_AFTER:
            return "+U"
        elif self is self.DELETE:
            return "-D"
        else:
            raise ValueError(
                f"Unknown value for Op: '{self.value}'. This is probably a bug"
            )  # pragma: no cover


class ChangelogRow:
    """A single row in a changelog stream, including changelog operation type
    and from-json row data."""

    __slots__ = ("op", "row")

    op: Op
    """The changelog operation type."""
    row: list[FromResponseTypes]
    """The row data as a list of from-response-api-json values."""

    def __init__(self, op: int, row: list[FromResponseTypes]):
        self.op = Op(op)
        self.row = row


class Phase(Enum):
    """Statement execution phases with terminal state detection."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    DELETING = "DELETING"
    FAILED = "FAILED"
    # This is not documented in the rest api docs, but mentioned here:
    # https://docs.confluent.io/cloud/current/flink/concepts/statements.html#flink-sql-statements
    DEGRADED = "DEGRADED"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"

    # This is only used internally,
    # never returned by the api.
    DELETED = "DELETED"

    def __init__(self, value: str) -> None:
        """Initialize Phase enum member."""
        self._value_ = value

    @property
    def is_terminal(self) -> bool:
        """Check if this phase is a terminal state (statement execution has ended).

        Terminal states are those where the statement is no longer executing and will
        not transition to any other state.

        Returns:
            True if the phase is COMPLETED, STOPPED, FAILED, or DELETED. False otherwise.
        """
        # Terminal phase values defined at class level
        return self.value in Phase._TERMINAL_PHASES  # type: ignore[attr-defined]


# Class-level constant defining terminal phases
Phase._TERMINAL_PHASES = frozenset({"COMPLETED", "STOPPED", "FAILED", "DELETED"})  # type: ignore[attr-defined]


@dataclass
class Statement:
    """Represents a Confluent SQL statement, including its metadata, spec, status,
    and parsed traits such as schema, sql kind, etc."""

    # SQL kinds that represent pure DDL statements (create/modify schema objects)
    _PURE_DDL_KINDS = frozenset(
        {"CREATE_TABLE", "DROP_TABLE", "CREATE_VIEW", "DROP_VIEW", "ALTER_TABLE"}
    )

    # From the cursor that created this statement ...
    connection: Connection

    # From the API response fields ...
    statement_id: str
    name: str
    spec: StrAnyDict
    status: StrAnyDict
    metadata: StrAnyDict
    # Parsed fields ...
    traits: Traits | None

    # Internal state
    _phase: Phase
    _deleted: bool = False

    @property
    def is_bounded(self) -> bool | None:
        """A bounded statement has a finite result set. It may either come from a snapshot query
        (those submitted in snapshot execution mode -- all such statements are bounded) or a
        streaming query with a defined end (need to find a good example here, but perhaps
        one selecting from a VALUES clause or whatnot).

        As of Jan 2026, streaming mode CREATE TABLE AS SELECT (CTAS) statements are being
        reported back wrongly as bounded, so this property should be used with caution unless
        considering other factors such as the current phase (such statements should never reach
        a terminal state on their own). This is captured as Jira FSE-1021.
        """
        return self._possible_traits().is_bounded

    def can_fetch_results(self, execution_mode: ExecutionMode) -> bool:
        """Check if results can be fetched from this statement based on execution mode.

        This method encapsulates all the complex readiness logic that depends on both
        the statement's characteristics and the execution mode in which it was submitted.

        Args:
            execution_mode: The execution mode (snapshot or streaming) the statement was
                submitted in.

        Returns:
            True if results can be fetched, False otherwise.
        """
        # Terminal states are always ready (COMPLETED, FAILED, STOPPED, DELETED)
        if self.phase.is_terminal:
            return True

        if execution_mode.is_streaming:
            # In streaming mode, readiness depends on statement type.
            if self.is_pure_ddl:
                # Pure DDL must complete fully before the created/modified objects
                # are ready for use. Since we already checked is_terminal above, return False.
                return False
            elif self.is_bounded and not self.is_append_only:
                # Bounded non-append-only queries (e.g., aggregations without streaming input)
                # must complete fully before results are available for fetching.
                # Since we already checked is_terminal above, return False.
                return False
            else:
                # Unbounded streaming queries and append-only bounded queries are ready
                # when RUNNING (terminal states already handled above)
                return self.phase == Phase.RUNNING
        else:
            # In snapshot mode, statements are only ready for result fetching when they
            # reach a terminal state. Terminal states are checked above, so return False.
            return False

    @property
    def is_failed(self) -> bool:
        """Did the statement fail?"""
        return self.phase == Phase.FAILED

    @property
    def is_running(self) -> bool:
        return self.phase == Phase.RUNNING

    @property
    def is_deletable(self) -> bool:
        """Check if the statement can be deleted safely."""
        return self.phase in {Phase.COMPLETED, Phase.FAILED, Phase.STOPPED}

    @property
    def is_degraded(self) -> bool:
        return self.phase is Phase.DEGRADED

    @property
    def scaling_status(self) -> StrAnyDict:
        """Get the scaling status from the statement status, if available."""
        scaling_status_dict: StrAnyDict | None = self.status.get("scaling_status")
        if scaling_status_dict is None:
            return {}
        else:
            return scaling_status_dict

    @property
    def is_pool_exhausted(self) -> bool:
        """Is the statement currently pending and waiting for compute resources due to
        compute pool exhaustion?"""
        return (
            self.phase is Phase.PENDING
            and self.scaling_status.get("scaling_state") == "POOL_EXHAUSTED"
        )

    @property
    def phase(self) -> Phase:
        if self._deleted:
            return Phase.DELETED
        return self._phase

    @property
    def compute_pool_id(self) -> str:
        return self.spec["compute_pool_id"]

    @property
    def principal(self) -> str:
        return self.spec["principal"]

    @property
    def sql_kind(self) -> str:
        return self._possible_traits().sql_kind

    @property
    def is_pure_ddl(self) -> bool:
        """Check if this statement is a pure DDL statement that creates or modifies schema objects.

        Pure DDL statements need to complete fully before the created/modified objects
        are ready for use, unlike streaming queries or CTAS which are ready when RUNNING.

        Returns:
            True if the statement is one of: CREATE_TABLE, DROP_TABLE, CREATE_VIEW,
            DROP_VIEW, ALTER_TABLE. False otherwise.
        """
        return self.sql_kind in self._PURE_DDL_KINDS

    @property
    def is_append_only(self) -> bool:
        """Will this statement's results changelog only have insert/append rows?"""

        return self._possible_traits().is_append_only

    @property
    def schema(self) -> Schema | None:
        """Get the result schema of this statement, if available.

        The schema describes the columns and their types in the statement's result set.
        It is populated from the server's traits response (status.traits.schema) when traits
        are present.

        **Return value:**
        - Returns Schema object for query statements (SELECT, etc.)
        - Returns None for DDL statements (CREATE TABLE, DROP TABLE, etc.)
        - Raises InterfaceError if traits unavailable (not polled or FAILED)

        **Why this is not an error for query without schema:**
        While it's unusual for a query statement to lack schema after traits are present,
        this method returns None rather than raising. This allows defensive code patterns
        where schema absence is checked at use time. This is more robust than eager
        validation because:

        1. Schema may become available between reader creation and row fetch
        2. Graceful None handling is clearer than exception handling
        3. Fits the deferred access pattern used throughout the codebase

        Use has_schema() to distinguish legitimate None (DDL) from unexpected None
        (query statement without schema), then decide if that's acceptable for your use case.

        **Usage Pattern - Why Deferred Access Matters:**
        Code should NOT assume schema is available during statement initialization or
        reader setup. Instead:

        - **Defensive approach** (recommended): Code that needs schema should defer access
          until it's actually required (e.g., during row formatting). This handles all cases:
          - schema becomes available between reader creation and row fetch
          - code gracefully handles None schema with clear error messages
          - no need to predict exact timing of schema availability

        - **Eager checking** (not recommended): Checking schema immediately requires precise
          knowledge of statement phase and type, is harder to maintain, and breaks if server
          behavior changes slightly.

        Returns:
            Schema object describing the result columns, or None if unavailable.

        Raises:
            InterfaceError: If traits are unavailable (statement not yet polled or failed).
        """
        return self._possible_traits().schema

    def has_schema(self) -> bool:
        """Check if this statement can have a result schema.

        A statement can have a schema if it's not a DDL statement. Query statements
        (SELECT, INSERT, etc.) produce result sets with schemas, while DDL statements
        (CREATE TABLE, DROP TABLE, etc.) do not.

        This is different from checking if schema is currently populated. Use this to
        distinguish between:
        - **Legitimate None schema**: DDL statements (will never have schema)
        - **Unexpected None schema**: Query statements (should have schema after first poll)

        Returns:
            True if this statement can produce a schema (non-DDL query), False if it's DDL.

        Raises:
            InterfaceError: If traits are unavailable (statement not yet polled or failed).
        """
        return not self.is_pure_ddl

    @property
    def description(self) -> list[tuple] | None:
        # This is required by the cursor object, see https://peps.python.org/pep-0249/#description
        # It's a list of 7-item tuples, the items represent:
        # (name, type_code, display_size, internal_size, precision, scale, null_ok)
        if self.schema is not None:
            return [
                (
                    col.name,
                    col.type.type,
                    None,  # display_size ???
                    None,  # internal_size ???
                    col.type.precision,
                    col.type.scale,
                    col.type.nullable,
                )
                for col in self.schema
            ]
        return None

    @property
    def is_deleted(self) -> bool:
        """Has this statement been explicitly deleted?"""
        return self._deleted

    def set_deleted(self):
        """Mark this statement as deleted."""
        self._deleted = True

    _type_converter: StatementTypeConverter | None = None
    """Cached SchemaTypeConverter for this statement's schema."""

    @property
    def type_converter(self) -> StatementTypeConverter:
        """Get or create the SchemaTypeConverter for this statement's schema.

        The converter handles conversion from JSON-from-API row values to Python values
        based on the statement's schema, for all columns in the result set.

        Should only be called after statement submission for statements that produce a result set,
        otherwise will raise InterfaceError.
        """
        if self.schema is None:
            raise InterfaceError("Cannot get type converter for statement with no schema.")

        if self._type_converter is None:
            self._type_converter = StatementTypeConverter(self.connection, self.schema)

        return self._type_converter

    @classmethod
    def from_response(cls, connection: Connection, response: StrAnyDict) -> Statement:
        """Create a Statement object from the JSON response returned by the statements API."""
        try:
            # Mandatory fields
            statement_id = response["metadata"]["uid"]
            name = response["name"]
            spec = response["spec"]
            status = response["status"]
            metadata = response["metadata"]

            # Check the phase first.
            try:
                phase = Phase(status["phase"])
            except ValueError as err:
                raise OperationalError(
                    f"Received an unknown phase for statement from the server: {status['phase']}. "
                    "This is probably a bug"
                ) from err

            # Parse traits, which includes the statement schema. Won't be present
            # if the statement failed.
            traits = (
                Traits.from_response(status["traits"])
                if "traits" in status and status["traits"] is not None
                else None
            )

            # Defensive check: non-failed statements should have traits
            if traits is None and phase != Phase.FAILED:
                raise OperationalError(
                    f"Received statement '{name}' in phase {phase} without traits. "
                    "This is unexpected and likely indicates a server API change or bug."
                )
        except KeyError as e:
            raise OperationalError(f"Error parsing statement response, missing {e}.") from e

        return cls(connection, statement_id, name, spec, status, metadata, traits, phase)

    def _possible_traits(self) -> Traits:
        """Raise InterfaceError if traits are not available, else return them."""
        traits = self.traits
        if traits is None:
            raise InterfaceError("Statement traits are not available -- failed statement?")
        return traits

    @property
    def end_user_labels(self) -> list[str]:
        """Returns list of end-user labels for this statement, if available.
        End-user labels are labels that were provided by the user at statement
        submission time. They are included in the statement metadata with a
        "user.confluent.io/" prefix, which is stripped off in the returned list.

        If no end-user labels are available, empty list is returned. If metadata is
        not available, raises InterfaceError.
        """

        labels = self.metadata.get("labels")
        if labels is None:
            raise InterfaceError("Statement metadata labels are not available.")

        # strip the "user.confluent.io/" prefix from label keys to get the original end-user labels
        end_user_labels = []
        prefix_length = len(LABEL_PREFIX)

        # For reasons unknown to me, statement labels is modeled as a dict/object
        # in the API, even though currently only used as a set of strings
        # (the values are always "true").
        for key in labels:
            if key.startswith(LABEL_PREFIX):
                end_user_labels.append(key[prefix_length:])

        return end_user_labels


@dataclass(kw_only=True)
class Column:
    """Describes a column in a statement's result set.

    Each column represents a projected expression or field in the SELECT clause, including
    its name, data type, and optional description (from column comments).

    Columns are part of the schema, which is part of the statement's traits. See
    Statement.schema for notes on when schema information becomes available.
    """

    name: str
    """The column name (may be a user-provided alias or auto-generated expression label)."""
    type: ColumnTypeDefinition
    """The data type of the column (e.g., INTEGER, VARCHAR, ARRAY<STRING>, ROW(...), etc.)."""
    description: str | None = None
    """Optional description or comment for this column."""

    @classmethod
    def from_response(cls, data: StrAnyDict) -> Column:
        column_type = ColumnTypeDefinition.from_response(data["type"])
        return cls(name=data["name"], type=column_type, description=data.get("description"))


@dataclass(kw_only=True)
class Schema:
    """Schema describing the columns and types of a statement's result set.

    This represents the structure of rows returned by a SQL query, including the
    column names and their data types. It is used internally for type conversion
    and for formatting rows as dicts (mapping column names to values).

    Schema is part of the statement's traits, which may not be available in early
    phases of statement execution. Code that needs to format rows as dicts should
    be prepared to defer schema access until rows are actually being formatted.
    """

    columns: list[Column]
    """The columns in the schema."""

    @classmethod
    def from_response(cls, data: StrAnyDict) -> Schema:
        columns = [Column.from_response(col) for col in data.get("columns", [])]
        return cls(columns=columns)

    def __iter__(self) -> Iterator[Column]:
        """Iterate over the columns in the schema."""
        return iter(self.columns)


@dataclass(kw_only=True)
class Traits:
    """Parsed statement traits from server response.

    Traits contain metadata about a statement including its schema, SQL kind, and
    whether it is append-only or bounded. They are populated from status.traits
    in the server response.

    **Availability:**
    Traits are typically available in all non-terminal phases (PENDING, RUNNING, etc.)
    and in terminal success phases (COMPLETED). The server does NOT send traits for
    FAILED statements. Traits are initially None until the statement is polled from
    the server for the first time.

    For code that needs the schema (e.g., formatting rows as dicts), defer access
    until rows are actually being formatted, as schema may not be available during
    reader initialization (before first server poll).

    See Statement.schema property for additional notes on schema availability.
    """

    connection_refs: list[str] | None
    """The names of connections that the SQL statement references (e.g., in FROM clauses)."""
    is_append_only: bool
    """Indicates the special case where results of a statement are insert/append only
       (indicating simple changelog parsing. May be either a streaming or batch/snapshot query.)."""
    is_bounded: bool
    """Does the result set have a bounded number of rows (aka not a streaming result?
       Implies is_append_only.)"""
    schema: Schema | None
    """The schema of the result set, describing columns and their types.

    **Lifecycle:**
    - During statement execution (PENDING → RUNNING → COMPLETED): May be present or absent
    - DDL statements (CREATE TABLE, DROP TABLE, etc.): Always None (no result set)
    - Query statements (SELECT, etc.): Present after first server poll (typically by PENDING phase)
    - FAILED statements: Never populated (traits omitted by server)

    **When it's None:**
    - Before the statement is first polled from the server
    - For DDL statements that don't produce result sets
    - For FAILED statements (traits not sent)
    - In rare cases where server doesn't include schema (defensive checks warn about this)

    Code that requires schema should check for None and handle gracefully, or use deferred
    access patterns (e.g., RowFormatter lazy initialization) to access schema only when needed.

    See Statement.schema property for usage guidance.
    """
    sql_kind: str
    upsert_columns: list[int] | None
    """Zero-based indices of upsert columns, if any."""

    @classmethod
    def from_response(cls, data: StrAnyDict) -> Traits:
        schema_data = data.get("schema")
        schema = Schema.from_response(schema_data) if schema_data else None
        return cls(
            connection_refs=data.get("connection_refs"),
            is_append_only=data["is_append_only"],
            is_bounded=data["is_bounded"],
            schema=schema,
            sql_kind=data["sql_kind"],
            upsert_columns=data.get("upsert_columns"),
        )
