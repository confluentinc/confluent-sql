from __future__ import annotations

import logging
from collections.abc import Iterator
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

from .exceptions import InterfaceError, OperationalError
from .types import ColumnTypeDefinition, FromResponseTypes, StatementTypeConverter, StrAnyDict

if TYPE_CHECKING:
    from .connection import Connection

logger = logging.getLogger(__name__)


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


@dataclass
class Statement:
    """Represents a Confluent SQL statement, including its metadata, spec, status,
    and parsed traits such as schema, sql kind, etc."""

    # From the cursor that created this statement ...
    connection: Connection

    # From the API response fields ...
    statement_id: str
    name: str
    spec: StrAnyDict
    status: StrAnyDict
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

    @property
    def is_ready(self) -> bool:
        """Is the statement in a ready state for consumption/deletion?"""
        if self.is_bounded:
            # Bounded statements are ready if completed, stopped, or failed.
            return self.phase in [Phase.COMPLETED, Phase.STOPPED, Phase.FAILED]
        else:
            # Streaming statements are ready if running, completed, or stopped, failed
            return self.phase in [
                Phase.COMPLETED,
                Phase.STOPPED,
                Phase.RUNNING,
                Phase.FAILED,
            ]

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
    def is_append_only(self) -> bool:
        """Will this statement's results changelog only have insert/append rows?"""

        return self._possible_traits().is_append_only

    @property
    def schema(self) -> Schema | None:
        return self._possible_traits().schema

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
        except KeyError as e:
            raise OperationalError(f"Error parsing statement response, missing {e}.") from e

        return cls(connection, statement_id, name, spec, status, traits, phase)

    def _possible_traits(self) -> Traits:
        """Raise InterfaceError if traits are not available, else return them."""
        traits = self.traits
        if traits is None:
            raise InterfaceError("Statement traits are not available -- failed statement?")
        return traits


@dataclass(kw_only=True)
class Column:
    """Fields correspond to statement.traits.schema.columns[] members
    Describes a projected column in the statement's result set: name, type definition,
    description (column comment).
    """

    name: str
    type: ColumnTypeDefinition
    description: str | None = None

    @classmethod
    def from_response(cls, data: StrAnyDict) -> Column:
        column_type = ColumnTypeDefinition.from_response(data["type"])
        return cls(name=data["name"], type=column_type, description=data.get("description"))


@dataclass(kw_only=True)
class Schema:
    """Fields correspond to statement.traits.schema"""

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
    """Fields correspond to statement.traits, including the statement's schema."""

    connection_refs: list[str] | None
    """The names of connections that the SQL statement references (e.g., in FROM clauses)."""
    is_append_only: bool
    """Indicates the special case where results of a statement are insert/append only
       (indicating simple changelog parsing. May be either a streaming or batch/snapshot query.)."""
    is_bounded: bool
    """Does the result set have a bounded number of rows (aka not a streaming result?
       Implies is_append_only.)"""
    schema: Schema | None
    """The schema of the result set, if any."""
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
