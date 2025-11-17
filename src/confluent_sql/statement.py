from __future__ import annotations

import logging
from collections.abc import Iterator
from dataclasses import dataclass
from enum import Enum
from typing import Any, TypeAlias

from confluent_sql.types import SchemaTypeConverter

from .exceptions import DatabaseError, OperationalError

logger = logging.getLogger(__name__)

StrAnyDict: TypeAlias = dict[str, Any]


class Op(Enum):
    """Row operation types for Flink SQL changelog streams."""

    INSERT = 0
    UPDATE_BEFORE = 1
    UPDATE_AFTER = 2
    DELETE = 3

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
            raise ValueError(f"Unknown value for Op: '{self.value}'. This is probably a bug")


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
    # From the API response fields ...
    statement_id: str
    name: str
    spec: StrAnyDict
    status: StrAnyDict
    # Parsed fields ...
    traits: Traits

    # Internal state
    _phase: Phase
    _deleted: bool = False

    @property
    def is_ready(self) -> bool:
        if self.is_bounded:
            return self.phase in [Phase.COMPLETED, Phase.STOPPED]
        else:
            return self.phase in [Phase.COMPLETED, Phase.STOPPED, Phase.RUNNING]

    @property
    def is_running(self) -> bool:
        return self.phase == Phase.RUNNING

    @property
    def is_completed(self) -> bool:
        return self.phase is Phase.COMPLETED

    @property
    def is_failed(self) -> bool:
        return self.phase is Phase.FAILED

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
        return self.traits.sql_kind

    @property
    def is_bounded(self) -> bool:
        return self.traits.is_bounded

    @property
    def is_append_only(self) -> bool:
        return self.traits.is_append_only

    @property
    def schema(self) -> Schema | None:
        return self.traits.schema

    @property
    def connection_refs(self) -> list | None:
        return self.traits.connection_refs

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

    _type_converter: SchemaTypeConverter | None = None
    """Cached SchemaTypeConverter for this statement's schema."""

    @property
    def type_converter(self) -> SchemaTypeConverter | None:
        """Get or create the SchemaTypeConverter for this statement's schema.

        The converter handles conversion from JSON-from-API row values to Python values
        based on the statement's schema, for all columns in the result set.
        """
        if self.schema is None:
            return None
        if self._type_converter is None:
            self._type_converter = SchemaTypeConverter(self.schema)
        return self._type_converter

    @classmethod
    def from_response(cls, response: StrAnyDict) -> Statement:
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

            # If it's failed, we won't get 'traits', and it's probably good to raise an error.
            # TODO: Should we instead set the phase and avoid erroring out here?
            if phase is Phase.FAILED:
                raise DatabaseError(status["detail"])

            # Parse traits, which includes the statement schema.
            traits = Traits.from_response(status["traits"])
        except KeyError as e:
            raise OperationalError(f"Error parsing statement response, missing {e}.") from e

        return cls(statement_id, name, spec, status, traits, phase)


@dataclass(kw_only=True)
class ColumnTypeDefinition:
    """Fields corresponding to statement.traits.schema.columns[].type members.

    Describes the Flink-side type defitition of a projected column.
    """

    type: str
    """Flink name of the type, e.g., "INT", "STRING", "ROW", etc."""
    nullable: bool
    length: int | None = None
    precision: int | None = None
    scale: int | None = None
    fractional_precision: int | None = None  # if an interval type
    resolution: str | None = None  # if an interval type
    key_type: str | None = None  # if type == "MAP"
    value_type: str | None = None  # if type == "MAP"
    element_type: str | None = None  # if type == "ARRAY"

    fields: list[RowColumn] | None = None
    """The interior fields of a ROW type, if applicable."""

    class_name: str | None = None
    """The Flink-side class name of the structured data type (if applicable)."""

    @property
    def type_name(self) -> str:
        """Return the Flink type name. Aliasing for clarity."""
        return self.type

    @classmethod
    def from_response(cls, data: StrAnyDict) -> ColumnTypeDefinition:
        """Create a ColumnTypeDefinition from JSON response data within from-API statement traits"""
        return cls(
            type=data["type"],
            nullable=data["nullable"],
            length=data.get("length"),
            precision=data.get("precision"),
            scale=data.get("scale"),
            fractional_precision=data.get("fractional_precision"),
            resolution=data.get("resolution"),
            key_type=data.get("key_type"),
            value_type=data.get("value_type"),
            element_type=data.get("element_type"),
            fields=[RowColumn.from_response(field) for field in data.get("fields", [])]
            if "fields" in data
            else None,
            class_name=data.get("class_name"),
        )


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


@dataclass
class RowColumn:
    """Fields corresponding to statement.traits.schema.columns[].type.fields members.
    Used when the column type is a ROW.
    Would be identical to Column, but the field carrying the type information is named differently.
    """

    name: str
    field_type: ColumnTypeDefinition
    description: str | None = None

    @property
    def type(self) -> ColumnTypeDefinition:
        """Alias for field_type to match Column. The API design is inconsistent here."""
        return self.field_type

    @classmethod
    def from_response(cls, data: StrAnyDict) -> RowColumn:
        column_type = ColumnTypeDefinition.from_response(data["type"])
        return cls(name=data["name"], field_type=column_type, description=data.get("description"))


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
       (indicicating simple changelog parsing)."""
    is_bounded: bool
    """Does the result set have a bounded number of rows (aka not a streaming result?)"""
    schema: Schema | None
    """The schema of the result set, if any."""
    sql_kind: str  # TODO will grow into an enum some day soon. It always does.
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
