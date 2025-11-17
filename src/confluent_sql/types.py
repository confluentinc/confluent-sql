"""Type conversions between Flink statement API string serializations and python representations."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypeAlias

if TYPE_CHECKING:
    from .statement import ColumnTypeDefinition, Schema


__all__ = [
    "SchemaTypeConverter",
]

"""
    Type conversion between the SQL results API and Python values, driven by the schema information
"""

FromResponseScalarTypes: TypeAlias = str | bool | None
"""Describes all possible scalar encoding types returned from from-response API calls."""

# Grr, row types are fully recursive and come to us in JSON as a nested list.
FromResponseTypes: TypeAlias = FromResponseScalarTypes | list["FromResponseTypes"]
"""Describes all possible encoding types returned from from-response API calls, including
   nested row types."""


class SchemaTypeConverter:
    """
    Acts on behalf of a Schema to convert SQL string values to Python values."""

    _schema: Schema
    _type_converters: list[TypeConverter]

    def __init__(self, schema: Schema):
        self._schema = schema
        self._type_converters = [get_data_type_converter(col.type) for col in schema.columns]

    def to_python_row(self, sql_row: tuple[FromResponseTypes]) -> tuple[Any]:
        """Convert a SQL row (list of from-results-API encoded values) to a Python row
        (tuple of Python values) to be returned by a Cursor."""
        return tuple(
            col.to_python_value(sql_value)  # type: ignore[arg-type]
            for col, sql_value in zip(self._type_converters, sql_row, strict=True)
        )


class TypeConverter:
    """Base class for all Flink -> Python data type converters."""

    _column_type: ColumnTypeDefinition

    def __init__(self, column_type: ColumnTypeDefinition):
        self._column_type = column_type

    def to_python_value(self, response_value: FromResponseTypes) -> Any:
        """Convert from statement-response-API-JSON representation to its Python value."""
        raise NotImplementedError("Subclasses should implement this method.")


def get_data_type_converter(column_type: ColumnTypeDefinition) -> TypeConverter:
    """Return the appropriate TypeConverter for a given from-Statement-JSON type description."""
    # Find the appropriate converter class mapped from the Flink type name
    cls = _flink_type_name_to_converter_map.get(column_type.type_name)
    if cls:
        return cls(column_type)

    # Add more type mappings as needed!
    raise NotImplementedError(f"TypeConverter for {column_type.type_name} is not implemented.")


class StringConverter(TypeConverter):
    """Handles Flink types for CHAR, VARCHAR, STRING"""

    def to_python_value(self, response_value: FromResponseTypes) -> str | None:
        """Expect string or None from the response value, return as-is or raise ValueError."""
        if response_value is None:
            return None

        if not isinstance(response_value, str):
            raise ValueError(
                f"Expected string value for StringConverter but got {type(response_value)}"
            )

        return response_value


class IntegerConverter(TypeConverter):
    def to_python_value(self, response_value: FromResponseTypes) -> int | None:
        """Expect string-encoded integer or None from the response value, return as int
        or raise ValueError."""
        if response_value is None:
            return None

        if not isinstance(response_value, str):
            raise ValueError(
                f"Expected SQL integers to be encoded as JSON strings but got {type(response_value)}"
            )

        return int(response_value)


class BooleanConverter(TypeConverter):
    def to_python_value(self, response_value: FromResponseTypes) -> bool | None:
        """Expect string 'TRUE'/'FALSE' or None from the response value, return as bool
        or raise ValueError."""
        if response_value is None:
            return None

        if not isinstance(response_value, str):
            raise ValueError(
                f"Expected string value for BooleanConverter but got {type(response_value)}"
            )

        return response_value.lower() == "true"


_flink_type_name_to_converter_map: dict[str, type[TypeConverter]] = {
    "BOOLEAN": BooleanConverter,
    #
    "TINYINT": IntegerConverter,
    "SMALLINT": IntegerConverter,
    "INT": IntegerConverter,
    "INTEGER": IntegerConverter,
    "BIGINT": IntegerConverter,
    #
    "CHAR": StringConverter,
    "VARCHAR": StringConverter,
    "STRING": StringConverter,
}
