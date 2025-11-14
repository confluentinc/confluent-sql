"""Type conversions between Flink statement API string serializations and python representations."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypeAlias

if TYPE_CHECKING:
    from .statement import ColumnType, Schema


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
"""Describes all possible encoding types returned from from-response API calls, including nested row types."""


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

    _column_type: ColumnType

    def __init__(self, column_type: ColumnType):
        self._column_type = column_type

    def to_python_value(self, sql_value: str) -> Any:
        """Convert a SQL string representation to its Python value."""
        raise NotImplementedError("Subclasses should implement this method.")


def get_data_type_converter(column_type: ColumnType) -> TypeConverter:
    """Factory method to get the appropriate TypeConverter for a given ColumnType."""
    cls = _flink_type_to_converter_map.get(column_type.type)
    if cls:
        return cls(column_type)

    # Add more type mappings as needed.
    raise NotImplementedError(f"TypeConverter for {column_type.type} is not implemented.")


class StringConverter(TypeConverter):
    """Handles Flink types for CHAR, VARCHAR, STRING"""

    def to_python_value(self, sql_value: str | None) -> str | None:
        if sql_value is None:
            return None
        return sql_value


class BooleanConverter(TypeConverter):
    def to_python_value(self, sql_value: str | bool | None) -> bool | None:
        breakpoint()

        if sql_value is None:
            return None

        if isinstance(sql_value, bool):
            return sql_value

        return sql_value.lower() == "true"


class IntegerConverter(TypeConverter):
    def to_python_value(self, sql_value: str | None) -> int | None:
        if sql_value is None:
            return None
        return int(sql_value)


_flink_type_to_converter_map: dict[str, type[TypeConverter]] = {
    "BOOLEAN": StringConverter,
    "TINYINT": IntegerConverter,
    "SMALLINT": IntegerConverter,
    "INT": IntegerConverter,
    "INTEGER": IntegerConverter,
    "BIGINT": IntegerConverter,
    "CHAR": StringConverter,
    "VARCHAR": StringConverter,
    "STRING": StringConverter,
}
