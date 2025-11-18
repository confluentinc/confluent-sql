"""Type conversions between Flink statement API string serializations and python representations."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypeAlias

from confluent_sql.exceptions import InterfaceError

if TYPE_CHECKING:
    from .statement import ColumnTypeDefinition, Schema


__all__ = [
    "StatementTypeConverter",
    "TypeConverter",
    "convert_statement_parameters",
]

"""
    Type conversion between the SQL results API and Python values, driven by the schema information
"""

FromResponseScalarTypes: TypeAlias = str | bool | None
"""Describes all possible scalar encoding types returned from from-response API calls."""

# Row types are fully recursive and come to us in JSON as a nested list.
FromResponseTypes: TypeAlias = FromResponseScalarTypes | list["FromResponseTypes"]
"""
Describes all possible encoding types returned from from-response API calls, including
nested row types.
"""


class StatementTypeConverter:
    """
    Acts on behalf of a statement's Schema to convert from-API-JSON-changelog values to Python,
    values.
    """

    _schema: Schema
    _type_converters: list[TypeConverter]

    def __init__(self, schema: Schema):
        self._schema = schema
        self._type_converters = [get_api_type_converter(col.type) for col in schema.columns]

    def to_python_row(self, sql_row: tuple[FromResponseTypes]) -> tuple[Any]:
        """Convert a SQL row (list of from-results-API encoded values) to a Python row
        (tuple of Python values) to be returned by a Cursor."""
        return tuple(
            col.to_python_value(sql_value)  # type: ignore[arg-type]
            for col, sql_value in zip(self._type_converters, sql_row, strict=True)
        )


class TypeConverter:
    """Base class for all Flink <-> Python data type converters."""

    _column_type: ColumnTypeDefinition

    def __init__(self, column_type: ColumnTypeDefinition):
        self._column_type = column_type

    def to_python_value(self, response_value: FromResponseTypes) -> Any:
        """Convert from statement-response-API-JSON representation to its Python value."""
        raise NotImplementedError("Subclasses should implement this method.")  # pragma: no cover

    @classmethod
    def to_statement_string(cls, python_value: Any) -> str:
        """Convert from Python value to its for-statement-string-interpolation representation."""
        raise NotImplementedError("Subclasses should implement this method.")  # pragma: no cover


def get_api_type_converter(column_type: ColumnTypeDefinition) -> TypeConverter:
    """Return the appropriate TypeConverter for a given from-Statement-JSON type description."""
    # Find the appropriate converter class mapped from the Flink type name
    cls = _flink_type_name_to_converter_map.get(column_type.type_name)
    if not cls:
        # Another type mapping needed!
        raise NotImplementedError(f"TypeConverter for {column_type.type_name} is not implemented.")

    return cls(column_type)


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

    @classmethod
    def to_statement_string(cls, python_value: Any) -> str:
        """Convert a Python string value to its for-statement-string-interpolation
        representation."""
        if not isinstance(python_value, str):
            raise ValueError(
                f"Expected Python string value for StringConverter but got {type(python_value)}"
            )
        # Escape single quotes by doubling them
        escaped_value = python_value.replace("'", "''")
        # Return wrapped in single quotes
        return f"'{escaped_value}'"


class IntegerConverter(TypeConverter):
    def to_python_value(self, response_value: FromResponseTypes) -> int | None:
        """Expect string-encoded integer or None from the response value, return as int
        or raise ValueError."""
        if response_value is None:
            return None

        if not isinstance(response_value, str):
            raise ValueError(
                f"Expected integers to be encoded as JSON strings but got {type(response_value)}"
            )

        return int(response_value)

    @classmethod
    def to_statement_string(cls, python_value: Any) -> str:
        """Convert a Python integer value to its for-statement-string-interpolation
        representation."""
        if not isinstance(python_value, int):
            raise ValueError(
                f"Expected Python integer value for IntegerConverter but got {type(python_value)}"
            )
        return str(python_value)


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

    @classmethod
    def to_statement_string(cls, python_value: Any) -> str:
        """Convert a Python boolean value to its for-statement-string-interpolation
        representation."""
        if not isinstance(python_value, bool):
            raise ValueError(
                f"Expected Python boolean value for BooleanConverter but got {type(python_value)}"
            )
        return "TRUE" if python_value else "FALSE"


class NullConverter(TypeConverter):
    def to_python_value(self, response_value: FromResponseTypes) -> None:
        """Expect None from the response value, return None or raise ValueError."""
        if response_value is not None:
            raise ValueError(
                f"Expected None value for NullConverter but got {type(response_value)}"
            )

        # implicitly return None

    @classmethod
    def to_statement_string(cls, python_value: Any) -> str:
        """Convert a Python None value to its statement-embedding representation."""
        if python_value is not None:
            raise ValueError(
                f"Expected Python None value for NullConverter but got {type(python_value)}"
            )
        return "NULL"


_flink_type_name_to_converter_map: dict[str, type[TypeConverter]] = {
    # Null type
    "NULL": NullConverter,
    # Boolean type
    "BOOLEAN": BooleanConverter,
    # Integer types
    "TINYINT": IntegerConverter,
    "SMALLINT": IntegerConverter,
    "INTEGER": IntegerConverter,
    "BIGINT": IntegerConverter,
    # String types
    "CHAR": StringConverter,
    "VARCHAR": StringConverter,
    "STRING": StringConverter,
}


_python_type_to_type_converter: dict[type, type[TypeConverter]] = {
    str: StringConverter,
    int: IntegerConverter,
    bool: BooleanConverter,
    None.__class__: NullConverter,
}


def convert_statement_parameters(
    parameters: tuple | list,
) -> tuple:
    """Convert a list or tuple of Python parameters to a tuple of their string representations
    for interpolation into a %s-laden statement string.

    Returns: A tuple of string representations of the parameters.
    """
    converted_params = []
    for param in parameters:
        converter_cls = _python_type_to_type_converter.get(type(param))
        if not converter_cls:
            raise InterfaceError(
                f"Conversion for parameter of type {type(param)} is not implemented."
            )
        param_as_flink_string = converter_cls.to_statement_string(param)
        converted_params.append(param_as_flink_string)

    return tuple(converted_params)
