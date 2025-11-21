"""Type conversions between Flink statement API string serializations and python representations."""

from __future__ import annotations

from cmath import isnan
from datetime import date, datetime, time, timezone
from decimal import Decimal
from math import isinf
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
        string literal representation."""

        ##
        ## Flink only uses single quotes to delimit string literals, and escapes
        ## single quotes inside string literals by doubling them.
        ##
        ## Backslash escaping is not supported in Flink SQL string literals -- that
        ## is, a backslash is just a normal character in a Flink SQL string literal.
        ##
        ## Backticks are used in Flink SQL to delimit identifiers, not string literals,
        ## and to have special meaning they must be the outermost delimiters. They
        ## do not need to be internally escaped in string literals.
        ##

        if not isinstance(python_value, str):
            raise ValueError(
                f"Expected Python string value for StringConverter but got {type(python_value)}"
            )

        # Ensure we're dealing with a standard str here, and not a subclass
        # that might do something "creative" when we do string operations on it.
        python_value = str(python_value)

        # Escape single quotes by doubling them
        escaped_value = python_value.replace("'", "''")

        # Return wrapped in single quotes
        return f"'{escaped_value}'"


class VarBinaryConverter(TypeConverter):
    """Handles Flink type VARBINARY"""

    def to_python_value(self, response_value: FromResponseTypes) -> bytes | None:
        """Expect hex-pair encoded string or None from the response value, return as bytes
        or raise ValueError.

        Examples: "x'7f0203'" <-> b"\x7f\x02\x03"
        """
        if response_value is None:
            return None

        if not isinstance(response_value, str):
            raise ValueError(
                f"Expected string value for VarBinaryConverter but got {type(response_value)}"
            )

        if not (response_value.startswith("x'") and response_value.endswith("'")):
            raise ValueError(
                f"Expected hex-pair encoded string starting with x' and ending with ' "
                f"for VarBinaryConverter but got {response_value}"
            )

        hex_string = response_value[2:-1]  # Strip off the x' and trailing '
        try:
            return bytes.fromhex(hex_string)
        except ValueError as e:
            raise ValueError(f"Invalid hex string for VarBinaryConverter: {hex_string}") from e

    @classmethod
    def to_statement_string(cls, python_value: Any) -> str:
        """Convert a Python bytes value to its for-statement-string-interpolation
        representation.

        Examples: b"\x7f\x02\x03" -> "x'7f0203'"
        """
        if not isinstance(python_value, bytes):
            raise ValueError(
                f"Expected bytes value for VarBinaryConverter but got {type(python_value)}"
            )
        hex_string = python_value.hex()
        return f"x'{hex_string}'"


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

        # Guard against "creative" types that pass as int but aren't really ints
        # by recasting to int before stringifying.

        return str(int(python_value))


class DecimalConverter(TypeConverter):
    """Handle fixed precision DECIMAL types, mapping to Python's decimal.Decimal"""

    def to_python_value(self, response_value: FromResponseTypes) -> Decimal | None:
        """Expect string-encoded decimal or None from the response value, return as str
        or raise ValueError."""
        if response_value is None:
            return None

        if not isinstance(response_value, str):
            raise ValueError(
                f"Expected decimal to be encoded as JSON strings but got {type(response_value)}"
            )

        return Decimal(response_value)

    @classmethod
    def to_statement_string(cls, python_value: Any) -> str:
        """Convert a Python Decimal value to its for-statement-string-interpolation
        representation."""
        if not isinstance(python_value, Decimal):
            raise ValueError(
                f"Expected Python Decimal value for DecimalConverter but got {type(python_value)}"
            )

        # Must include explicit cast to DECIMAL to avoid Flink interpreting
        # the literal as a DOUBLE.

        # Must include precision and scale in the cast to get any decimal
        # value with fractional part honored, otherwise Flink will
        # truncate to integer.
        precision = len(python_value.as_tuple().digits)  # type: ignore[attr-defined]
        scale = -python_value.as_tuple().exponent  # type: ignore[attr-defined]

        return f"cast('{python_value}' as decimal({precision},{scale}))"


class FloatConverter(TypeConverter):
    """Handles Flink types for FLOAT, DOUBLE to Python float"""

    def to_python_value(self, response_value: FromResponseTypes) -> float | None:
        """Expect string-encoded float or None from the response value, return as float
        or raise ValueError."""
        if response_value is None:
            return None

        if not isinstance(response_value, str):
            raise ValueError(
                f"Expected float to be encoded as JSON string but got {type(response_value)}"
            )

        return float(response_value)

    @classmethod
    def to_statement_string(cls, python_value: Any) -> str:
        """Convert a Python float value to its for-statement-string-interpolation
        representation as a Flink double.

        Err on the side of casting to the higher-precision DOUBLE type to avoid
        precision loss in FLOAT representation if the target type ended up
        being DOUBLE.
        """
        if not isinstance(python_value, float):
            raise ValueError(
                f"Expected Python float value for FloatConverter but got {type(python_value)}"
            )

        # Check for NaN or Infinity, IEEEE 754 float representation allows these values, but Flink
        # SQL does not (statement will crash).
        if isnan(python_value) or isinf(python_value):
            raise ValueError("Cannot convert NaN or Infinity to a Flink SQL float/double literal")

        # Will be interpolated as a literal number in the statement, no quotes.
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

        return None  # noqa: PLR1711 # explicit return for clarity.

    @classmethod
    def to_statement_string(cls, python_value: Any) -> str:
        """Convert a Python None value to its statement-embedding representation."""
        if python_value is not None:
            raise ValueError(
                f"Expected Python None value for NullConverter but got {type(python_value)}"
            )
        return "NULL"


class DateConverter(TypeConverter):
    """Handles Flink DATE type to Python datetime.date"""

    def to_python_value(self, response_value: FromResponseTypes) -> date | None:
        """Expect string-encoded date in 'YYYY-MM-DD' format or None from the response value,
        return as datetime.date or raise ValueError."""
        if response_value is None:
            return None

        if not isinstance(response_value, str):
            raise ValueError(
                f"Expected date to be encoded as JSON string but got {type(response_value)}"
            )

        try:
            date = datetime.fromisoformat(response_value).date()
            return date
        except Exception as e:
            raise ValueError(f"Invalid date string for DateConverter: {response_value}") from e

    @classmethod
    def to_statement_string(cls, python_value: Any) -> str:
        """Convert a Python datetime.date value to its for-statement-string-interpolation
        representation, quoted YYYY-MM-DD."""

        if not isinstance(python_value, date):
            raise ValueError(
                f"Expected Python datetime.date value for DateConverter "
                f"but got {type(python_value)}"
            )

        # Our use cases need the prefixed 'DATE' keyword, so include it here.
        return f"DATE '{python_value.isoformat()}'"


class TimeConverter(TypeConverter):
    """Handles Flink TIME type to Python datetime.time"""

    def to_python_value(self, response_value: FromResponseTypes) -> time | None:
        """Expect string-encoded time in 'HH:MM:SS(.MMMMMM)' format or None from the response value,
        return as datetime.time or raise ValueError."""
        if response_value is None:
            return None

        if not isinstance(response_value, str):
            raise ValueError(
                f"Expected time to be encoded as JSON string but got {type(response_value)}"
            )

        try:
            return time.fromisoformat(response_value)
        except Exception as e:
            raise ValueError(f"Invalid time string for TimeConverter: {response_value}") from e

    @classmethod
    def to_statement_string(cls, python_value: Any) -> str:
        """Convert a Python datetime.time value to its for-statement-string-interpolation
        representation, quoted `' TIME HH:MM:SS.MMMMMM.XXXXX'`"""

        if not isinstance(python_value, time):
            raise ValueError(
                f"Expected Python datetime.time value for TimeConverter "
                f"but got {type(python_value)}"
            )

        return f"TIME '{python_value.isoformat(timespec='microseconds')}'"


class TimestampConverter(TypeConverter):
    """Handles converting Flink TIMESTAMP and TIMESTAMP_LTZ types to/from
    Python datetime.datetime (with or with tzinfo).

    When converting from Python datetime to Flink TIMESTAMP representation, if the
    datetime carries tzinfo, it is transposed to the equivalent UTC time before conversion,
    which should correspond to any submitted statement's default statement property
    'sql.local-time-zone' default setting of UTC.

    When converting from Flink TIMESTAMP type, a tz-naive datetime is returned.
    When converting from Flink TIMESTAMP_LTZ type, a tz-aware datetime with tzinfo=UTC is returned.

    Therefore, when round-tripping a tz-aware datetime through TIMESTAMP_LTZ, the original
    tzinfo is lost (if not UTC) and replaced with UTC, but the instant in time is preserved.

    When providing data intented for TIMESTAMP columns, tz-independent datetimes should be used.
    When providing data intended for TIMESTAMP_LTZ columns, tz-aware datetimes should be used.
    """

    def __init__(self, column_type: ColumnTypeDefinition):
        # Prevent confusion from possible aliases (test suite). Statement schema
        # JSON spells these out canonically.
        if column_type.type_name not in (
            "TIMESTAMP_WITHOUT_TIME_ZONE",
            "TIMESTAMP_WITH_LOCAL_TIME_ZONE",
        ):
            raise ValueError(
                f"TimestampConverter can only be used with TIMESTAMP_WITHOUT_TIME_ZONE or"
                f" TIMESTAMP_WITH_LOCAL_TIME_ZONE types, got {column_type.type_name}"
            )
        super().__init__(column_type)

    @classmethod
    def to_statement_string(cls, python_value: Any) -> str:
        """Convert a Python datetime.datetime value to its for-statement-string-interpolation
        representation, based on whether it has tzinfo or not."""

        if not isinstance(python_value, datetime):
            raise ValueError(
                f"Expected Python datetime.datetime value for TimestampConverter "
                f"but got {type(python_value)}"
            )

        # If has tzinfo, convert to UTC time w/o tzinfo for Flink TIMESTAMP_LTZ
        if python_value.tzinfo is not None:
            python_value = python_value.astimezone(tz=timezone.utc).replace(tzinfo=None)
            # Must explicitly cast in the string forms ...
            flink_type = "timestamp_ltz"
        else:
            flink_type = "timestamp"

        iso_str = python_value.isoformat(sep=" ", timespec="microseconds")
        return f"cast('{iso_str}' as {flink_type})"

    def to_python_value(self, response_value: FromResponseTypes) -> datetime | None:
        """Expect string-encoded timestamp in 'YYYY-MM-DD HH:MM:SS(.MMMMMM)' format
        or None from the response value, return as datetime.datetime or raise ValueError.

        If the column type is TIMESTAMP_LTZ, the returned datetime will have tzinfo=UTC,
        otherwise it will be tz-naive.
        """

        if response_value is None:
            return None

        if not isinstance(response_value, str):
            raise ValueError(
                f"Expected timestamp to be encoded as JSON string but got {type(response_value)}"
            )

        try:
            # Should only be given TZ-free strings from Flink, otherwise the logic here
            # may be rotten and should be reconsidered.
            dt = datetime.fromisoformat(response_value)

        except Exception as e:
            raise ValueError(
                f"Invalid timestamp string for TimestampConverter: {response_value}"
            ) from e

        if dt.tzinfo is not None:
            raise ValueError(
                f"Expected timezone-naive timestamp string from Flink but got {response_value}"
            )

        # But if we're dealing with TIMESTAMP_LTZ, we should interpret
        # the timestamp as being in UTC and set tzinfo accordingly.
        if self._column_type.type_name == "TIMESTAMP_WITH_LOCAL_TIME_ZONE":
            dt = dt.replace(tzinfo=timezone.utc)

        return dt


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
    # Fixed precision types
    "DECIMAL": DecimalConverter,
    "DEC": DecimalConverter,
    "NUMERIC": DecimalConverter,
    # Floating point types
    "FLOAT": FloatConverter,
    "DOUBLE": FloatConverter,
    "DOUBLE PRECISION": FloatConverter,
    # Date type
    "DATE": DateConverter,
    # Time type
    "TIME": TimeConverter,
    "TIME_WITHOUT_TIME_ZONE": TimeConverter,
    # Timestamp type
    "TIMESTAMP": TimestampConverter,
    "TIMESTAMP_WITHOUT_TIME_ZONE": TimestampConverter,
    "TIMESTAMP_LTZ": TimestampConverter,
    "TIMESTAMP_WITH_LOCAL_TIME_ZONE": TimestampConverter,
    # String types
    "CHAR": StringConverter,
    "VARCHAR": StringConverter,
    "STRING": StringConverter,
    # Binary types
    "VARBINARY": VarBinaryConverter,
    "BINARY": VarBinaryConverter,
    "BYTES": VarBinaryConverter,
}


_python_type_to_type_converter: dict[type, type[TypeConverter]] = {
    None.__class__: NullConverter,
    bool: BooleanConverter,
    int: IntegerConverter,
    Decimal: DecimalConverter,
    float: FloatConverter,
    date: DateConverter,
    time: TimeConverter,
    str: StringConverter,
    bytes: VarBinaryConverter,
    datetime: TimestampConverter,
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
