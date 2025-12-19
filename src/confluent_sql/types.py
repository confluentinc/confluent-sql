"""Type conversions between Flink statement API string serializations and python representations."""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from math import isinf, isnan
from types import NoneType
from typing import TYPE_CHECKING, Any, Generic, TypeAlias, TypeVar

from confluent_sql.exceptions import InterfaceError

PyType = TypeVar("PyType")
"""The data type of the Python value being converted to/from Flink SQL representation by
a TypeConverter subclass."""


if TYPE_CHECKING:
    from .statement import ColumnTypeDefinition, Schema

__all__ = [
    "StatementTypeConverter",
    "TypeConverter",
    "convert_statement_parameters",
    "SqlNone",
    "YearMonthInterval",
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
    values. Drives per-column TypeConverter deserialization to python types based on the schema.
    """

    _schema: Schema
    _type_converters: list[TypeConverter]

    def __init__(self, schema: Schema):
        self._schema = schema
        self._type_converters = [get_api_type_converter(col.type) for col in schema.columns]

    def to_python_row(self, sql_row: list[FromResponseTypes]) -> tuple[Any]:
        """Convert a SQL row (list of from-results-API encoded values) to a Python row
        (tuple of Python values) to be returned by a Cursor."""
        return tuple(
            converter.to_python_value(sql_value)  # type: ignore[arg-type]
            for converter, sql_value in zip(self._type_converters, sql_row, strict=True)
        )


class TypeConverter(Generic[PyType]):
    """Base class for all Flink <-> Python data type converters.

    A TypeConverter handles conversion between a specific Flink SQL type's
    representation in the statement API JSON responses and the corresponding
    Python type.

    Conversion from Flink SQL type to Python type is handled by the instance method
    `to_python_value()`, which takes a from-response-API-JSON-encoded value and returns
    the corresponding Python value, and may be hinted by the ColumnTypeDefinition
    further clarifying the Flink-side type provided at construction time (from
    the statement's schema).
    """

    PRIMARY_FLINK_TYPE_NAME: str
    """The primary Flink SQL type name that this TypeConverter handles."""

    _column_type: ColumnTypeDefinition

    def __init__(self, column_type: ColumnTypeDefinition):
        self._column_type = column_type

    def to_python_value(self, response_value: FromResponseTypes) -> PyType | None:
        """Convert from statement-response-API-JSON representation to its Python value.

        All columns might also be nullable, in which case None should be returned.
        """
        raise NotImplementedError("Subclasses should implement this method.")  # pragma: no cover

    @classmethod
    def to_statement_string(cls, python_value: PyType) -> str:
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


class StringConverter(TypeConverter[str]):
    """Handles Flink types for CHAR, VARCHAR, STRING"""

    PRIMARY_FLINK_TYPE_NAME = "STRING"

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
    def to_statement_string(cls, python_value: str) -> str:
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


class VarBinaryConverter(TypeConverter[bytes]):
    """Handles Flink type VARBINARY"""

    PRIMARY_FLINK_TYPE_NAME = "VARBINARY"

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
    def to_statement_string(cls, python_value: bytes) -> str:
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


class IntegerConverter(TypeConverter[int]):
    """Handles Flink types for TINYINT, SMALLINT, INTEGER, BIGINT to/from Python int"""

    PRIMARY_FLINK_TYPE_NAME = "INTEGER"

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
    def to_statement_string(cls, python_value: int) -> str:
        """Convert a Python integer value to its for-statement-string-interpolation
        representation -- just bare integer, no quotes."""
        if not isinstance(python_value, int):
            raise ValueError(
                f"Expected Python integer value for IntegerConverter but got {type(python_value)}"
            )

        # Guard against "creative" types that pass as int but aren't really ints
        # by recasting to int before stringifying.

        return str(int(python_value))


class DecimalConverter(TypeConverter[Decimal]):
    """Handle fixed precision DECIMAL types, mapping to/from Python's decimal.Decimal"""

    PRIMARY_FLINK_TYPE_NAME = "DECIMAL"

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
    def to_statement_string(cls, python_value: Decimal) -> str:
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


class FloatConverter(TypeConverter[float]):
    """Handles Flink types for FLOAT, DOUBLE to/from Python float"""

    PRIMARY_FLINK_TYPE_NAME = "DOUBLE"

    # Special cases when coming from Flink string representation.
    _transcendental_spellings = {
        "NaN": float("nan"),
        "Infinity": float("inf"),
        "-Infinity": float("-inf"),
    }

    def to_python_value(self, response_value: FromResponseTypes) -> float | None:
        """Expect string-encoded float or None from the response value, return as float
        or raise ValueError."""
        if response_value is None:
            return None

        if not isinstance(response_value, str):
            raise ValueError(
                f"Expected float to be encoded as JSON string but got {type(response_value)}"
            )

        # Must specifically handle the Flink/Java spellings of NaN and infinities.
        if float_repr := self._transcendental_spellings.get(response_value, None):
            return float_repr

        # Not a transcendental, parse as normal float.
        return float(response_value)

    @classmethod
    def to_statement_string(cls, python_value: float) -> str:
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
        # SQL convert-from-string does not (statement will crash at this time, but hopefully
        # fixed soon. Flink does support these if, say, produced by avro Kafka, so ...).
        if isnan(python_value) or isinf(python_value):
            raise ValueError("Cannot convert NaN or Infinity to a Flink SQL float/double literal")

        # Will be interpolated as a literal number in the statement, no quotes.
        return str(python_value)


class BooleanConverter(TypeConverter[bool]):
    """Handles Flink type BOOLEAN to/from Python bool"""

    PRIMARY_FLINK_TYPE_NAME = "BOOLEAN"

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
    def to_statement_string(cls, python_value: bool) -> str:
        """Convert a Python boolean value to its for-statement-string-interpolation
        representation."""
        if not isinstance(python_value, bool):
            raise ValueError(
                f"Expected Python boolean value for BooleanConverter but got {type(python_value)}"
            )
        return "TRUE" if python_value else "FALSE"


class SqlNone:
    """Marker class to indicate a parameter that should be treated as NULL
    of a specific type.

    As of time of writing, Flink SQL does not support bare NULL literals
    in statements. NULL values must be cast to a specific type.
    """

    # Static members for NULLs of common types, initialized at end of module.
    INTEGER: SqlNone
    VARCHAR: SqlNone
    STRING: SqlNone
    BOOLEAN: SqlNone
    DECIMAL: SqlNone
    FLOAT: SqlNone
    DATE: SqlNone
    TIME: SqlNone
    TIMESTAMP: SqlNone
    VARBINARY: SqlNone
    YEAR_MONTH_INTERVAL: SqlNone
    DAY_SECOND_INTERVAL: SqlNone

    def __init__(self, python_or_flink_type: str | type):
        if isinstance(python_or_flink_type, str):
            # The caller provided a Flink type name directly.
            # Ensure is a known type from _flink_type_name_to_converter_map keys (or lowercase
            # thereof)
            python_or_flink_type = python_or_flink_type.upper()
            if (
                python_or_flink_type not in _flink_type_name_to_converter_map
                and not python_or_flink_type.startswith("ARRAY")
                and not python_or_flink_type.startswith("MAP")
            ):
                raise InterfaceError(f"Unknown Flink type name {python_or_flink_type}")

            # Found in the map or is an annotated array type, roll with it as is.
            flink_type_name = python_or_flink_type
        else:
            # Map from Python type to Flink SQL type name
            converter_cls = _python_type_to_type_converter.get(python_or_flink_type)
            if not converter_cls:
                raise InterfaceError(
                    f"Cannot determine Flink SQL type name for Python type {python_or_flink_type}"
                )

            flink_type_name = converter_cls.PRIMARY_FLINK_TYPE_NAME

        self._flink_type_name = flink_type_name

    def __str__(self) -> str:
        return f"cast (null as {self._flink_type_name})"


class NullResultConverter(TypeConverter[NoneType]):
    PRIMARY_FLINK_TYPE_NAME = "NULL"
    """Handles Flink NULL values to Python None. Only handles from
    results -> Python None conversion"""

    def to_python_value(self, response_value: FromResponseTypes) -> None:
        """Expect None from the response value, return None or raise ValueError."""
        if response_value is not None:
            raise ValueError(
                f"Expected None value for NullConverter but got {type(response_value)}"
            )

        return None  # noqa: PLR1711 # explicit return for clarity.

    @classmethod
    def to_statement_string(cls, python_value: NoneType) -> str:
        raise InterfaceError(
            "NullConverter cannot convert Python None to statement string directly. "
            "Use AnnotatedNull to specify the desired SQL type for NULL parameters."
        )


class SqlNoneConverter(TypeConverter[SqlNone]):
    """Handles conversion of SqlNone to SQL NULL of specified type."""

    # Have to say something here, but we're not ever going to be used
    # to go from SQL NULL to Python SqlNone. We're one-way only,
    # the opposite from NullResultConverter.
    PRIMARY_FLINK_TYPE_NAME = ""

    # Since is never used for Flink result -> Python conversion,
    # this class is not registered _flink_type_name_to_converter_map.

    def to_python_value(self, response_value: FromResponseTypes) -> None:
        """Never needed, as SqlNone is only for parameter conversion."""
        raise InterfaceError(
            "SqlNoneConverter cannot convert from response values to Python. "
            "It is only for converting SqlNone parameters to SQL NULL strings."
        )

    @classmethod
    def to_statement_string(cls, python_value: SqlNone) -> str:
        """Convert an SqlNone instance to its for-statement-string-interpolation
        representation."""
        if not isinstance(python_value, SqlNone):
            raise ValueError(
                f"Expected SqlNone value for SqlNoneConverter but got {type(python_value)}"
            )
        # SqlNone's str() includes the cast syntax to its embedded type.
        return str(python_value)


class DateConverter(TypeConverter[date]):
    """Handles Flink DATE type to Python datetime.date"""

    PRIMARY_FLINK_TYPE_NAME = "DATE"

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
    def to_statement_string(cls, python_value: date) -> str:
        """Convert a Python datetime.date value to its for-statement-string-interpolation
        representation, quoted YYYY-MM-DD."""

        if not isinstance(python_value, date):
            raise ValueError(
                f"Expected Python datetime.date value for DateConverter "
                f"but got {type(python_value)}"
            )

        # Our use cases need the prefixed 'DATE' keyword, so include it here.
        return f"DATE '{python_value.isoformat()}'"


class TimeConverter(TypeConverter[time]):
    """Handles Flink TIME type to Python datetime.time"""

    PRIMARY_FLINK_TYPE_NAME = "TIME"

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
    def to_statement_string(cls, python_value: time) -> str:
        """Convert a Python datetime.time value to its for-statement-string-interpolation
        representation, quoted `' TIME HH:MM:SS.MMMMMM.XXXXX'`"""

        if not isinstance(python_value, time):
            raise ValueError(
                f"Expected Python datetime.time value for TimeConverter "
                f"but got {type(python_value)}"
            )

        return f"TIME '{python_value.isoformat(timespec='microseconds')}'"


class TimestampConverter(TypeConverter[datetime]):
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

    PRIMARY_FLINK_TYPE_NAME = "TIMESTAMP"

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
    def to_statement_string(cls, python_value: datetime) -> str:
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


@dataclass
class YearMonthInterval:
    """Class representing a Flink YEAR TO MONTH interval with separate year and month components.

    Negative intervals have negative years and/or months. When the years is negative,
    the months should also be negative, and vice versa (so as to avoid ambiguity and to
    represent negative months-only intervals). The smallest magnitude negative interval is
    therefore 0 years and -1 month. When either years or months is non-positive, both will be,
    and vice versa for positive intervals. Property `is_negative` can be used to check the sign.

    (This differs from Python's timedelta, which represents less than one negative day
    intervals by having negative days and positive seconds/microseconds, which, when
    added together, end up at the right negative point in time (that is, not having
    a zero days component when the total interval is negative but less than one day).)

    The string representation is of the form '+-Y-M', with a leading '+' or '-' sign,
    followed by the absolute value of years, a hyphen, and the absolute value of months
    zero-padded to two digits.
    """

    years: int
    months: int

    def __post_init__(self):
        if not isinstance(self.years, int) or not isinstance(self.months, int):
            raise TypeError("YearMonthInterval years and months must be integers.")

        if (self.years < 0 and self.months > 0) or (self.years > 0 and self.months < 0):
            raise ValueError("YearMonthInterval years and months must have the same sign.")

        if abs(self.months) >= 12:  # noqa: PLR2004
            raise ValueError("YearMonthInterval months must be in the range -11 to 11.")

        if abs(self.years) > 9999:  # noqa: PLR2004
            raise ValueError("YearMonthInterval years must be in the range -9999 to 9999")

    @property
    def is_negative(self) -> bool:
        """Return True if the interval is negative, False otherwise."""
        return self.years < 0 or self.months < 0

    def __str__(self) -> str:
        sign = "-" if (self.years < 0 or self.months < 0) else "+"
        return f"{sign}{abs(self.years)}-{abs(self.months):02d}"

    # Rich comparison methods for vague parity with timedelta
    def __lt__(self, other: Any) -> bool:
        if not isinstance(other, YearMonthInterval):
            return NotImplemented
        return (self.years, self.months) < (other.years, other.months)

    def __le__(self, other: Any) -> bool:
        if not isinstance(other, YearMonthInterval):
            return NotImplemented
        return (self.years, self.months) <= (other.years, other.months)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, YearMonthInterval):
            return NotImplemented
        return self.years == other.years and self.months == other.months

    def __gt__(self, other: Any) -> bool:
        if not isinstance(other, YearMonthInterval):
            return NotImplemented
        return (self.years, self.months) > (other.years, other.months)

    def __ge__(self, other: Any) -> bool:
        if not isinstance(other, YearMonthInterval):
            return NotImplemented
        return (self.years, self.months) >= (other.years, other.months)

    def __ne__(self, other: Any) -> bool:
        if not isinstance(other, YearMonthInterval):
            return NotImplemented
        return self.years != other.years or self.months != other.months

    def __hash__(self) -> int:
        """Hash based on years and months, since overriding __eq__."""
        return hash((self.years, self.months))


class YearMonthIntervalConverter(TypeConverter[YearMonthInterval]):
    """Handles Flink YEAR TO MONTH variant INTERVAL types as strings.

    INTERVAL YEAR TO MONTH is mapped to Python YearMonthInterval dataclass. Its string
    representation is of the form '+-Y-M', and the Flink schema type will be INTERVAL_YEAR_MONTH.
    """

    PRIMARY_FLINK_TYPE_NAME = "INTERVAL_YEAR_MONTH"

    def __init__(self, column_type: ColumnTypeDefinition):
        if column_type.type_name != "INTERVAL_YEAR_MONTH":
            raise ValueError(
                f"YearMonthIntervalConverter can only be used with INTERVAL_YEAR_MONTH types, "
                f"got {column_type.type_name}"
            )
        super().__init__(column_type)

    def to_python_value(self, response_value: FromResponseTypes) -> YearMonthInterval | None:
        """Expect string-encoded interval or None from the response value,
        return as YearMonthInterval or raise ValueError."""

        # Example: '+1-06' for interval of 1 year, 6 months.
        if response_value is None:
            return None
        if not isinstance(response_value, str):
            raise ValueError(
                f"Expected interval to be encoded as JSON string but got {type(response_value)}"
            )

        # Parse the interval string into a YearMonthInterval
        try:
            sign, rest = response_value[0], response_value[1:]
            years_str, months_str = rest.split("-", 1)
            years = int(years_str)
            months = int(months_str)
            if sign == "-":
                years = -years
                months = -months
            return YearMonthInterval(years=years, months=months)
        except Exception as e:
            raise ValueError(
                f"Invalid interval string for YearMonthIntervalConverter: {response_value}"
            ) from e

    @classmethod
    def to_statement_string(cls, python_value: YearMonthInterval) -> str:
        """Convert a Python YearMonthInterval value representing an interval to its
        for-statement-string-interpolation representation."""
        if not isinstance(python_value, YearMonthInterval):
            raise ValueError(
                f"Expected Python YearMonthInterval value for YearMonthIntervalConverter "
                f"but got {type(python_value)}"
            )

        interval_str = str(python_value)
        return f"INTERVAL '{interval_str}' YEAR TO MONTH"


class DaysIntervalConverter(TypeConverter[timedelta]):
    """Handles Flink DAYS TO SECOND variant INTERVAL types as strings.

    INTERVAL DAY TO SECOND is mapped to Python timedelta. Its string representation
    is of the form '+-D HH:MM:SS.MMMMMM', and the Flink schema type will be
    INTERVAL_DAY_TIME.

    We have to take care when converting negative intervals carrying fractional
    seconds, since Python's timedelta normalizes negative timedeltas in a surprising way,
    expressing them with negative days and positive seconds/microseconds.
    """

    PRIMARY_FLINK_TYPE_NAME = "INTERVAL_DAY_TIME"

    _HOURS_TO_SECONDS_RE = re.compile(
        r"^(?P<sign>[+-])(?P<days>\d+)\s(?P<hours>\d{2}):(?P<minutes>\d{2}):(?P<seconds>\d{2})(?:\.(?P<micro>\d{1,6}))?$"
    )

    def to_python_value(self, response_value: FromResponseTypes) -> timedelta | None:
        """Expect string-encoded interval or None from the response value,
        return as str or raise ValueError."""

        # Example: '+0 04:00:00.000' for interval of 0 days, 4 hours.

        if response_value is None:
            return None

        if not isinstance(response_value, str):
            raise ValueError(
                f"Expected interval to be encoded as JSON string but got {type(response_value)}"
            )

        # Parse the interval string into a timedelta
        # Examples:
        #   * '+1 12:30:45.123456' (positive days through to microseconds),
        #   * '-0 00:15:00' (negative 15 minutes, no fractional seconds)
        try:
            m = self._HOURS_TO_SECONDS_RE.match(response_value)
            if not m:
                raise ValueError(f"Invalid interval format: {response_value}")

            days = int(m.group("days"))
            hours = int(m.group("hours"))
            minutes = int(m.group("minutes"))
            seconds = int(m.group("seconds"))

            micro_group = m.group("micro")
            microseconds = int(micro_group.ljust(6, "0")) if micro_group else 0

            # Build a positive timedelta first
            td = timedelta(
                days=days, hours=hours, minutes=minutes, seconds=seconds, microseconds=microseconds
            )

            # Negate if needed.
            if m.group("sign") == "-":
                td = -td

            return td
        except Exception as e:
            raise ValueError(
                f"Invalid interval string for IntervalConverter: {response_value}"
            ) from e

    ZERO_TIMEDELTA = timedelta(0)

    @classmethod
    def to_statement_string(cls, python_value: timedelta) -> str:
        """Convert a Python timedelta value representing an interval to its
        for-statement-string-interpolation representation."""
        if not isinstance(python_value, timedelta):
            raise ValueError(
                f"Expected Python timedelta for IntervalConverter but got {type(python_value)}"
            )

        # If negative, convert to positive and remember sign to avoid negative timedelta
        # normalization quirks (python normalizes to negative days, positive seconds/microseconds
        # which end up representing the right point in timeline when all added together).
        if python_value < cls.ZERO_TIMEDELTA:
            # Make positive for field extraction.
            python_value = -python_value
            sign = "-"
        else:
            sign = "+"

        # Collect integral days, hours, minutes, seconds, microseconds for Flink string
        # representation.
        total_seconds = int(python_value.total_seconds())
        days, remainder = divmod(total_seconds, 86400)
        hours, remainder = divmod(remainder, 3600)
        minutes, seconds = divmod(remainder, 60)
        microseconds = python_value.microseconds

        interval_str = f"{sign}{days} {hours:02}:{minutes:02}:{seconds:02}"
        if microseconds > 0:
            interval_str += f".{microseconds:06}"
            precision = "(6)"
        else:
            precision = ""

        return f"INTERVAL '{interval_str}' DAY TO SECOND{precision}"


class ArrayConverter(TypeConverter[list]):
    """Handles Flink ARRAY type to/from Python list.

    Caveats:
      * Nested lists / arrays are supported, but empty arrays are not (empty array literals
    are not supported by Flink at this time).
      * Nones in the list are supported, and will be converted to SQL NULLs of the
    appropriate element type, however a list of all Nones is not supported since
    the element type cannot be determined in that case.
    """

    PRIMARY_FLINK_TYPE_NAME = "ARRAY"

    _element_converter: TypeConverter
    """Type converter for array element type."""

    def __init__(self, column_type: ColumnTypeDefinition):
        if not column_type.type_name == "ARRAY":
            raise InterfaceError(
                f"ArrayConverter can only be used with ARRAY types, got {column_type.type_name}"
            )

        # Determine the element type's converter from the column_type's type parameters.
        element_type_def = column_type.element_type
        if not element_type_def:
            raise InterfaceError(
                "ArrayConverter cannot determine element type from column type definition."
            )

        element_converter_cls = _flink_type_name_to_converter_map.get(element_type_def.type_name)
        if not element_converter_cls:
            raise TypeError(
                f"Conversion for array element of type {element_type_def.type_name} is not"
                " implemented."
            )

        self._element_converter = element_converter_cls(element_type_def)

        super().__init__(column_type)

    def to_python_value(self, response_value: FromResponseTypes) -> list | None:
        """Expect list or None from the response value, return as list or raise ValueError."""
        if response_value is None:
            return None

        if not isinstance(response_value, list):
            raise ValueError(
                f"Expected list value for ArrayConverter but got {type(response_value)}"
            )

        response_value_converted = []
        for element in response_value:
            converted_element = self._element_converter.to_python_value(element)
            response_value_converted.append(converted_element)

        return response_value_converted

    @classmethod
    def to_statement_string(cls, python_value: list) -> str:
        """Convert a Python list value to its for-statement-string-interpolation
        representation."""
        if not isinstance(python_value, list):
            raise ValueError(f"Expected list value for ArrayConverter but got {type(python_value)}")

        if len(python_value) == 0:
            # Empty array, it seems that Flink does not support literal empty arrays grr boo hoo.
            # (as well as would make it hard for us to determine element type anyway to spell the
            #  element type in an empty ARRAY<element_type> literal).
            raise ValueError("Cannot convert empty list to Flink ARRAY literal.")

        # Convert each element to its string representation
        element_converter_cls = determine_element_converter_cls(python_value)
        none_element_str = SqlNone(element_converter_cls.PRIMARY_FLINK_TYPE_NAME).__str__()

        element_strings = []

        for element in python_value:
            # May raise ValueError if individual element is of wrong type.
            if element is not None:
                element_str = element_converter_cls.to_statement_string(element)
            else:
                element_str = none_element_str

            element_strings.append(element_str)

        # Join elements with commas and wrap in ARRAY[...]
        return f"ARRAY[{', '.join(element_strings)}]"


class MapConverter(TypeConverter[dict]):
    """Handles Flink MAP type to/from Python dict.

    Caveats:
    * Empty python dicts are not supported since Flink does not support literal empty maps at this
      time.
    * Flink Map keys must be of a type that is hashable in Python.
    * Python dict keys and values may be None, which will be converted to SQL NULLs of the
      appropriate types, however a map with all keys or all values as None is not supported since
      the key/value types cannot be determined in that case.
    * Python dict keys and values must be of uniform type (or None), since Flink MAP types
      require uniform key and value types.
    """

    PRIMARY_FLINK_TYPE_NAME = "MAP"

    key_converter: TypeConverter
    """Type converter for map key type."""
    value_converter: TypeConverter
    """Type converter for map value type."""

    def __init__(self, column_type: ColumnTypeDefinition):
        if not column_type.type_name == "MAP":
            raise InterfaceError(
                f"MapConverter can only be used with MAP types, got {column_type.type_name}"
            )

        # Determine the key and value type's converters from the column_type's key and value
        # type parameters.
        key_type_def = column_type.key_type
        value_type_def = column_type.value_type
        if not key_type_def:
            raise InterfaceError(
                "MapConverter cannot determine key type from column type definition."
            )
        if not value_type_def:
            raise InterfaceError(
                "MapConverter cannot determine value type from column type definition."
            )

        key_converter_cls = _flink_type_name_to_converter_map.get(key_type_def.type_name)
        if not key_converter_cls:
            raise TypeError(
                f"Conversion for map key of type {key_type_def.type_name} is not implemented."
            )

        self.key_converter = key_converter_cls(key_type_def)

        value_converter_cls = _flink_type_name_to_converter_map.get(value_type_def.type_name)
        if not value_converter_cls:
            raise TypeError(
                f"Conversion for map value of type {value_type_def.type_name} is not implemented."
            )
        self.value_converter = value_converter_cls(value_type_def)

        super().__init__(column_type)

    def to_python_value(self, response_value: FromResponseTypes) -> dict | None:
        """Expect dict or None from the response value, return as dict or raise ValueError."""
        if response_value is None:
            return None

        if not isinstance(response_value, list):
            raise TypeError(f"Expected list value for MapConverter but got {type(response_value)}")

        # Will be a list of pair lists: [[enc-key1, enc-value1], [enc-key2, enc-value2], ...]
        # where keys and values will be the from-response encodings for their
        # types. Use the decoders for the key and value types for each pair.

        result_dict = {}
        for pair in response_value:
            if not isinstance(pair, list) or len(pair) != 2:  # noqa: PLR2004
                raise ValueError(
                    f"Expected key-value pair list of length 2 for MapConverter but got: {pair}"
                )

            # Promote this key/value pair from from-response encodings to Python values.
            key = self.key_converter.to_python_value(pair[0])
            value = self.value_converter.to_python_value(pair[1])

            result_dict[key] = value

        return result_dict

    @classmethod
    def to_statement_string(cls, python_value: dict) -> str:
        """Convert a Python dict value to its for-statement-string-interpolation
        representation."""

        # Example: MAP['key1', 12, 'key2', 22] for a map with string keys and integer values.
        if not isinstance(python_value, dict):
            raise TypeError(f"Expected dict value for MapConverter but got {type(python_value)}")

        if len(python_value) == 0:
            # Empty map, it seems that Flink does not support literal empty maps grr boo hoo.
            raise ValueError("Cannot convert empty dict to Flink MAP literal.")

        # Find the converter classes for keys and values
        key_converter_cls = determine_element_converter_cls(python_value.keys())
        value_converter_cls = determine_element_converter_cls(python_value.values())

        none_key_str = SqlNone(key_converter_cls.PRIMARY_FLINK_TYPE_NAME).__str__()
        none_value_str = SqlNone(value_converter_cls.PRIMARY_FLINK_TYPE_NAME).__str__()

        # Convert each key-value pair to its string representation, append each
        # to list to join later.
        keys_and_values: list[str] = []

        for key, value in python_value.items():
            # May raise ValueError if individual key or value is of wrong type.
            if key is not None:
                key_str = key_converter_cls.to_statement_string(key)
            else:
                key_str = none_key_str

            keys_and_values.append(key_str)

            if value is not None:
                value_str = value_converter_cls.to_statement_string(value)
            else:
                value_str = none_value_str

            keys_and_values.append(value_str)

        # Join key-value pairs with commas and wrap in MAP[...]
        return f"MAP[{', '.join(keys_and_values)}]"


_flink_type_name_to_converter_map: dict[str, type[TypeConverter]] = {
    # Null type
    "NULL": NullResultConverter,
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
    # Interval types
    "INTERVAL_DAY_TIME": DaysIntervalConverter,
    "INTERVAL DAYS TO SECOND": DaysIntervalConverter,
    "INTERVAL_YEAR_MONTH": YearMonthIntervalConverter,
    "INTERVAL YEAR TO MONTH": YearMonthIntervalConverter,
    # String types
    "CHAR": StringConverter,
    "VARCHAR": StringConverter,
    "STRING": StringConverter,
    # Binary types
    "VARBINARY": VarBinaryConverter,
    "BINARY": VarBinaryConverter,
    "BYTES": VarBinaryConverter,
    # Array type
    "ARRAY": ArrayConverter,
    # Map type
    "MAP": MapConverter,
}


_python_type_to_type_converter: dict[type, type[TypeConverter]] = {
    None.__class__: NullResultConverter,
    SqlNone: SqlNoneConverter,
    bool: BooleanConverter,
    int: IntegerConverter,
    Decimal: DecimalConverter,
    float: FloatConverter,
    date: DateConverter,
    time: TimeConverter,
    str: StringConverter,
    bytes: VarBinaryConverter,
    datetime: TimestampConverter,
    YearMonthInterval: YearMonthIntervalConverter,
    timedelta: DaysIntervalConverter,
    list: ArrayConverter,
    dict: MapConverter,
}

# Initialize static SqlNone members for common types, must be done after class definition
# and after the global type maps are defined.
SqlNone.INTEGER = SqlNone("INTEGER")
SqlNone.VARCHAR = SqlNone("VARCHAR")
SqlNone.STRING = SqlNone("STRING")
SqlNone.VARBINARY = SqlNone("VARBINARY")
SqlNone.BOOLEAN = SqlNone("BOOLEAN")
SqlNone.DECIMAL = SqlNone("DECIMAL")
SqlNone.FLOAT = SqlNone("FLOAT")
SqlNone.DATE = SqlNone("DATE")
SqlNone.TIME = SqlNone("TIME")
SqlNone.TIMESTAMP = SqlNone("TIMESTAMP")
SqlNone.YEAR_MONTH_INTERVAL = SqlNone("INTERVAL YEAR TO MONTH")
SqlNone.DAY_SECOND_INTERVAL = SqlNone("INTERVAL DAYS TO SECOND")


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


def determine_element_converter_cls(python_value: Iterable) -> type[TypeConverter]:
    """Determine the TypeConverter class for the elements of the given Python sequence.

    Assumes the list is non-empty and that all elements are of the same type, or
    contains None elements. Cannot be all None. The list will already have
    been proven to be non-empty by the caller.

    Returns: The TypeConverter class for the type of the first non-None element.

    Raises: ValueError if the element type is not supported.
    """
    element = None
    for element in python_value:
        if element is not None:
            break

    if element is None:
        raise ValueError("Cannot determine element type: all elements are None.")

    converter_cls = _python_type_to_type_converter.get(type(element))
    if not converter_cls:
        raise TypeError(f"Conversion for array element of type {type(element)} is not implemented.")

    return converter_cls
