"""Type conversions between Flink statement API string serializations and python representations."""

from __future__ import annotations

import logging
import re
from collections import Counter
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, fields, is_dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from math import isinf, isnan
from types import NoneType
from typing import TYPE_CHECKING, Any, ClassVar, Generic, Protocol, TypeAlias, TypeVar

from confluent_sql.exceptions import InterfaceError, TypeMismatchError

if TYPE_CHECKING:
    from .connection import Connection

logger = logging.getLogger(__name__)


PyType = TypeVar("PyType")
"""The data type of the Python value being converted to/from Flink SQL representation by
a TypeConverter subclass."""
ResponseType = TypeVar("ResponseType")
"""The data type of the from-response-API-JSON-encoded value being converted from
   in to_python_value()."""


if TYPE_CHECKING:
    from .statement import Schema

__all__ = [
    "ColumnTypeDefinition",
    "StrAnyDict",
    "StatementTypeConverter",
    "TypeConverter",
    "convert_statement_parameters",
    "SqlNone",
    "YearMonthInterval",
    "TypeMismatchError",
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

StrAnyDict: TypeAlias = dict[str, Any]


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
        column_type = ColumnTypeDefinition.from_response(data["field_type"])
        return cls(name=data["name"], field_type=column_type, description=data.get("description"))


@dataclass(kw_only=True)
class ColumnTypeDefinition:
    """Fields corresponding to statement.traits.schema.columns[].type members.

    Describes the Flink-side type definition of a projected column.
    """

    type: str
    """Flink name of the type, e.g., "INT", "STRING", "ROW", etc."""
    nullable: bool
    length: int | None = None
    precision: int | None = None
    scale: int | None = None
    fractional_precision: int | None = None  # if an interval type
    resolution: str | None = None  # if an interval type
    key_type: ColumnTypeDefinition | None = None  # if type == "MAP"
    value_type: ColumnTypeDefinition | None = None  # if type == "MAP"
    element_type: ColumnTypeDefinition | None = None  # if type == "ARRAY" or "MULTISET"

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

        element_type = key_type = value_type = None

        column_type = data["type"]

        if column_type in {"ARRAY", "MULTISET"}:
            element_type = data.get("element_type")
            if element_type is not None:
                # Describes the element type of an ARRAY or a MULTISET.
                # Promote from element type dict to a ColumnTypeDefinition
                element_type = cls.from_response(data["element_type"])

        elif column_type == "MAP":
            # For MAP types, we need to parse key_type and value_type specially.
            key_type = cls.from_response(data["key_type"])
            value_type = cls.from_response(data["value_type"])

        return cls(
            type=column_type,
            nullable=data["nullable"],
            length=data.get("length"),
            precision=data.get("precision"),
            scale=data.get("scale"),
            fractional_precision=data.get("fractional_precision"),
            resolution=data.get("resolution"),
            key_type=key_type,
            value_type=value_type,
            element_type=element_type,
            fields=[RowColumn.from_response(field) for field in data.get("fields", [])]
            if "fields" in data
            else None,
            class_name=data.get("class_name"),
        )


class StatementTypeConverter:
    """
    Acts on behalf of a statement's Schema to convert from-API-JSON-changelog values to Python,
    values. Drives per-column TypeConverter deserialization to python types based on the schema.
    """

    _schema: Schema
    _type_converters: list[TypeConverter]

    def __init__(self, connection: Connection, schema: Schema):
        self._schema = schema
        self._type_converters = [
            get_api_type_converter(connection, col.type) for col in schema.columns
        ]

    def to_python_row(self, sql_row: list[FromResponseTypes]) -> tuple[Any]:
        """Convert a SQL row (list of from-results-API encoded values) to a Python row
        (tuple of Python values) to be returned by a Cursor."""
        return tuple(
            converter.to_python_value(sql_value)  # type: ignore[arg-type]
            for converter, sql_value in zip(self._type_converters, sql_row, strict=True)
        )


class TypeConverter(Generic[PyType, ResponseType]):
    """Base class for all Flink <-> Python data type converters.

    A TypeConverter handles conversion between a specific Flink SQL type's
    representation in the statement API JSON responses and the corresponding
    Python type.

    Conversion from Flink SQL type to Python type is handled by the instance method
    `to_python_value()`, which takes a from-response-API-JSON-encoded value and returns
    the corresponding Python value, and may be hinted by the ColumnTypeDefinition
    further clarifying the Flink-side type provided at construction time (from
    the statement's schema).

    Generic parameter PyType indicates the Python type handled by this converter --
    the return type of to_python_value() (in addition to None, for nullable
    columns) and the parameter type of to_statement_string().

    Generic parameter ResponseType indicates the from-response-API-JSON-encoded type
    handled by this converter -- the parameter type of to_python_value() (in addition
    to None, for nullable columns).
    """

    PRIMARY_FLINK_TYPE_NAME: str
    """The primary Flink SQL type name that this TypeConverter handles."""

    _column_type: ColumnTypeDefinition

    def __init__(self, connection: Connection, column_type: ColumnTypeDefinition):
        self._connection = connection
        self._column_type = column_type

    def to_python_value(self, response_value: ResponseType | None) -> PyType | None:
        """Convert from statement-response-API-JSON representation to its Python value.

        All columns might also be nullable, in which case None should be returned.
        """
        raise NotImplementedError("Subclasses should implement this method.")  # pragma: no cover

    @classmethod
    def to_statement_string(cls, python_value: PyType) -> str:
        """Convert from Python value to its for-statement-string-interpolation representation."""
        raise NotImplementedError("Subclasses should implement this method.")  # pragma: no cover

    def _check_to_python_param_type(
        self,
        expected_type: type[ResponseType],
        value: Any,
    ) -> None:
        """Raises TypeMismatchError if the value is not of the expected from-response-API type."""
        if not isinstance(value, expected_type):
            raise TypeMismatchError(
                converter_name=self.__class__.__name__,
                method_name="to_python_value",
                expected_type=expected_type.__name__,
                bad_value=value,
            )

    @classmethod
    def _check_to_statement_string_param_type(
        cls,
        expected_type: type,
        value: Any,
    ) -> None:
        """Raises TypeMismatchError if the value is not of the expected Python type."""
        if not isinstance(value, expected_type):
            raise TypeMismatchError(
                converter_name=cls.__name__,
                method_name="to_statement_string",
                expected_type=expected_type.__name__,
                bad_value=value,
            )


def get_api_type_converter(
    connection: Connection, column_type: ColumnTypeDefinition
) -> TypeConverter:
    """Return the appropriate TypeConverter for a given from-Statement-JSON type description."""
    # Find the appropriate converter class mapped from the Flink type name
    cls = _flink_type_name_to_converter_map.get(column_type.type_name)
    if not cls:
        # Another type mapping needed!
        raise NotImplementedError(f"TypeConverter for {column_type.type_name} is not implemented.")

    return cls(connection, column_type)


class StringConverter(TypeConverter[str, str]):
    """Handles Flink types for CHAR, VARCHAR, STRING"""

    PRIMARY_FLINK_TYPE_NAME = "STRING"

    def to_python_value(self, response_value: str | None) -> str | None:
        """Expect string or None from the response value, return as-is or
        raise TypeMismatchError."""
        if response_value is None:
            return None

        self._check_to_python_param_type(str, response_value)

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

        cls._check_to_statement_string_param_type(str, python_value)

        # Ensure we're dealing with a standard str here, and not a subclass
        # that might do something "creative" when we do string operations on it.
        python_value = str(python_value)

        # Escape single quotes by doubling them
        escaped_value = python_value.replace("'", "''")

        # Return wrapped in single quotes
        return f"'{escaped_value}'"


class VarBinaryConverter(TypeConverter[bytes, str]):
    """Handles Flink type VARBINARY"""

    PRIMARY_FLINK_TYPE_NAME = "VARBINARY"

    def to_python_value(self, response_value: str | None) -> bytes | None:
        """Expect hex-pair encoded string or None from the response value, return as bytes
        or raise ValueError.

        Examples: "x'7f0203'" <-> b"\x7f\x02\x03"
        """
        if response_value is None:
            return None

        self._check_to_python_param_type(str, response_value)

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
        cls._check_to_statement_string_param_type(bytes, python_value)

        hex_string = python_value.hex()
        return f"x'{hex_string}'"


class IntegerConverter(TypeConverter[int, str]):
    """Handles Flink types for TINYINT, SMALLINT, INTEGER, BIGINT to/from Python int"""

    PRIMARY_FLINK_TYPE_NAME = "INTEGER"

    def to_python_value(self, response_value: str | None) -> int | None:
        """Expect string-encoded integer or None from the response value, return as int
        or raise ValueError."""
        if response_value is None:
            return None

        self._check_to_python_param_type(str, response_value)

        return int(response_value)

    @classmethod
    def to_statement_string(cls, python_value: int) -> str:
        """Convert a Python integer value to its for-statement-string-interpolation
        representation -- just bare integer, no quotes."""
        cls._check_to_statement_string_param_type(int, python_value)

        # Guard against "creative" types that pass as int but aren't really ints
        # by recasting to int before stringifying.

        return str(int(python_value))


class DecimalConverter(TypeConverter[Decimal, str]):
    """Handle fixed precision DECIMAL types, mapping to/from Python's decimal.Decimal"""

    PRIMARY_FLINK_TYPE_NAME = "DECIMAL"

    def to_python_value(self, response_value: str | None) -> Decimal | None:
        """Expect string-encoded decimal or None from the response value, return as str
        or raise ValueError."""
        if response_value is None:
            return None

        self._check_to_python_param_type(str, response_value)

        return Decimal(response_value)

    @classmethod
    def to_statement_string(cls, python_value: Decimal) -> str:
        """Convert a Python Decimal value to its for-statement-string-interpolation
        representation."""

        cls._check_to_statement_string_param_type(Decimal, python_value)

        # Must include explicit cast to DECIMAL to avoid Flink interpreting
        # the literal as a DOUBLE.

        # Must include precision and scale in the cast to get any decimal
        # value with fractional part honored, otherwise Flink will
        # truncate to integer.
        precision = len(python_value.as_tuple().digits)  # type: ignore[attr-defined]
        scale = -python_value.as_tuple().exponent  # type: ignore[attr-defined]

        return f"cast('{python_value}' as decimal({precision},{scale}))"


class FloatConverter(TypeConverter[float, str]):
    """Handles Flink types for FLOAT, DOUBLE to/from Python float"""

    PRIMARY_FLINK_TYPE_NAME = "DOUBLE"

    # Special cases when coming from Flink string representation.
    _transcendental_spellings = {
        "NaN": float("nan"),
        "Infinity": float("inf"),
        "-Infinity": float("-inf"),
    }

    def to_python_value(self, response_value: str | None) -> float | None:
        """Expect string-encoded float or None from the response value, return as float
        or raise ValueError."""
        if response_value is None:
            return None

        self._check_to_python_param_type(str, response_value)

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
        cls._check_to_statement_string_param_type(float, python_value)

        # Check for NaN or Infinity, IEEEE 754 float representation allows these values, but Flink
        # SQL convert-from-string does not (statement will crash at this time, but hopefully
        # fixed soon. Flink does support these if, say, produced by avro Kafka, so ...).
        if isnan(python_value) or isinf(python_value):
            raise ValueError("Cannot convert NaN or Infinity to a Flink SQL float/double literal")

        # Will be interpolated as a literal number in the statement, no quotes.
        return str(python_value)


class BooleanConverter(TypeConverter[bool, str]):
    """Handles Flink type BOOLEAN to/from Python bool"""

    PRIMARY_FLINK_TYPE_NAME = "BOOLEAN"

    def to_python_value(self, response_value: str | None) -> bool | None:
        """Expect string 'TRUE'/'FALSE' or None from the response value, return as bool
        or raise ValueError."""
        if response_value is None:
            return None

        self._check_to_python_param_type(str, response_value)

        return response_value.lower() == "true"

    @classmethod
    def to_statement_string(cls, python_value: bool) -> str:
        """Convert a Python boolean value to its for-statement-string-interpolation
        representation."""
        cls._check_to_statement_string_param_type(bool, python_value)
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

    _known_types_regex: re.Pattern | None = None
    """Compiled regex pattern for known Flink type names, for validation."""
    # (Initialized on first use based on _flink_type_name_to_converter_map keys.)

    _parameterized_type_regex = re.compile(r"^(?:ARRAY|MAP|MULTISET|ROW)\b", re.IGNORECASE)
    """Compiled regex pattern for parameterized Flink type names."""

    def __init__(self, python_or_flink_type: str | type):
        if isinstance(python_or_flink_type, str):
            # The caller provided a Flink type name directly.
            # Validate the provided Flink type name using case-insensitive regexes.

            if SqlNone._known_types_regex is None:
                # Initialize the known types pattern on first use based on
                # the registered type converter keys.
                SqlNone._known_types_regex = re.compile(
                    r"^(?:"
                    + "|".join(re.escape(t) for t in _flink_type_name_to_converter_map)
                    + r")$",
                    re.IGNORECASE,
                )

            if not (
                SqlNone._known_types_regex.match(python_or_flink_type)
                or SqlNone._parameterized_type_regex.match(python_or_flink_type)
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


class NullResultConverter(TypeConverter[NoneType, NoneType]):
    PRIMARY_FLINK_TYPE_NAME = "NULL"
    """Handles Flink NULL values to Python None. Only handles from
    results -> Python None conversion"""

    def to_python_value(self, response_value: NoneType) -> None:
        """Expect None from the response value, return None or raise ValueError."""
        self._check_to_python_param_type(NoneType, response_value)

        return None  # noqa: PLR1711 # explicit return for clarity.

    @classmethod
    def to_statement_string(cls, python_value: NoneType) -> str:
        raise InterfaceError(
            "NullConverter cannot convert Python None to statement string directly. "
            "Use AnnotatedNull to specify the desired SQL type for NULL parameters."
        )


class SqlNoneConverter(TypeConverter[SqlNone, NoneType]):
    """Handles conversion of SqlNone to SQL NULL of specified type."""

    # Have to say something here, but we're not ever going to be used
    # to go from SQL NULL to Python SqlNone. We're one-way only,
    # the opposite from NullResultConverter.
    PRIMARY_FLINK_TYPE_NAME = ""

    # Since is never used for Flink result -> Python conversion,
    # this class is not registered _flink_type_name_to_converter_map.

    def to_python_value(self, response_value: NoneType) -> None:
        """Never needed, as SqlNone is only for parameter conversion."""
        raise InterfaceError(
            "SqlNoneConverter cannot convert from response values to Python. "
            "It is only for converting SqlNone parameters to SQL NULL strings."
        )

    @classmethod
    def to_statement_string(cls, python_value: SqlNone) -> str:
        """Convert an SqlNone instance to its for-statement-string-interpolation
        representation."""
        cls._check_to_statement_string_param_type(SqlNone, python_value)
        # SqlNone's str() includes the cast syntax to its embedded type.
        return str(python_value)


class DateConverter(TypeConverter[date, str]):
    """Handles Flink DATE type to Python datetime.date"""

    PRIMARY_FLINK_TYPE_NAME = "DATE"

    def to_python_value(self, response_value: str | None) -> date | None:
        """Expect string-encoded date in 'YYYY-MM-DD' format or None from the response value,
        return as datetime.date or raise ValueError."""
        if response_value is None:
            return None

        self._check_to_python_param_type(str, response_value)

        try:
            date = datetime.fromisoformat(response_value).date()
            return date
        except Exception as e:
            raise ValueError(f"Invalid date string for DateConverter: {response_value}") from e

    @classmethod
    def to_statement_string(cls, python_value: date) -> str:
        """Convert a Python datetime.date value to its for-statement-string-interpolation
        representation, quoted YYYY-MM-DD."""

        cls._check_to_statement_string_param_type(date, python_value)

        # Our use cases need the prefixed 'DATE' keyword, so include it here.
        return f"DATE '{python_value.isoformat()}'"


class TimeConverter(TypeConverter[time, str]):
    """Handles Flink TIME type to Python datetime.time"""

    PRIMARY_FLINK_TYPE_NAME = "TIME"

    def to_python_value(self, response_value: str | None) -> time | None:
        """Expect string-encoded time in 'HH:MM:SS(.MMMMMM)' format or None from the response value,
        return as datetime.time or raise ValueError."""
        if response_value is None:
            return None

        self._check_to_python_param_type(str, response_value)

        try:
            return time.fromisoformat(response_value)
        except Exception as e:
            raise ValueError(f"Invalid time string for TimeConverter: {response_value}") from e

    @classmethod
    def to_statement_string(cls, python_value: time) -> str:
        """Convert a Python datetime.time value to its for-statement-string-interpolation
        representation, quoted `' TIME HH:MM:SS.MMMMMM.XXXXX'`"""

        cls._check_to_statement_string_param_type(time, python_value)

        return f"TIME '{python_value.isoformat(timespec='microseconds')}'"


class TimestampConverter(TypeConverter[datetime, str]):
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

    def __init__(self, connection: Connection, column_type: ColumnTypeDefinition):
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
        super().__init__(connection, column_type)

    @classmethod
    def to_statement_string(cls, python_value: datetime) -> str:
        """Convert a Python datetime.datetime value to its for-statement-string-interpolation
        representation, based on whether it has tzinfo or not."""

        cls._check_to_statement_string_param_type(datetime, python_value)

        # If has tzinfo, convert to UTC time w/o tzinfo for Flink TIMESTAMP_LTZ
        if python_value.tzinfo is not None:
            python_value = python_value.astimezone(tz=timezone.utc).replace(tzinfo=None)
            # Must explicitly cast in the string forms ...
            flink_type = "timestamp_ltz"
        else:
            flink_type = "timestamp"

        iso_str = python_value.isoformat(sep=" ", timespec="microseconds")
        return f"cast('{iso_str}' as {flink_type})"

    def to_python_value(self, response_value: str | None) -> datetime | None:
        """Expect string-encoded timestamp in 'YYYY-MM-DD HH:MM:SS(.MMMMMM)' format
        or None from the response value, return as datetime.datetime or raise ValueError.

        If the column type is TIMESTAMP_LTZ, the returned datetime will have tzinfo=UTC,
        otherwise it will be tz-naive.
        """

        if response_value is None:
            return None

        self._check_to_python_param_type(str, response_value)

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


class YearMonthIntervalConverter(TypeConverter[YearMonthInterval, str]):
    """Handles Flink YEAR TO MONTH variant INTERVAL types as strings.

    INTERVAL YEAR TO MONTH is mapped to Python YearMonthInterval dataclass. Its string
    representation is of the form '+-Y-M', and the Flink schema type will be INTERVAL_YEAR_MONTH.
    """

    PRIMARY_FLINK_TYPE_NAME = "INTERVAL_YEAR_MONTH"

    def __init__(self, connection: Connection, column_type: ColumnTypeDefinition):
        if column_type.type_name != "INTERVAL_YEAR_MONTH":
            raise ValueError(
                f"YearMonthIntervalConverter can only be used with INTERVAL_YEAR_MONTH types, "
                f"got {column_type.type_name}"
            )
        super().__init__(connection, column_type)

    def to_python_value(self, response_value: str | None) -> YearMonthInterval | None:
        """Expect string-encoded interval or None from the response value,
        return as YearMonthInterval or raise ValueError."""

        # Example: '+1-06' for interval of 1 year, 6 months.
        if response_value is None:
            return None

        self._check_to_python_param_type(str, response_value)

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
        cls._check_to_statement_string_param_type(YearMonthInterval, python_value)

        interval_str = str(python_value)
        return f"INTERVAL '{interval_str}' YEAR TO MONTH"


class DaysIntervalConverter(TypeConverter[timedelta, str]):
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

    def to_python_value(self, response_value: str | None) -> timedelta | None:
        """Expect string-encoded interval or None from the response value,
        return as str or raise ValueError."""

        # Example: '+0 04:00:00.000' for interval of 0 days, 4 hours.

        if response_value is None:
            return None

        self._check_to_python_param_type(str, response_value)

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
        cls._check_to_statement_string_param_type(timedelta, python_value)

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


class ArrayConverter(TypeConverter[list, list]):
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

    def __init__(self, connection: Connection, column_type: ColumnTypeDefinition):
        if column_type.type_name != "ARRAY":
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

        self._element_converter = element_converter_cls(connection, element_type_def)

        super().__init__(connection, column_type)

    def to_python_value(self, response_value: list | None) -> list | None:
        """Expect list or None from the response value, return as list or raise ValueError."""
        if response_value is None:
            return None

        self._check_to_python_param_type(list, response_value)

        response_value_converted = []
        for element in response_value:
            converted_element = self._element_converter.to_python_value(element)
            response_value_converted.append(converted_element)

        return response_value_converted

    @classmethod
    def to_statement_string(cls, python_value: list) -> str:
        """Convert a Python list value to its for-statement-string-interpolation
        representation."""
        cls._check_to_statement_string_param_type(list, python_value)

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


class MapConverter(TypeConverter[dict, list]):
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

    def __init__(self, connection: Connection, column_type: ColumnTypeDefinition):
        if column_type.type_name != "MAP":
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

        self.key_converter = key_converter_cls(connection, key_type_def)

        value_converter_cls = _flink_type_name_to_converter_map.get(value_type_def.type_name)
        if not value_converter_cls:
            raise TypeError(
                f"Conversion for map value of type {value_type_def.type_name} is not implemented."
            )
        self.value_converter = value_converter_cls(connection, value_type_def)

        super().__init__(connection, column_type)

    def to_python_value(self, response_value: list | None) -> dict | None:
        """Expect dict or None from the response value, return as dict or raise ValueError."""
        if response_value is None:
            return None

        self._check_to_python_param_type(list, response_value)

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

        cls._check_to_statement_string_param_type(dict, python_value)

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


class MultisetConverter(TypeConverter[Counter, list]):
    """Handles Flink MULTISET type to/from Python collections.Counter.

    A MULTISET is like a MAP from element to count, where the count is an integer
    representing the number of occurrences of the element in the multiset.
    This is mapped to Python's collections.Counter class.

    The Counter must not be empty, since we need at least one non-None key element
    to determine the key type for conversion.
    """

    PRIMARY_FLINK_TYPE_NAME = "MULTISET"

    element_converter: TypeConverter
    """Type converter for the multiset's element / key type."""

    int_converter: IntegerConverter
    """Integer converter for the counts portion of the multiset."""

    def __init__(self, connection: Connection, column_type: ColumnTypeDefinition):
        if column_type.type_name != "MULTISET":
            raise InterfaceError(
                f"MultisetConverter can only be used with MULTISET types, got {column_type.type_name}"  # noqa: E501
            )

        # Determine the element type's converter from the column_type's type parameters.
        element_type_def = column_type.element_type
        if not element_type_def:
            raise InterfaceError(
                "MultisetConverter cannot determine element type from column type definition."
            )

        element_converter_cls = _flink_type_name_to_converter_map.get(element_type_def.type_name)
        if not element_converter_cls:
            raise TypeError(
                f"Conversion for multiset element of type {element_type_def.type_name} is not implemented."  # noqa: E501
            )

        self.element_converter = element_converter_cls(connection, element_type_def)

        # Always use IntegerConverter for the corresponding counts.
        self.int_converter = IntegerConverter(
            connection, ColumnTypeDefinition(type="INTEGER", nullable=False)
        )

        super().__init__(connection, column_type)

    def to_python_value(self, response_value: list | None) -> Counter | None:
        """Expect list of [element, count] pairs or None from the response value,
        return as Counter or raise ValueError."""
        if response_value is None:
            return None

        self._check_to_python_param_type(list, response_value)

        result_counter: Counter = Counter()
        for pair in response_value:
            if not isinstance(pair, list):
                raise InterfaceError(
                    f"Expected to receive value+count list for MultisetConverter, but got {type(pair)} instead."  # noqa: E501
                )
            try:
                left, right = pair
            except Exception as e:
                raise InterfaceError(
                    f"Expected element + count pair list for MultisetConverter but got: {pair}"
                ) from e

            element = self.element_converter.to_python_value(left)
            if element is None:
                raise InterfaceError("Expected element for MultisetConverter but got None")

            count = self.int_converter.to_python_value(right)
            if count is None:
                raise InterfaceError("Expected integer count for MultisetConverter but got None")

            result_counter[element] = count

        return result_counter

    @classmethod
    def to_statement_string(cls, python_value: Counter) -> str:
        """Flink does not currently support any literal MULTISET syntax."""
        raise InterfaceError("Flink does not currently support MULTISET literals.")


class IsDataclass(Protocol):
    """Protocol describing @dataclass instances, surprisingly enough there is no built-in one."""

    __dataclass_fields__: ClassVar[dict[str, Any]]


RowPythonTypes = tuple | IsDataclass
"""The types that can be used to represent Flink ROW column values in Python:
either tuple (including namedtuple() and typing.NamedTuple) or @dataclass instances."""


class RowConverter(TypeConverter[RowPythonTypes, list]):
    """Convert Flink ROW type to/from Python tuple or namedtuple instances.

    When converting from Flink ROW type, a namedtuple instance is returned,
    with field names corresponding to the ROW's field names. The namedtuple
    class is cached globally based on the field names, so that multiple
    ROWs with the same field names share the same namedtuple class (even across
    multiple RowConverter instances / separate queries or cursors).

    When interpolating python tuples or namedtuples into statements strings,
    the values are converted positionally field by field, and the resulting string is
    of the form "ROW(field1_value, field2_value, ...)".
    """

    PRIMARY_FLINK_TYPE_NAME = "ROW"

    _field_converters: list[TypeConverter]
    """List of TypeConverter instances for each field in the row, in order."""
    _field_names: list[str]
    """List of field names in the row, in order."""
    _python_value_class: type[RowPythonTypes]
    """The namedtuple or @dataclass class from the connection's row class registy
       corresponding to this row type's field names."""

    def __init__(self, connection: Connection, column_type: ColumnTypeDefinition):
        if column_type.type_name != "ROW":
            raise InterfaceError(
                f"RowConverter can only be used with ROW types, got {column_type.type_name}"
            )

        if not column_type.fields:
            raise InterfaceError("RowConverter requires column type definition with fields")

        self._field_converters = []
        self._field_names = []

        for field_def in column_type.fields:
            field_name = field_def.name
            self._field_names.append(field_name)

            field_type_def = field_def.type
            if not field_type_def:
                raise InterfaceError(
                    f"RowConverter cannot determine type for field '{field_name}'."
                )

            field_converter_cls = _flink_type_name_to_converter_map.get(field_type_def.type_name)
            if not field_converter_cls:
                raise TypeError(
                    f"Conversion for row field '{field_name}' of type "
                    f"{field_type_def.type_name} is not implemented."
                )

            field_converter = field_converter_cls(connection, field_type_def)
            self._field_converters.append(field_converter)

        # Get or create the class for this row type's field names.
        self._python_value_class = connection._row_type_registry.get_row_class(self._field_names)

        super().__init__(connection, column_type)

    def to_python_value(self, response_value: list | None) -> RowPythonTypes | None:
        """Expect list or None from the response value, return as registered class (or namedtuple)
        or raise InterfaceError."""
        if response_value is None:
            return None

        self._check_to_python_param_type(list, response_value)

        if len(response_value) != len(self._field_converters):
            raise InterfaceError(
                f"Expected {len(self._field_converters)} fields for RowConverter but got "
                f"{len(response_value)}"
            )

        field_values = []
        for field_name, converter, field_value in zip(
            self._field_names, self._field_converters, response_value, strict=True
        ):
            # Each converter may raise if field value is unexpected type, range, etc.
            try:
                converted_field_value = converter.to_python_value(field_value)
            except Exception as e:
                raise InterfaceError(
                    f"Error converting field '{field_name}' value in RowConverter: {e}"
                ) from e

            field_values.append(converted_field_value)

        # Return an instance of the registered class corresponding to the
        # ROW's field names with the converted field values.
        return self._python_value_class(*field_values)

    @classmethod
    def to_statement_string(cls, python_value: RowPythonTypes) -> str:
        """Convert a Python tuple, namedtuple instance to its for-statement-string-interpolation
        representation, "(ROW(field1_value, field2_value, ...))".

        (The whole expression must be wrapped in parentheses when used in a larger expression,
        e.g., in an INSERT statement VALUES clause, otherwise strange parsing errors will occur.)
        """

        value_as_sequence: Sequence[Any]

        if isinstance(python_value, tuple):
            value_as_sequence = python_value
        elif is_dataclass(python_value):
            value_as_sequence = tuple(getattr(python_value, f.name) for f in fields(python_value))
        else:
            raise TypeMismatchError(
                converter_name=cls.__name__,
                method_name="to_statement_string",
                expected_type="tuple, namedtuple, NamedTuple, or dataclass",
                bad_value=python_value,
            )

        field_strings: list[str] = []
        for field_value in value_as_sequence:
            # May raise InterfaceError if individual field is not of a handled type.
            field_converter_cls = get_converter_for_python_value(field_value)

            field_str = field_converter_cls.to_statement_string(field_value)
            field_strings.append(field_str)

        return f"(ROW({', '.join(field_strings)}))"


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
    # Multiset type
    "MULTISET": MultisetConverter,
    # Row type
    "ROW": RowConverter,
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
    Counter: MultisetConverter,
    tuple: RowConverter,  # well, namedtuple is a duck-typed subclass of tuple
}

SupportedPythonTypes: TypeAlias = (
    None.__class__
    | SqlNone
    | bool
    | int
    | Decimal
    | float
    | date
    | time
    | str
    | bytes
    | datetime
    | YearMonthInterval
    | timedelta
    | list
    | dict
    | Counter
    | tuple
)


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


def get_converter_for_python_value(python_value: SupportedPythonTypes) -> type[TypeConverter]:
    """Get the TypeConverter class for the given Python value. Used prior to calling
    converter_class.to_statement_string().

    Raises InterfaceError if the type is not supported.
    """
    # Most converters can be found directly from the type of the value, other than
    # namedtuples which are duck-typed subclasses of tuple.
    value_type = type(python_value)

    # Will find for most types, including if user has provided a plain tuple to be converted
    # to a ROW.
    converter_class = _python_type_to_type_converter.get(value_type)
    if not converter_class and isinstance(python_value, tuple) and hasattr(python_value, "_fields"):
        # namedtuples handled by RowConverter
        converter_class = RowConverter

    if not converter_class:
        raise InterfaceError(f"Conversion for parameter of type {value_type} is not implemented.")

    return converter_class


def convert_statement_parameters(
    parameters: tuple | list,
) -> tuple:
    """Convert a list or tuple of Python parameters to a tuple of their string representations
    for interpolation into a %s-laden statement string.

    Returns: A tuple of string representations of the parameters.
    """

    # get_converter_for_python_value() may raise InterfaceError if any parameter's type is
    # not supported.
    return tuple(
        get_converter_for_python_value(param).to_statement_string(param) for param in parameters
    )


def determine_element_converter_cls(python_value: Iterable) -> type[TypeConverter]:
    """Determine the TypeConverter class for the elements of the given Python sequence.

    Assumes the list is non-empty and that all elements are of the same type, or
    contains None elements. Cannot be all None. The list will already have
    been proven to be non-empty by the caller.

    Returns: The TypeConverter class for the type of the first non-None element.

    Raises: InterfaceError if the element type is not supported.
    """
    for element in python_value:
        if element is not None:
            break
    else:
        raise InterfaceError("Cannot determine element type: all elements are None.")

    # Will raise InterfaceError if type not supported.
    try:
        return get_converter_for_python_value(element)
    except InterfaceError as e:
        raise InterfaceError(
            f"Conversion for array element of type {type(element)} is not implemented."
        ) from e
