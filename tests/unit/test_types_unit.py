"""Unit tests over type conversion between Flink and Python types."""

from collections.abc import Callable
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from math import isnan

import pytest

from confluent_sql.exceptions import InterfaceError
from confluent_sql.statement import ColumnTypeDefinition
from confluent_sql.types import (
    ArrayConverter,
    BooleanConverter,
    DateConverter,
    DaysIntervalConverter,
    DecimalConverter,
    FloatConverter,
    IntegerConverter,
    MapConverter,
    NullResultConverter,
    SqlNone,
    SqlNoneConverter,
    StringConverter,
    TimeConverter,
    TimestampConverter,
    VarBinaryConverter,
    YearMonthInterval,
    YearMonthIntervalConverter,
    _flink_type_name_to_converter_map,
    convert_statement_parameters,
    get_api_type_converter,
)


@pytest.mark.unit
@pytest.mark.typeconv
class TestYearMonthInterval:
    """Unit tests over YearMonthInterval type, new python type to represent
    Flink YEAR TO MONTH intervals."""

    @pytest.mark.parametrize(
        "years, months",
        [
            (2.5, 6),
            (3, "4"),
        ],
    )
    def test_hates_non_integer_init_args(self, years, months):
        with pytest.raises(
            TypeError,
            match="years and months must be integers",
        ):
            YearMonthInterval(years=years, months=months)  # type: ignore

    @pytest.mark.parametrize(
        "years, months",
        [
            (2, -3),
            (-1, 4),
        ],
    )
    def test_hates_mismatched_signs(self, years: int, months: int):
        with pytest.raises(
            ValueError,
            match="years and months must have the same sign",
        ):
            YearMonthInterval(years=years, months=months)

    @pytest.mark.parametrize(
        "months",
        [12, -15],
    )
    def test_hates_months_out_of_range(self, months: int):
        with pytest.raises(
            ValueError,
            match="months must be in the range -11 to 11",
        ):
            YearMonthInterval(years=0, months=months)

    @pytest.mark.parametrize(
        "years",
        [10000, -10000],
    )
    def test_hates_years_out_of_range(self, years: int):
        with pytest.raises(
            ValueError,
            match="years must be in the range -9999 to 9999",
        ):
            YearMonthInterval(years=years, months=0)

    @pytest.mark.parametrize(
        "yminterval, expected_is_negative",
        [
            (YearMonthInterval(years=2, months=6), False),
            (YearMonthInterval(years=0, months=5), False),
            (YearMonthInterval(years=0, months=0), False),
            (YearMonthInterval(years=0, months=-5), True),
            (YearMonthInterval(years=-1, months=-3), True),
        ],
    )
    def test_is_negative_property(self, yminterval: YearMonthInterval, expected_is_negative: bool):
        assert yminterval.is_negative == expected_is_negative

    @pytest.mark.parametrize(
        "years, months, expected_str",
        [
            (2, 6, "+2-06"),  # 2y 6m
            (0, 0, "+0-00"),  # zero interval
            (-1, -3, "-1-03"),  # -1y -3m
            (0, -3, "-0-03"),  # -3m
            (5, 0, "+5-00"),  # 5y
            (0, 10, "+0-10"),  # 10m
        ],
    )
    def test_str_representation(self, years: int, months: int, expected_str: str):
        interval = YearMonthInterval(years=years, months=months)
        assert str(interval) == expected_str

    LEAST_Y_M = YearMonthInterval(years=-9999, months=-11)
    LEAST_Y = YearMonthInterval(years=-9999, months=0)
    MIDDLE = YearMonthInterval(years=0, months=0)
    GREATEST_Y = YearMonthInterval(years=9999, months=0)
    GREATEST_Y_M = YearMonthInterval(years=9999, months=11)

    @pytest.mark.parametrize(
        "left, right, expected",
        [
            (MIDDLE, MIDDLE, 0),
            (LEAST_Y_M, LEAST_Y, -1),
            (LEAST_Y, LEAST_Y_M, 1),
            (LEAST_Y, MIDDLE, -1),
            (MIDDLE, LEAST_Y, 1),
            (GREATEST_Y, MIDDLE, 1),
            (MIDDLE, GREATEST_Y, -1),
            (GREATEST_Y, GREATEST_Y_M, -1),
            (GREATEST_Y_M, GREATEST_Y, 1),
        ],
    )
    def test_comparisons(self, left: YearMonthInterval, right: YearMonthInterval, expected: int):
        if expected < 0:
            assert left < right
            assert left <= right
            assert not left > right
            assert left != right
        elif expected > 0:
            assert left > right
            assert left >= right
            assert not left < right
            assert left != right
        else:
            assert left == right
            assert not left < right
            assert not left > right

    @pytest.mark.parametrize(
        "bound_comparator",
        [
            MIDDLE.__lt__,
            MIDDLE.__le__,
            MIDDLE.__gt__,
            MIDDLE.__ge__,
            MIDDLE.__eq__,
            MIDDLE.__ne__,
        ],
    )
    def test_comparison_with_non_interval_is_not_implemented(
        self, bound_comparator: Callable[[YearMonthInterval], bool]
    ):
        assert bound_comparator(42) is NotImplemented  # type: ignore

    def test_hashability(self):
        interval1 = YearMonthInterval(years=2, months=3)
        interval2 = YearMonthInterval(years=2, months=3)  # equivalent to interval1
        interval3 = YearMonthInterval(years=-1, months=0)

        assert interval1 == interval2 and interval1 is not interval2
        assert hash(interval1) == hash(interval2)
        assert hash(interval1) != hash(interval3)

        # Make a dict with YearMonthInterval keys for fun and profit
        interval_dict = {
            interval1: "interval one or two",
            interval3: "interval three",
        }
        assert interval_dict[interval2] == "interval one or two"
        assert interval_dict[interval3] == "interval three"


@pytest.mark.unit
@pytest.mark.typeconv
class TestNullConverter:
    """Unit tests over NullConverter."""

    converter = NullResultConverter(ColumnTypeDefinition(type="NULL", nullable=True))

    def test_to_python_value(self):
        assert self.converter.to_python_value(None) is None

    def test_to_python_value_invalid_type(self):
        with pytest.raises(
            ValueError, match="Expected None value for NullConverter but got <class 'int'>"
        ):
            self.converter.to_python_value(123)  # type: ignore

    def test_to_statement_string_always_throws(self):
        with pytest.raises(
            InterfaceError, match="cannot convert Python None to statement string directly"
        ):
            NullResultConverter.to_statement_string("anything")  # type: ignore


@pytest.mark.unit
@pytest.mark.typeconv
class TestSqlNoneConverter:
    """Unit tests over SqlNoneConverter."""

    def test_to_python_value_always_throws(self):
        converter = SqlNoneConverter(ColumnTypeDefinition(type="INTEGER", nullable=True))
        with pytest.raises(InterfaceError, match="cannot convert from response values to Python"):
            converter.to_python_value("12")

    def test_to_statement_string_invalid_type(self):
        with pytest.raises(
            ValueError,
            match="Expected SqlNone value for SqlNoneConverter but got <class 'int'>",
        ):
            SqlNoneConverter.to_statement_string(123)  # type: ignore

    def test_to_statement_string(self):
        result = SqlNoneConverter.to_statement_string(SqlNone.INTEGER)
        assert result == "cast (null as INTEGER)"


@pytest.mark.unit
@pytest.mark.typeconv
class TestStringConverter:
    """Unit tests over StringConverter."""

    @pytest.mark.parametrize("value, expected", [("hello", "hello"), (None, None)])
    def test_to_python_value(self, value, expected):
        converter = StringConverter(ColumnTypeDefinition(type="STRING", nullable=False))
        assert converter.to_python_value(value) == expected

    def test_to_python_value_invalid_type(self):
        converter = StringConverter(ColumnTypeDefinition(type="STRING", nullable=False))
        with pytest.raises(
            ValueError, match="Expected string value for StringConverter but got <class 'int'>"
        ):
            converter.to_python_value(123)  # type: ignore

    @pytest.mark.parametrize(
        "value, expected",
        [
            # Simple string -- gets wrapped in single quotes
            ("hello", "'hello'"),
            # String with an innocent interior single quote -- single quote gets escaped by doubling
            ("O'Reilly", "'O''Reilly'"),
            # Same variation, but with SQL injection attempt
            ("Robert'); DROP TABLE Students;--", "'Robert''); DROP TABLE Students;--'"),
            # Empty string becomes two single quotes.
            ("", "''"),
        ],
    )
    def test_to_statement_string(self, value, expected):
        result = StringConverter.to_statement_string(value)
        assert result == expected

    def test_to_statement_string_invalid_type(self):
        with pytest.raises(
            ValueError,
            match="Expected Python string value for StringConverter but got <class 'int'>",
        ):
            StringConverter.to_statement_string(123)  # type: ignore

    def test_to_statement_guard_against_malicious_subclass(self):
        """Test that to_statement_string guards against malicious subclasses of str."""

        class MaliciousStr(str):
            """A string subclass that attempts to inject SQL code via overridden __str__
            and __iter__ methods."""

            poison = "malicious_code()'; DROP TABLE users;--"

            def __str__(self):
                return self.poison

            def __iter__(self):
                return iter(self.poison)

        malicious_value = MaliciousStr("innocent_looking_string")
        result = StringConverter.to_statement_string(malicious_value)

        # The result should be the escaped version of the poison string -- they
        # should not be able to inject code by overriding __str__.

        assert result == "'malicious_code()''; DROP TABLE users;--'"


@pytest.mark.unit
@pytest.mark.typeconv
class TestVarBinaryConverter:
    """Unit tests over VarBinaryConverter."""

    converter = VarBinaryConverter(ColumnTypeDefinition(type="VARBINARY", nullable=True))

    @pytest.mark.parametrize("value, expected", [("x'7f0203'", b"\x7f\x02\x03"), (None, None)])
    def test_to_python_value(self, value: str, expected: bytes):
        assert self.converter.to_python_value(value) == expected

    def test_to_python_value_invalid_type(self):
        with pytest.raises(
            ValueError, match="Expected string value for VarBinaryConverter but got <class 'int'>"
        ):
            self.converter.to_python_value(123)  # type: ignore

    def test_to_python_value_invalid_format(self):
        with pytest.raises(
            ValueError,
            match="Expected hex-pair encoded string",
        ):
            self.converter.to_python_value("7f0203'")  # Missing x' prefix

    def test_to_python_value_invalid_hex(self):
        with pytest.raises(
            ValueError,
            match="Invalid hex string",
        ):
            self.converter.to_python_value("x'7g0203'")  # 'g' is not a valid hex digit

    @pytest.mark.parametrize(
        "value, expected",
        [
            # Simple bytes -- gets represented as hex string prefixed with X
            (b"hello", "x'68656c6c6f'"),
            # Empty bytes becomes x''
            (b"", "x''"),
        ],
    )
    def test_to_statement_string(self, value, expected):
        result = VarBinaryConverter.to_statement_string(value)
        assert result == expected

    def test_to_statement_string_invalid_type(self):
        with pytest.raises(
            ValueError,
            match="Expected bytes value for VarBinaryConverter but got <class 'str'>",
        ):
            VarBinaryConverter.to_statement_string("hello")  # type: ignore


@pytest.mark.unit
@pytest.mark.typeconv
class TestIntegerConverter:
    """Unit tests over IntegerConverter."""

    converter = IntegerConverter(ColumnTypeDefinition(type="INTEGER", nullable=False))

    @pytest.mark.parametrize("value, expected", [("123", 123), (None, None)])
    def test_to_python_value(self, value, expected):
        assert self.converter.to_python_value(value) == expected

    def test_to_python_value_invalid_type(self):
        with pytest.raises(
            ValueError,
            match="Expected integers to be encoded as JSON strings but got <class 'int'>",
        ):
            self.converter.to_python_value(123)  # type: ignore

    @pytest.mark.parametrize(
        "value, expected",
        [
            (123, "123"),
            (0, "0"),
            (-456, "-456"),
        ],
    )
    def test_to_statement_string(self, value, expected):
        result = IntegerConverter.to_statement_string(value)
        assert result == expected

    def test_to_statement_value_invalid_type(self):
        with pytest.raises(
            ValueError,
            match="Expected Python integer value for IntegerConverter but got <class 'str'>",
        ):
            IntegerConverter.to_statement_string("123")  # type: ignore


@pytest.mark.unit
@pytest.mark.typeconv
class TestDecimalConverter:
    """Unit tests over DecimalConverter."""

    converter = DecimalConverter(ColumnTypeDefinition(type="DECIMAL", nullable=True))

    @pytest.mark.parametrize("value, expected", [("123.45", Decimal("123.45")), (None, None)])
    def test_to_python_value(self, value, expected):
        assert self.converter.to_python_value(value) == expected

    def test_to_python_value_invalid_type(self):
        with pytest.raises(
            ValueError,
            match="Expected decimal to be encoded as JSON strings but got <class 'int'>",
        ):
            self.converter.to_python_value(123)  # type: ignore

    @pytest.mark.parametrize(
        "value, expected",
        [
            (Decimal("123.4564564"), "cast('123.4564564' as decimal(10,7))"),
            (Decimal("0"), "cast('0' as decimal(1,0))"),
            (Decimal("-678.90"), "cast('-678.90' as decimal(5,2))"),
        ],
    )
    def test_to_statement_string(self, value, expected):
        result = DecimalConverter.to_statement_string(value)
        assert result == expected

    def test_to_statement_value_invalid_type(self):
        with pytest.raises(
            ValueError,
            match="Expected Python Decimal value for DecimalConverter but got <class 'int'>",
        ):
            DecimalConverter.to_statement_string(123)  # type: ignore

    def test_to_statement_guard_against_malicious_subclass(self):
        """Test that to_statement_string guards against malicious subclasses of int."""

        class MaliciousInt(int):
            """An integer subclass that attempts to inject SQL code via overridden __str__."""

            def __str__(self):
                return "0'; DROP TABLE users;--"

        malicious_value = MaliciousInt(42)
        result = IntegerConverter.to_statement_string(malicious_value)

        # The result should be the stringified integer value -- they
        # should not be able to inject code by overriding __str__.

        assert result == "42"


@pytest.mark.unit
@pytest.mark.typeconv
class TestFloatConverter:
    """Unit tests over FloatConverter."""

    converter = FloatConverter(ColumnTypeDefinition(type="FLOAT", nullable=True))

    @pytest.mark.parametrize(
        "value, expected",
        [
            ("123.5", float("123.5")),
            ("0.0", float("0.0")),
            ("NaN", float("nan")),
            ("Infinity", float("inf")),
            ("-Infinity", float("-inf")),
            (None, None),
        ],
    )
    def test_to_python_value(self, value, expected):
        result = self.converter.to_python_value(value)
        # Special handling for NaN comparison
        if value == "NaN":
            # nan is never equal to itself.
            assert isnan(result)  # pyright: ignore[reportArgumentType]
        else:
            # Regular comparison works for regular and Infinity values.
            assert result == expected

    def test_to_python_value_invalid_type(self):
        with pytest.raises(
            ValueError,
            match="Expected float to be encoded as JSON string but got <class 'int'>",
        ):
            self.converter.to_python_value(123)  # type: ignore

    @pytest.mark.parametrize(
        "value, expected",
        [
            (123.45, "123.45"),
            (0.0, "0.0"),
            (-678.9, "-678.9"),
        ],
    )
    def test_to_statement_string(self, value, expected):
        result = FloatConverter.to_statement_string(value)
        assert result == expected

    @pytest.mark.parametrize("bad_value", [float("nan"), float("inf"), float("-inf")])
    def test_to_statement_string_rejects_nan_inf(self, bad_value: float):
        """Ensure that NaN and Infinity are rejected, as they are not supported
        in Flink SQL statements."""
        with pytest.raises(
            ValueError,
            match="Cannot convert NaN or Infinity to a Flink SQL float/double literal",
        ):
            FloatConverter.to_statement_string(bad_value)

    def test_to_statement_value_invalid_type(self):
        with pytest.raises(
            ValueError,
            match="Expected Python float value for FloatConverter but got <class 'str'>",
        ):
            FloatConverter.to_statement_string("sdf")  # type: ignore


@pytest.mark.unit
@pytest.mark.typeconv
class TestBooleanConverter:
    """Unit tests over BooleanConverter."""

    converter = BooleanConverter(ColumnTypeDefinition(type="BOOLEAN", nullable=False))

    @pytest.mark.parametrize(
        "value, expected",
        [("TRUE", True), ("FALSE", False), (None, None)],
    )
    def test_to_python_value(self, value, expected):
        assert self.converter.to_python_value(value) == expected

    def test_to_python_value_invalid_type(self):
        with pytest.raises(
            ValueError, match="Expected string value for BooleanConverter but got <class 'int'>"
        ):
            self.converter.to_python_value(1)  # type: ignore

    @pytest.mark.parametrize(
        "value, expected",
        [
            (True, "TRUE"),
            (False, "FALSE"),
        ],
    )
    def test_to_statement_string(self, value, expected):
        result = BooleanConverter.to_statement_string(value)
        assert result == expected

    def test_to_statement_string_invalid_type(self):
        with pytest.raises(
            ValueError,
            match="Expected Python boolean value for BooleanConverter but got <class 'str'>",
        ):
            BooleanConverter.to_statement_string("TRUE")  # type: ignore


@pytest.mark.unit
@pytest.mark.typeconv
class TestDateConverter:
    """Unit tests over DateConverter."""

    converter = DateConverter(ColumnTypeDefinition(type="DATE", nullable=False))

    @pytest.mark.parametrize(
        "value, expected",
        [("2024-06-15", date(2024, 6, 15)), (None, None)],
    )
    def test_to_python_value(self, value, expected):
        assert self.converter.to_python_value(value) == expected

    def test_to_python_value_invalid_type(self):
        with pytest.raises(
            ValueError, match="Expected date to be encoded as JSON string but got <class 'int'>"
        ):
            self.converter.to_python_value(20240615)  # type: ignore

    def test_to_python_value_invalid_format(self):
        with pytest.raises(
            ValueError,
            match="Invalid date string",
        ):
            self.converter.to_python_value("15-06-2024")  # Wrong format

    @pytest.mark.parametrize(
        "value, expected",
        [
            (date(2024, 6, 15), "DATE '2024-06-15'"),
            (date(2000, 1, 1), "DATE '2000-01-01'"),
        ],
    )
    def test_to_statement_string(self, value, expected):
        result = DateConverter.to_statement_string(value)
        assert result == expected

    def test_to_statement_string_invalid_type(self):
        with pytest.raises(
            ValueError,
            match="Expected Python datetime.date value for DateConverter but got <class 'str'>",
        ):
            DateConverter.to_statement_string("2024-06-15")  # type: ignore


@pytest.mark.unit
@pytest.mark.typeconv
class TestTimeConverter:
    """Unit tests over TimeConverter."""

    converter = TimeConverter(ColumnTypeDefinition(type="TIME", nullable=False))

    @pytest.mark.parametrize(
        "value, expected",
        [("12:34:56", time(12, 34, 56)), ("12:34:56.789", time(12, 34, 56, 789000)), (None, None)],
    )
    def test_to_python_value(self, value, expected):
        assert self.converter.to_python_value(value) == expected

    def test_to_python_value_invalid_type(self):
        with pytest.raises(
            ValueError, match="Expected time to be encoded as JSON string but got <class 'int'>"
        ):
            self.converter.to_python_value(123456)  # type: ignore

    def test_to_python_value_invalid_format(self):
        with pytest.raises(
            ValueError,
            match="Invalid time string",
        ):
            self.converter.to_python_value("12.34.56")  # Wrong format for time.fromisoformat().

    @pytest.mark.parametrize(
        "value, expected",
        [
            (time(12, 34, 56), "TIME '12:34:56.000000'"),
            (time(12, 34, 56, 789000), "TIME '12:34:56.789000'"),
            (time(0, 0, 0), "TIME '00:00:00.000000'"),
        ],
    )
    def test_to_statement_string(self, value, expected):
        result = TimeConverter.to_statement_string(value)
        assert result == expected

    def test_to_statement_string_invalid_type(self):
        with pytest.raises(
            ValueError,
            match="Expected Python datetime.time value for TimeConverter but got <class 'str'>",
        ):
            TimeConverter.to_statement_string("12:34:56.789")  # type: ignore


@pytest.mark.unit
@pytest.mark.typeconv
class TestTimestampConverter:
    """Unit tests over TimestampConverter."""

    # Separate converters for TIMESTAMP and TIMESTAMP_LTZ to test both variants
    ts_converter = TimestampConverter(
        ColumnTypeDefinition(type="TIMESTAMP_WITHOUT_TIME_ZONE", nullable=False)
    )
    lts_converter = TimestampConverter(
        ColumnTypeDefinition(type="TIMESTAMP_WITH_LOCAL_TIME_ZONE", nullable=False)
    )

    @pytest.mark.parametrize(
        "bad_name",
        [
            "TIMESTAMP",
            "TIMESTAMP_LTZ",
        ],
    )
    def test_constructor_hates_alternative_type_names(self, bad_name: str):
        with pytest.raises(
            ValueError,
            match="TimestampConverter can only be used",
        ):
            TimestampConverter(ColumnTypeDefinition(type=bad_name, nullable=False))

    @pytest.mark.parametrize(
        "converter, str_value, expected",
        [
            # ts without timezone
            (ts_converter, "2024-06-15 12:34:56", datetime(2024, 6, 15, 12, 34, 56)),
            (ts_converter, "2024-06-15 12:34:56.789", datetime(2024, 6, 15, 12, 34, 56, 789000)),
            # ts with timezone
            (
                lts_converter,
                "2023-06-15 12:34:56",
                datetime(
                    2023,
                    6,
                    15,
                    12,
                    34,
                    56,
                    tzinfo=timezone(timedelta(hours=0)),
                ),
            ),
            # null value
            (ts_converter, None, None),
        ],
    )
    def test_to_python_value(
        self, converter: TimestampConverter, str_value: str, expected: datetime | None
    ):
        assert converter.to_python_value(str_value) == expected

    def test_to_python_value_invalid_type(self):
        with pytest.raises(
            ValueError,
            match="Expected timestamp to be encoded as JSON string but got <class 'bool'>",
        ):
            self.ts_converter.to_python_value(False)

    def test_to_python_value_hates_timezone_in_timestamp(self):
        """Ensure that if Flink serialization ever changes, we will notice, because
        other assumptions may be invalid"""
        with pytest.raises(
            ValueError,
            match="Expected timezone-naive timestamp string from Flink but got",
        ):
            self.ts_converter.to_python_value("2024-06-15 12:34:56+02:00")  # Has timezone

    def test_to_python_value_invalid_format(self):
        with pytest.raises(
            ValueError,
            match="Invalid timestamp string",
        ):
            self.ts_converter.to_python_value("2024/06/15 12:34:56")  # Wrong date spelling format

    @pytest.mark.parametrize(
        "value, expected",
        [
            (datetime(2024, 6, 15, 12, 34, 56), "cast('2024-06-15 12:34:56.000000' as timestamp)"),
            (
                datetime(2024, 6, 15, 12, 34, 56, 789000),
                "cast('2024-06-15 12:34:56.789000' as timestamp)",
            ),
            (
                datetime(
                    2024,
                    6,
                    15,
                    12,
                    34,
                    56,
                    tzinfo=timezone(timedelta(hours=2)),
                ),
                # Projected 2h behind to UTC for TIMESTAMP_LTZ, so 12:34:56+02:00 becomes 10:34:56Z
                "cast('2024-06-15 10:34:56.000000' as timestamp_ltz)",
            ),
            (
                datetime(
                    2024,
                    6,
                    15,
                    12,
                    34,
                    56,
                    789000,
                    tzinfo=timezone(timedelta(hours=-5)),
                ),
                # Projected +5 hours to UTC for TIMESTAMP_LTZ, so 12:34:56.789-05:00
                # becomes 17:34:56.789Z
                "cast('2024-06-15 17:34:56.789000' as timestamp_ltz)",
            ),
        ],
    )
    def test_to_statement_string(self, value, expected):
        result = self.ts_converter.to_statement_string(value)
        assert result == expected

    def test_to_statement_string_invalid_type(self):
        with pytest.raises(
            ValueError,
            match="Expected Python datetime.datetime value",
        ):
            self.ts_converter.to_statement_string("2024-06-15 12:34:56")  # type: ignore


@pytest.mark.unit
@pytest.mark.typeconv
class TestYearMonthIntervalConverter:
    """Unit tests over YearMonthIntervalConverter."""

    converter = YearMonthIntervalConverter(
        ColumnTypeDefinition(type="INTERVAL_YEAR_MONTH", nullable=True)
    )

    def test_constructor_hates_alternative_type_name(self):
        with pytest.raises(
            ValueError,
            match="YearMonthIntervalConverter can only be used",
        ):
            YearMonthIntervalConverter(ColumnTypeDefinition(type="INTERVAL_YM", nullable=True))

    @pytest.mark.parametrize(
        "value, expected",
        [
            ("+2-06", YearMonthInterval(years=2, months=6)),
            ("-1-03", YearMonthInterval(years=-1, months=-3)),
            (None, None),
        ],
    )
    def test_to_python_value(self, value, expected):
        assert self.converter.to_python_value(value) == expected

    def test_to_python_value_invalid_type(self):
        with pytest.raises(
            ValueError,
            match="Expected interval to be encoded as JSON string",
        ):
            self.converter.to_python_value(123)  # type: ignore

    def test_to_python_value_invalid_format(self):
        with pytest.raises(
            ValueError,
            match="Invalid interval string",
        ):
            self.converter.to_python_value("2:06")  # Wrong format

    @pytest.mark.parametrize(
        "value, expected",
        [
            (YearMonthInterval(years=2, months=6), "INTERVAL '+2-06' YEAR TO MONTH"),
            (YearMonthInterval(years=-1, months=-3), "INTERVAL '-1-03' YEAR TO MONTH"),
        ],
    )
    def test_to_statement_string(self, value, expected):
        result = YearMonthIntervalConverter.to_statement_string(value)
        assert result == expected

    def test_to_statement_string_invalid_type(self):
        with pytest.raises(
            ValueError,
            match="Expected Python YearMonthInterval",
        ):
            YearMonthIntervalConverter.to_statement_string("2-06")  # type: ignore


@pytest.mark.unit
@pytest.mark.typeconv
class TestDaysIntervalConverter:
    """Unit tests over DaysIntervalConverter."""

    converter = DaysIntervalConverter(ColumnTypeDefinition(type="INTERVAL_DAY_TIME", nullable=True))

    @pytest.mark.parametrize(
        "value, expected",
        [
            (
                # positive interval without fractional seconds
                "+10 12:30:45",
                timedelta(days=10, hours=12, minutes=30, seconds=45),
            ),
            (
                # positive interval with fractional seconds
                "+10 12:30:45.123",
                timedelta(days=10, hours=12, minutes=30, seconds=45, milliseconds=123),
            ),
            (
                # zero interval
                "+0 00:00:00",
                timedelta(days=0, hours=0, minutes=0, seconds=0),
            ),
            (
                # negative interval, less than one day, no microseconds
                "-0 01:15:30",
                -1 * timedelta(hours=1, minutes=15, seconds=30),
            ),
            (
                # negative interval, less than one day, fractional seconds at low precision.
                "-0 12:30:15.5",
                -1 * timedelta(hours=12, minutes=30, seconds=15, milliseconds=500),
            ),
            (
                # negative interval, days only.
                "-5 00:00:00",
                timedelta(days=-5),
            ),
            (
                # negative interval beyond one day without fractional seconds
                "-5 01:02:03",
                timedelta(days=-5, hours=-1, minutes=-2, seconds=-3),
            ),
            (
                # negative interval beyond one day with fractional seconds
                "-5 01:02:03.456",
                -1 * timedelta(days=5, hours=1, minutes=2, seconds=3, microseconds=456000),
            ),
            (
                # null value
                None,
                None,
            ),
        ],
    )
    def test_to_python_value(self, value, expected):
        assert self.converter.to_python_value(value) == expected

    def test_to_python_value_invalid_type(self):
        with pytest.raises(
            ValueError,
            match="Expected interval to be encoded as JSON string",
        ):
            self.converter.to_python_value(123)  # type: ignore

    def test_to_python_value_invalid_format(self):
        with pytest.raises(
            ValueError,
            match="Invalid interval string",
        ):
            self.converter.to_python_value("10:12:30")  # Wrong format

    @pytest.mark.parametrize(
        "value, expected",
        [
            (
                # positive interval without fractional seconds
                timedelta(days=10, hours=12, minutes=30, seconds=45),
                "INTERVAL '+10 12:30:45' DAY TO SECOND",
            ),
            (
                # positive interval with fractional seconds
                timedelta(days=10, hours=12, minutes=30, seconds=45, milliseconds=123),
                "INTERVAL '+10 12:30:45.123000' DAY TO SECOND(6)",
            ),
            (
                # zero interval
                timedelta(days=0, hours=0, minutes=0, seconds=0),
                "INTERVAL '+0 00:00:00' DAY TO SECOND",
            ),
            (
                # negative interval without fractional seconds
                -1 * timedelta(days=5),
                "INTERVAL '-5 00:00:00' DAY TO SECOND",
            ),
            (
                # negative interval with fractional seconds
                -1 * timedelta(days=5, hours=1, minutes=2, seconds=3, microseconds=456000),
                "INTERVAL '-5 01:02:03.456000' DAY TO SECOND(6)",
            ),
        ],
    )
    def test_to_statement_string(self, value, expected):
        result = DaysIntervalConverter.to_statement_string(value)
        assert result == expected

    def test_to_statement_string_invalid_type(self):
        with pytest.raises(
            ValueError,
            match="Expected Python timedelta",
        ):
            DaysIntervalConverter.to_statement_string("10 12:30:45.123")  # type: ignore


@pytest.mark.unit
@pytest.mark.typeconv
class TestGetDataTypeConverter:
    """Unit tests over get_data_type_converter function."""

    @pytest.mark.parametrize(
        "column_type_name, expected_converter_cls",
        [
            # Simpler than Integer types
            ("NULL", NullResultConverter),
            ("BOOLEAN", BooleanConverter),
            # Integer types
            ("TINYINT", IntegerConverter),
            ("SMALLINT", IntegerConverter),
            ("INTEGER", IntegerConverter),
            ("BIGINT", IntegerConverter),
            # Fixed precision types
            ("DECIMAL", DecimalConverter),
            ("DEC", DecimalConverter),
            ("NUMERIC", DecimalConverter),
            # Floating point types
            ("FLOAT", FloatConverter),
            ("DOUBLE", FloatConverter),
            ("DOUBLE PRECISION", FloatConverter),
            # Date / time types
            ("DATE", DateConverter),
            ("TIME", TimeConverter),
            ("TIMESTAMP_WITHOUT_TIME_ZONE", TimestampConverter),
            ("TIMESTAMP_WITH_LOCAL_TIME_ZONE", TimestampConverter),
            # Character string types
            ("CHAR", StringConverter),
            ("VARCHAR", StringConverter),
            ("STRING", StringConverter),
            # Binary types
            ("VARBINARY", VarBinaryConverter),
            ("BINARY", VarBinaryConverter),
            ("BYTES", VarBinaryConverter),
        ],
    )
    def test_get_data_type_converter(self, column_type_name, expected_converter_cls):
        """Test that the correct TypeConverter is returned for given type descriptions."""

        # As if fragment from REST response ...
        column_type_dict = {
            "type": column_type_name,
            "nullable": False,
        }

        column_type_definition = ColumnTypeDefinition.from_response(column_type_dict)
        converter = get_api_type_converter(column_type_definition)
        assert isinstance(converter, expected_converter_cls), (
            f"Expected {expected_converter_cls} given but got {type(converter)}"
        )

    def test_get_data_type_converter_unsupported_type(self):
        """Test that NotImplementedError is raised for unsupported types."""

        column_type_dict = {
            "type": "UNSUPPORTED_TYPE",
            "nullable": False,
        }

        column_type_definition = ColumnTypeDefinition.from_response(column_type_dict)

        with pytest.raises(
            NotImplementedError, match="TypeConverter for UNSUPPORTED_TYPE is not implemented."
        ):
            get_api_type_converter(column_type_definition)


@pytest.mark.unit
@pytest.mark.typeconv
class TestConvertStatementParameters:
    """Unit tests over convert_statement_parameters(), proving that the
    expected type converters are registered and used."""

    value_expected_string_pairs = [
        (SqlNone.INTEGER, "cast (null as INTEGER)"),
        (SqlNone(int), "cast (null as INTEGER)"),
        (True, "TRUE"),
        (False, "FALSE"),
        (123, "123"),
        (Decimal("45.67"), "cast('45.67' as decimal(4,2))"),
        (12.34, "12.34"),
        ("test", "'test'"),
        (b"\x01\x02", "x'0102'"),
        (time(12, 34, 56), "TIME '12:34:56.000000'"),
        (time(12, 34, 56, 789000), "TIME '12:34:56.789000'"),
        (date(2024, 6, 15), "DATE '2024-06-15'"),
        (date(2024, 6, 15), "DATE '2024-06-15'"),
        (datetime(2024, 6, 15, 12, 34, 56), "cast('2024-06-15 12:34:56.000000' as timestamp)"),
        (
            datetime(
                2024,
                6,
                15,
                12,
                34,
                56,
                tzinfo=timezone(timedelta(hours=2)),
            ),
            # Projected 2h behind to UTC
            "cast('2024-06-15 10:34:56.000000' as timestamp_ltz)",
        ),
    ]

    @pytest.mark.parametrize("value, expected_string", value_expected_string_pairs)
    def test_single_good_conversion(self, value, expected_string):
        # Test one at a time to isolate single-type conversion fails
        params = [value]
        expected = (expected_string,)
        result = convert_statement_parameters(params)
        assert result == expected

    def test_all_types_conversion(self):
        # Test all at once to prove multi-type conversion works
        params = [pair[0] for pair in self.value_expected_string_pairs]
        expected = tuple(pair[1] for pair in self.value_expected_string_pairs)
        result = convert_statement_parameters(params)
        assert result == expected


@pytest.mark.unit
@pytest.mark.typeconv
class TestArrayConverter:
    """Unit tests over ArrayConverter."""

    def test_constructor_hates_non_array_type(self):
        with pytest.raises(
            InterfaceError,
            match="ArrayConverter can only be used with ARRAY types, got INTEGER",
        ):
            ArrayConverter(ColumnTypeDefinition(type="INTEGER", nullable=False))

    def test_constructor_hates_missing_element_type(self):
        with pytest.raises(
            InterfaceError,
            match="ArrayConverter cannot determine element type from column type definition",
        ):
            ArrayConverter(ColumnTypeDefinition(type="ARRAY", nullable=False))

    def test_constructor_hates_unsupported_element_type(self):
        with pytest.raises(
            TypeError,
            match="Conversion for array element of type UNKNOWN is not implemented.",
        ):
            ArrayConverter(
                ColumnTypeDefinition(
                    type="ARRAY",
                    nullable=False,
                    element_type=ColumnTypeDefinition(type="UNKNOWN", nullable=False),
                )
            )

    int_array_converter = ArrayConverter(
        ColumnTypeDefinition(
            type="ARRAY",
            nullable=True,
            element_type=ColumnTypeDefinition(type="INTEGER", nullable=False),
        )
    )

    def test_to_python_value_invalid_type(self):
        with pytest.raises(
            ValueError,
            match="Expected list value for ArrayConverter but got <class 'str'>",
        ):
            self.int_array_converter.to_python_value("not an array")  # type: ignore

    def test_to_python_value_invalid_element(self):
        with pytest.raises(
            ValueError,
            match="invalid literal for int",
        ):
            self.int_array_converter.to_python_value(["10", "not an int", "30"])

    @pytest.mark.parametrize(
        "from_json_payload, expected",
        [
            (["10", "20", "30"], [10, 20, 30]),
            (["12", None, "34"], [12, None, 34]),  # Some members may be null
            ([], []),  # We can go from statement result empty array to Python empty list.
            (None, None),
        ],
    )
    def test_to_python_value(self, from_json_payload, expected):
        result = self.int_array_converter.to_python_value(from_json_payload)
        assert result == expected

    def test_to_statement_string_invalid_type(self):
        with pytest.raises(
            ValueError,
            match="Expected list value for ArrayConverter but got <class 'str'>",
        ):
            self.int_array_converter.to_statement_string("not an array")  # type: ignore

    def test_to_statement_string_hates_empty_array(self):
        with pytest.raises(
            ValueError,
            match="Cannot convert empty list to Flink ARRAY literal",
        ):
            self.int_array_converter.to_statement_string([])

    def test_to_statement_string_unsupported_first_element(self):
        class UserObject:
            pass

        with pytest.raises(
            TypeError,
            match="Conversion for array element of type .* is not implemented.",
        ):
            self.int_array_converter.to_statement_string([UserObject(), UserObject()])

    def test_to_statement_string_hates_mixed_type_elements(self):
        with pytest.raises(
            ValueError,
            match="Expected Python integer value for IntegerConverter but got <class 'str'>",
        ):
            self.int_array_converter.to_statement_string([1, "two", 3])

    def test_to_statement_string_hates_all_none_elements(self):
        with pytest.raises(
            ValueError,
            match="Cannot determine element type: all elements are None.",
        ):
            self.int_array_converter.to_statement_string([None, None, None])

    @pytest.mark.parametrize(
        "python_value, expected_statement_string",
        [
            ([10, 20, 30], "ARRAY[10, 20, 30]"),
            # Some members may be None
            ([12, None, 34], "ARRAY[12, cast (null as INTEGER), 34]"),
            # Even the leading members may be None as long as not all are None
            ([None, None, 34], "ARRAY[cast (null as INTEGER), cast (null as INTEGER), 34]"),
        ],
    )
    def test_to_statement_string(self, python_value, expected_statement_string):
        result = self.int_array_converter.to_statement_string(python_value)
        assert result == expected_statement_string

    @pytest.mark.parametrize(
        "python_value, expected_statement_string",
        [
            ([[1, 2], [3, 4]], "ARRAY[ARRAY[1, 2], ARRAY[3, 4]]"),
            (
                [[None, 2], [3, None]],
                "ARRAY[ARRAY[cast (null as INTEGER), 2], ARRAY[3, cast (null as INTEGER)]]",
            ),
            (
                [None, [3, 4]],  # Outer array has a null element array
                "ARRAY[cast (null as ARRAY), ARRAY[3, 4]]",
            ),
        ],
    )
    def test_to_statement_string_nested_arrays(self, python_value, expected_statement_string):
        result = ArrayConverter.to_statement_string(python_value)
        assert result == expected_statement_string

    nested_array_converter = ArrayConverter(
        ColumnTypeDefinition(
            type="ARRAY",
            nullable=True,
            element_type=ColumnTypeDefinition(
                type="ARRAY",
                nullable=True,
                element_type=ColumnTypeDefinition(type="INTEGER", nullable=True),
            ),
        )
    )

    @pytest.mark.parametrize(
        "from_json_payload, expected",
        [
            ([["1", "2"], ["3", "4"]], [[1, 2], [3, 4]]),
            ([[None, "2"], ["3", None]], [[None, 2], [3, None]]),
            ([None, ["3", "4"]], [None, [3, 4]]),  # first outer array element is null, second not
            (None, None),  # Null outer array
        ],
    )
    def test_nested_array_converter_to_python_value(self, from_json_payload, expected):
        result = self.nested_array_converter.to_python_value(from_json_payload)
        assert result == expected


@pytest.mark.unit
@pytest.mark.typeconv
class TestMapConverter:
    """Unit tests over MapConverter."""

    def test_constructor_hates_non_map_type(self):
        with pytest.raises(
            InterfaceError,
            match="MapConverter can only be used with MAP types, got INTEGER",
        ):
            MapConverter(ColumnTypeDefinition(type="INTEGER", nullable=False))

    def test_constructor_hates_missing_key_type(self):
        with pytest.raises(
            InterfaceError,
            match="MapConverter cannot determine key type from column type definition",
        ):
            MapConverter(
                ColumnTypeDefinition(
                    type="MAP",
                    nullable=False,
                    value_type=ColumnTypeDefinition(type="STRING", nullable=False),
                )
            )

    def test_constructor_hates_missing_value_type(self):
        with pytest.raises(
            InterfaceError,
            match="MapConverter cannot determine value type from column type definition",
        ):
            MapConverter(
                ColumnTypeDefinition(
                    type="MAP",
                    nullable=False,
                    key_type=ColumnTypeDefinition(type="STRING", nullable=False),
                )
            )

    def test_constructor_hates_unsupported_key_type(self):
        with pytest.raises(
            TypeError,
            match="Conversion for map key of type UNKNOWN is not implemented.",
        ):
            MapConverter(
                ColumnTypeDefinition(
                    type="MAP",
                    nullable=False,
                    key_type=ColumnTypeDefinition(type="UNKNOWN", nullable=False),
                    value_type=ColumnTypeDefinition(type="STRING", nullable=False),
                )
            )

    def test_constructor_hates_unsupported_value_type(self):
        with pytest.raises(
            TypeError,
            match="Conversion for map value of type UNKNOWN is not implemented.",
        ):
            MapConverter(
                ColumnTypeDefinition(
                    type="MAP",
                    nullable=False,
                    key_type=ColumnTypeDefinition(type="STRING", nullable=False),
                    value_type=ColumnTypeDefinition(type="UNKNOWN", nullable=False),
                )
            )

    str_int_converter = MapConverter(
        ColumnTypeDefinition(
            type="MAP",
            nullable=False,
            key_type=ColumnTypeDefinition(type="STRING", nullable=False),
            value_type=ColumnTypeDefinition(type="INTEGER", nullable=False),
        )
    )

    def test_to_python_value_expects_list_response_value(self):
        with pytest.raises(TypeError, match="Expected list value for MapConverter"):
            self.str_int_converter.to_python_value("sdf")

    @pytest.mark.parametrize(
        "bad_value",
        [
            ["sdf"],  # is a list, but not containing interior pair lists.
            [["one", "two", "three"]],  # interior lists expected to be exactly pairs
        ],
    )
    def test_to_python_value_expects_interior_pair_lists(self, bad_value: list):
        with pytest.raises(ValueError, match="Expected key-value pair list of length 2"):
            self.str_int_converter.to_python_value(bad_value)

    @pytest.mark.parametrize(
        "key_type_name, value_type_name, from_json_payload, expected",
        [
            (
                # String -> int map
                "STRING",
                "INTEGER",
                [["one", "1"], ["two", "2"]],
                {"one": 1, "two": 2},
            ),
            (
                # Int -> string map
                "INTEGER",
                "STRING",
                [["1", "one"], ["2", "two"]],
                {1: "one", 2: "two"},
            ),
            (
                # string to nullable int
                "STRING",
                "INTEGER",
                [["one", "1"], ["two", "2"], ["null", None]],
                {"one": 1, "two": 2, "null": None},
            ),
        ],
    )
    def test_to_python_value_simple(
        self,
        key_type_name,
        value_type_name,
        from_json_payload,
        expected,
    ):
        """Test decoding maps with simple value types"""
        converter = MapConverter(
            ColumnTypeDefinition(
                type="MAP",
                nullable=True,
                key_type=ColumnTypeDefinition(type=key_type_name, nullable=False),
                value_type=ColumnTypeDefinition(type=value_type_name, nullable=False),
            )
        )
        result = converter.to_python_value(from_json_payload)
        assert result == expected

    @pytest.mark.parametrize(
        "key_type_name, value_column_type_definition, from_json_payload, expected",
        [
            (
                # Map of String to array of boolean
                "STRING",
                ColumnTypeDefinition(
                    type="ARRAY",
                    nullable=False,
                    element_type=ColumnTypeDefinition(type="BOOLEAN", nullable=False),
                ),
                [["trues", ["TRUE", "TRUE"]], ["falses", ["FALSE", "FALSE", "FALSE"]]],
                {"trues": [True, True], "falses": [False, False, False]},
            ),
            (
                # Map of string to map of string -> nullable boolean. Second interior map (horses)
                # is empty.
                "STRING",
                ColumnTypeDefinition(
                    type="MAP",
                    nullable=False,
                    key_type=ColumnTypeDefinition(type="STRING", nullable=False),
                    value_type=ColumnTypeDefinition(type="BOOLEAN", nullable=True),
                ),
                [["people", [["joe", "TRUE"], ["mary", "FALSE"], ["jane", None]]], ["horses", []]],
                {"people": {"joe": True, "mary": False, "jane": None}, "horses": {}},
            ),
            (
                # Map of int to array of maps of string to int
                "INTEGER",
                ColumnTypeDefinition(
                    type="ARRAY",
                    nullable=False,
                    element_type=ColumnTypeDefinition(
                        type="MAP",
                        nullable=False,
                        key_type=ColumnTypeDefinition(type="STRING", nullable=False),
                        value_type=ColumnTypeDefinition(type="INTEGER", nullable=False),
                    ),
                ),
                [
                    ["1", [[["a", "10"], ["b", "20"]], [["c", "30"]]]],
                    ["2", [[["d", "40"]]]],
                ],
                {1: [{"a": 10, "b": 20}, {"c": 30}], 2: [{"d": 40}]},
            ),
        ],
    )
    def test_python_value_nested_value(
        self,
        key_type_name,
        value_column_type_definition,
        from_json_payload,
        expected,
    ):
        """Test decoding maps with complex value types"""
        converter = MapConverter(
            ColumnTypeDefinition(
                type="MAP",
                nullable=True,
                key_type=ColumnTypeDefinition(type=key_type_name, nullable=False),
                value_type=value_column_type_definition,
            )
        )
        result = converter.to_python_value(from_json_payload)
        assert result == expected

    def test_to_statement_string_hates_non_dict(self):
        with pytest.raises(TypeError, match="Expected dict value"):
            MapConverter.to_statement_string("sdf")  # type: ignore

    def test_to_statement_string_hates_empty_dict(self):
        with pytest.raises(ValueError, match="Cannot convert empty dict"):
            MapConverter.to_statement_string({})  # type: ignore

    def test_to_statement_string_hates_inconsistent_key_types(self):
        with pytest.raises(ValueError, match="Expected Python integer value"):
            # mixes integer and string keys
            MapConverter.to_statement_string({1: "sdf", "fgh": "ert"})

    def test_to_statement_string_hates_inconsistent_value_types(self):
        with pytest.raises(ValueError, match="Expected Python integer value"):
            # mixes integer and string values
            MapConverter.to_statement_string({1: 1, 2: "two"})

    @pytest.mark.parametrize(
        "python_value,expected_string",
        [
            # int -> int mapping.
            ({1: 10, 2: 20}, "MAP[1, 10, 2, 20]"),
            # nullable int -> int mapping
            ({1: 10, None: -1}, "MAP[1, 10, cast (null as INTEGER), -1]"),
            # str -> nullable bool mapping
            (
                {"a": False, "b": True, "c": None},
                "MAP['a', FALSE, 'b', TRUE, 'c', cast (null as BOOLEAN)]",
            ),
            # str -> array of maps of string to int
            (
                {"a": [{"b": 12, "c": 33}, {"d": 99}]},
                "MAP['a', ARRAY[MAP['b', 12, 'c', 33], MAP['d', 99]]]",
            ),
        ],
    )
    def test_to_statement_string(self, python_value, expected_string):
        assert MapConverter.to_statement_string(python_value) == expected_string


@pytest.mark.unit
@pytest.mark.typeconv
@pytest.mark.parametrize("converter_cls", set(_flink_type_name_to_converter_map.values()))
def test_converter_wiring(converter_cls):
    """Ensure that the PRIMARY_FLINK_TYPE_NAME class attribute for the converter
    is one of the types it is registered to in _flink_type_name_to_converter_map."""
    primary_type_name = converter_cls.PRIMARY_FLINK_TYPE_NAME

    mapped_to = _flink_type_name_to_converter_map.get(primary_type_name)
    assert mapped_to == converter_cls, (
        f"Converter {converter_cls} has PRIMARY_FLINK_TYPE_NAME '{primary_type_name}' "
        f"but that type is mapped to {mapped_to} in _flink_type_name_to_converter_map."
    )


@pytest.mark.unit
@pytest.mark.typeconv
class TestSqlNone:
    """Unit tests over SqlNone functionality."""

    @pytest.mark.parametrize(
        "flink_type_name",
        [
            "TINYINT",
            "SMALLINT",
            "INTEGER",
            "BIGINT",
            "DECIMAL",
            "FLOAT",
            "DOUBLE",
            "BOOLEAN",
            "CHAR",
            "VARCHAR",
            "STRING",
            "VARBINARY",
            "DATE",
            "TIME",
            "TIMESTAMP_WITHOUT_TIME_ZONE",
            "TIMESTAMP_WITH_LOCAL_TIME_ZONE",
            "INTERVAL_DAY_TIME",
            "INTERVAL_YEAR_MONTH",
            "ARRAY",
        ],
    )
    def test_from_flink_type_name(self, flink_type_name):
        # test with both upper and lower case type names ...
        for type_name in (flink_type_name, flink_type_name.lower()):
            annotated_null = SqlNone(type_name)
            assert annotated_null._flink_type_name == flink_type_name
            assert str(annotated_null) == f"cast (null as {type_name.upper()})"

    @pytest.mark.parametrize(
        "python_type, flink_type_name",
        [
            (int, "INTEGER"),
            (Decimal, "DECIMAL"),
            (float, "DOUBLE"),
            (bool, "BOOLEAN"),
            (str, "STRING"),
            (bytes, "VARBINARY"),
            (date, "DATE"),
            (time, "TIME"),
            (datetime, "TIMESTAMP"),
            (YearMonthInterval, "INTERVAL_YEAR_MONTH"),
            (timedelta, "INTERVAL_DAY_TIME"),
            (list, "ARRAY"),
        ],
    )
    def test_from_python_type(self, python_type, flink_type_name):
        annotated_null = SqlNone(python_type)
        assert isinstance(annotated_null, SqlNone)
        assert annotated_null._flink_type_name == flink_type_name
        assert str(annotated_null) == f"cast (null as {flink_type_name})"

    @pytest.mark.parametrize(
        "flink_type_name, element_type_name",
        [
            ("ARRAY<INTEGER>", "INTEGER"),
            ("ARRAY<VARCHAR>", "VARCHAR"),
            ("ARRAY<DECIMAL>", "DECIMAL"),
            ("ARRAY<ARRAY<BOOLEAN>>", "ARRAY<BOOLEAN>"),
        ],
    )
    def test_construct_annotated_array(self, flink_type_name, element_type_name):
        """Explicit construction given ARRAY<element_type> Flink type name."""
        array_null = SqlNone(flink_type_name)
        assert str(array_null) == f"cast (null as ARRAY<{element_type_name}>)"

    def test_invalid_flink_type(self):
        with pytest.raises(
            InterfaceError,
            match="Unknown Flink type name",
        ):
            SqlNone("UNSUPPORTED_TYPE")

    def test_invalid_python_type(self):
        class UnsupportedType:
            pass

        with pytest.raises(
            InterfaceError,
            match="Cannot determine Flink SQL type name",
        ):
            SqlNone(UnsupportedType)

    @pytest.mark.parametrize(
        "const, expected",
        [
            (SqlNone.INTEGER, "cast (null as INTEGER)"),
            (SqlNone.DECIMAL, "cast (null as DECIMAL)"),
            (SqlNone.FLOAT, "cast (null as FLOAT)"),
            (SqlNone.BOOLEAN, "cast (null as BOOLEAN)"),
            (SqlNone.STRING, "cast (null as STRING)"),
            (SqlNone.VARCHAR, "cast (null as VARCHAR)"),
            (SqlNone.DATE, "cast (null as DATE)"),
            (SqlNone.TIME, "cast (null as TIME)"),
            (SqlNone.TIMESTAMP, "cast (null as TIMESTAMP)"),
            (SqlNone.VARBINARY, "cast (null as VARBINARY)"),
            (SqlNone.YEAR_MONTH_INTERVAL, "cast (null as INTERVAL YEAR TO MONTH)"),
            (SqlNone.DAY_SECOND_INTERVAL, "cast (null as INTERVAL DAYS TO SECOND)"),
        ],
    )
    def test_constants(self, const, expected):
        """Test that the predefined SqlNone constants have the expected string representations."""
        assert str(const) == expected
