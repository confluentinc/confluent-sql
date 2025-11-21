"""Unit tests over type conversion between Flink and Python types."""

from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal

import pytest

from confluent_sql.statement import ColumnTypeDefinition
from confluent_sql.types import (
    BooleanConverter,
    DateConverter,
    DecimalConverter,
    FloatConverter,
    IntegerConverter,
    NullConverter,
    StringConverter,
    TimeConverter,
    TimestampConverter,
    VarBinaryConverter,
    convert_statement_parameters,
    get_api_type_converter,
)


@pytest.mark.unit
@pytest.mark.typeconv
class TestNullConverter:
    """Unit tests over NullConverter."""

    converter = NullConverter(ColumnTypeDefinition(type="NULL", nullable=True))

    def test_to_python_value(self):
        assert self.converter.to_python_value(None) is None

    def test_to_python_value_invalid_type(self):
        with pytest.raises(
            ValueError, match="Expected None value for NullConverter but got <class 'int'>"
        ):
            self.converter.to_python_value(123)  # type: ignore

    def test_to_statement_string(self):
        result = NullConverter.to_statement_string(None)
        assert result == "NULL"

    def test_to_statement_value_invalid_type(self):
        with pytest.raises(
            ValueError,
            match="Expected Python None value for NullConverter but got <class 'int'>",
        ):
            NullConverter.to_statement_string(123)


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
            StringConverter.to_statement_string(123)

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
            VarBinaryConverter.to_statement_string("hello")


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
            IntegerConverter.to_statement_string("123")


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
            DecimalConverter.to_statement_string(123)

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

    @pytest.mark.parametrize("value, expected", [("123.5", float("123.5")), (None, None)])
    def test_to_python_value(self, value, expected):
        assert self.converter.to_python_value(value) == expected

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
            match="Expected Python float value for FloatConverter but got <class 'int'>",
        ):
            FloatConverter.to_statement_string(123)


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
            BooleanConverter.to_statement_string("TRUE")


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
            DateConverter.to_statement_string("2024-06-15")


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
            TimeConverter.to_statement_string("12:34:56.789")


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
            self.ts_converter.to_statement_string("2024-06-15 12:34:56")


@pytest.mark.unit
@pytest.mark.typeconv
class TestGetDataTypeConverter:
    """Unit tests over get_data_type_converter function."""

    @pytest.mark.parametrize(
        "column_type_name, expected_converter_cls",
        [
            # Simpler than Integer types
            ("NULL", NullConverter),
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
        (None, "NULL"),
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

    def test_to_python_value_invalid_type(self):
        converter = NullConverter(ColumnTypeDefinition(type="NULL", nullable=True))
        with pytest.raises(
            ValueError, match="Expected None value for NullConverter but got <class 'int'>"
        ):
            converter.to_python_value(123)  # type: ignore

    def test_to_statement_string(self):
        result = NullConverter.to_statement_string(None)
        assert result == "NULL"

    def test_to_statement_value_invalid_type(self):
        with pytest.raises(
            ValueError,
            match="Expected Python None value for NullConverter but got <class 'int'>",
        ):
            NullConverter.to_statement_string(123)
