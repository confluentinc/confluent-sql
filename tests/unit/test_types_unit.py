"""Unit tests over type conversion between Flink and Python types."""

import pytest

from confluent_sql.statement import ColumnTypeDefinition
from confluent_sql.types import (
    BooleanConverter,
    IntegerConverter,
    NullConverter,
    StringConverter,
    get_api_type_converter,
)


@pytest.mark.unit
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
            # Character string types
            ("CHAR", StringConverter),
            ("VARCHAR", StringConverter),
            ("STRING", StringConverter),
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
class TestNullConverter:
    """Unit tests over NullConverter."""

    def test_to_python_value(self):
        converter = NullConverter(ColumnTypeDefinition(type="NULL", nullable=True))
        assert converter.to_python_value(None) is None

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


@pytest.mark.unit
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
            """A string subclass that attempts to inject SQL code via overridden __str__ method."""

            def __str__(self):
                return "malicious_code()'; DROP TABLE users;--"

        malicious_value = MaliciousStr("innocent_looking_string")
        result = StringConverter.to_statement_string(malicious_value)

        # The result should be the escaped version of the poison string -- they
        # should not be able to inject code by overriding __str__.

        assert result == "'malicious_code()''; DROP TABLE users;--'"


@pytest.mark.unit
class TestIntegerConverter:
    """Unit tests over IntegerConverter."""

    @pytest.mark.parametrize("value, expected", [("123", 123), (None, None)])
    def test_to_python_value(self, value, expected):
        converter = IntegerConverter(ColumnTypeDefinition(type="INTEGER", nullable=False))
        assert converter.to_python_value(value) == expected

    def test_to_python_value_invalid_type(self):
        converter = IntegerConverter(ColumnTypeDefinition(type="INTEGER", nullable=False))
        with pytest.raises(
            ValueError,
            match="Expected integers to be encoded as JSON strings but got <class 'int'>",
        ):
            converter.to_python_value(123)  # type: ignore

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

    def test_to_statement_guard_against_malicious_subclass(self):
        """Test that to_statement_string guards against malicious subclasses of int."""

        class MaliciousInt(int):
            """An integer subclass that attempts to inject SQL code via overridden __str__ method."""

            def __str__(self):
                return "0'; DROP TABLE users;--"

        malicious_value = MaliciousInt(42)
        result = IntegerConverter.to_statement_string(malicious_value)

        # The result should be the stringified integer value -- they
        # should not be able to inject code by overriding __str__.

        assert result == "42"


@pytest.mark.unit
class TestBooleanConverter:
    """Unit tests over BooleanConverter."""

    @pytest.mark.parametrize(
        "value, expected",
        [("TRUE", True), ("FALSE", False), (None, None)],
    )
    def test_to_python_value(self, value, expected):
        converter = BooleanConverter(ColumnTypeDefinition(type="BOOLEAN", nullable=False))
        assert converter.to_python_value(value) == expected

    def test_to_python_value_invalid_type(self):
        converter = BooleanConverter(ColumnTypeDefinition(type="BOOLEAN", nullable=False))
        with pytest.raises(
            ValueError, match="Expected string value for BooleanConverter but got <class 'int'>"
        ):
            converter.to_python_value(1)  # type: ignore

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
