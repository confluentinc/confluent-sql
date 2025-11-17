"""Unit tests over type conversion between Flink and Python types."""

import pytest

from confluent_sql.statement import ColumnTypeDefinition
from confluent_sql.types import (
    BooleanConverter,
    IntegerConverter,
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
            ("BOOLEAN", BooleanConverter),
            # Integer types
            ("TINYINT", IntegerConverter),
            ("SMALLINT", IntegerConverter),
            ("INTEGER", IntegerConverter),
            ("INT", IntegerConverter),
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


@pytest.mark.unit
class TestIntegerConverter:
    """Unit tests over IntegerConverter."""

    @pytest.mark.parametrize("value, expected", [("123", 123), (None, None)])
    def test_to_python_value(self, value, expected):
        converter = IntegerConverter(ColumnTypeDefinition(type="INT", nullable=False))
        assert converter.to_python_value(value) == expected

    def test_to_python_value_invalid_type(self):
        converter = IntegerConverter(ColumnTypeDefinition(type="INT", nullable=False))
        with pytest.raises(
            ValueError,
            match="Expected integers to be encoded as JSON strings but got <class 'int'>",
        ):
            converter.to_python_value(123)  # type: ignore


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
