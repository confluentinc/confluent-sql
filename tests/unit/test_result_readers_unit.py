"""Unit tests for result readers module (RowFormatter family)."""

from unittest.mock import MagicMock

import pytest

from confluent_sql.exceptions import InterfaceError
from confluent_sql.result_readers import (
    DictRowFormatter,
    RowFormatter,
    TupleRowFormatter,
)
from confluent_sql.statement import Schema


@pytest.fixture
def schema_fixture():
    """Create a test schema with id, name, and value columns."""
    schema = MagicMock(spec=Schema)
    col_id = MagicMock()
    col_id.name = "id"
    col_name = MagicMock()
    col_name.name = "name"
    col_value = MagicMock()
    col_value.name = "value"
    schema.columns = [col_id, col_name, col_value]
    return schema


@pytest.mark.unit
class TestRowFormatter:
    """Tests for RowFormatter family (RowFormatter, TupleRowFormatter, DictRowFormatter)."""

    def test_create_tuple_formatter(self, schema_fixture):
        """Test creating a TupleRowFormatter via factory."""
        formatter = RowFormatter.create(as_dict=False, schema=schema_fixture)

        assert isinstance(formatter, TupleRowFormatter)

    def test_create_dict_formatter(self, schema_fixture):
        """Test creating a DictRowFormatter via factory."""
        formatter = RowFormatter.create(as_dict=True, schema=schema_fixture)

        assert isinstance(formatter, DictRowFormatter)

    def test_create_dict_formatter_requires_schema(self):
        """Test that creating a DictRowFormatter without schema raises InterfaceError."""
        with pytest.raises(InterfaceError, match="Schema required to format rows as dicts"):
            RowFormatter.create(as_dict=True, schema=None)

    def test_tuple_formatter_pass_through(self, schema_fixture):
        """Test that TupleRowFormatter returns rows unchanged."""
        formatter = RowFormatter.create(as_dict=False, schema=schema_fixture)
        row = (1, "hello", 3.14)

        result = formatter.format(row)

        assert result is row
        assert result == (1, "hello", 3.14)

    def test_dict_formatter_converts_to_dict(self, schema_fixture):
        """Test that DictRowFormatter converts tuples to dicts."""
        formatter = RowFormatter.create(as_dict=True, schema=schema_fixture)
        row = (1, "hello", 3.14)

        result = formatter.format(row)

        assert isinstance(result, dict)
        assert result == {"id": 1, "name": "hello", "value": 3.14}

    def test_dict_formatter_with_none_value(self, schema_fixture):
        """Test that DictRowFormatter handles None values in rows."""
        formatter = RowFormatter.create(as_dict=True, schema=schema_fixture)
        row = (42, None, 2.71)

        result = formatter.format(row)

        assert result == {"id": 42, "name": None, "value": 2.71}

    def test_dict_formatter_preserves_column_order(self, schema_fixture):
        """Test that DictRowFormatter maps values to correct column names."""
        formatter = RowFormatter.create(as_dict=True, schema=schema_fixture)
        # Column order: id, name, value
        row = (999, "test", 1.23)

        result = formatter.format(row)

        assert isinstance(result, dict)
        assert result["id"] == 999
        assert result["name"] == "test"
        assert abs(result["value"] - 1.23) < 1e-9  # Avoid floating point equality issues
