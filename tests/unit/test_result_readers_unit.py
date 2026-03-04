"""Unit tests for result readers module (RowFormatter family)."""

from unittest.mock import MagicMock

import pytest

from confluent_sql.exceptions import InterfaceError
from confluent_sql.execution_mode import ExecutionMode
from confluent_sql.result_readers import (
    AppendOnlyResultReader,
    ChangeloggedRow,
    DictRowFormatter,
    RowFormatter,
    TupleRowFormatter,
)
from confluent_sql.statement import Op, Schema, Statement


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


@pytest.mark.unit
class TestAppendOnlyResultReader:
    """Tests for AppendOnlyResultReader initialization and lazy formatter creation."""

    @pytest.fixture
    def mock_connection(self):
        """Create a mock connection for testing result readers."""
        return MagicMock()

    @pytest.fixture
    def mock_statement_without_schema(self):
        """Create a mock statement without schema (simulating pre-execution state)."""
        statement = MagicMock(spec=Statement)
        statement.name = "test-query"
        statement.schema = None  # Schema not yet available
        statement.type_converter = MagicMock()
        return statement

    @pytest.fixture
    def mock_statement_with_schema(self, schema_fixture):
        """Create a mock statement with schema (simulating post-execution state)."""
        statement = MagicMock(spec=Statement)
        statement.name = "test-query"
        statement.schema = schema_fixture
        statement.type_converter = MagicMock()
        return statement

    def test_initialization_with_as_dict_true_and_no_schema(
        self, mock_connection, mock_statement_without_schema
    ):
        """Test that ResultReader can be initialized with as_dict=True even when schema is None.

        This is critical because schema is populated asynchronously for DML queries.
        The formatter should be created lazily when actually needed, not at initialization.

        This test would have caught the bug where RowFormatter.create() was called
        too early in __init__(), before statement.schema was available.
        """
        # This should NOT raise an error, even though as_dict=True and schema=None
        reader = AppendOnlyResultReader(
            connection=mock_connection,
            statement=mock_statement_without_schema,
            execution_mode=ExecutionMode.SNAPSHOT,
            as_dict=True,
        )

        # Formatter should not be created yet
        assert reader._row_formatter is None
        assert reader._as_dict is True

    def test_lazy_formatter_creation_on_fetch(self, mock_connection, mock_statement_with_schema):
        """Test that the formatter is lazily created when first needed during _fetch_next_page().

        The formatter should be created on the first call to _fetch_next_page(),
        at which point statement.schema is guaranteed to be available.
        """
        # Setup mock to simulate results fetching
        mock_statement_with_schema.can_fetch_results.return_value = True
        mock_statement_with_schema.type_converter.to_python_row.return_value = (1, "test", 3.14)

        mock_connection._get_statement_results.return_value = (
            [ChangeloggedRow(Op.INSERT, (1, "test", 3.14))],
            None,  # No next page
        )

        reader = AppendOnlyResultReader(
            connection=mock_connection,
            statement=mock_statement_with_schema,
            execution_mode=ExecutionMode.SNAPSHOT,
            as_dict=True,
        )

        # Formatter should not exist yet
        assert reader._row_formatter is None

        # Call _fetch_next_page which should lazily create the formatter
        reader._fetch_next_page()

        # Now formatter should be created and be a DictRowFormatter
        assert reader._row_formatter is not None
        assert isinstance(reader._row_formatter, DictRowFormatter)

    def test_formatter_created_with_tuple_mode(self, mock_connection, mock_statement_with_schema):
        """Test that TupleRowFormatter is created when as_dict=False."""
        mock_statement_with_schema.can_fetch_results.return_value = True
        mock_statement_with_schema.type_converter.to_python_row.return_value = (1, "test", 3.14)

        mock_connection._get_statement_results.return_value = (
            [ChangeloggedRow(Op.INSERT, (1, "test", 3.14))],
            None,
        )

        reader = AppendOnlyResultReader(
            connection=mock_connection,
            statement=mock_statement_with_schema,
            execution_mode=ExecutionMode.SNAPSHOT,
            as_dict=False,
        )

        # Formatter should not exist yet
        assert reader._row_formatter is None

        # Call _fetch_next_page which should lazily create the formatter
        reader._fetch_next_page()

        # Now formatter should be created and be a TupleRowFormatter
        assert reader._row_formatter is not None
        assert isinstance(reader._row_formatter, TupleRowFormatter)
