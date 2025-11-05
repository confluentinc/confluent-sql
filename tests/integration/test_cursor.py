import pytest

from confluent_sql.exceptions import InterfaceError, ProgrammingError
from confluent_sql.statement import Phase

"""A one column very fast to complete query."""
SINGLE_COLUMN_QUERY = "SELECT 42 as answer FROM `INFORMATION_SCHEMA`.`TABLES`"
# (Queries against INFORMATION_SCHEMA execute very quickly)


def test_cursor_metadata(cursor):
    # 'Cursor.execute' defaults to snapshot queries
    cursor.execute(SINGLE_COLUMN_QUERY)

    assert cursor._statement.is_bounded is True
    assert cursor._statement.phase is Phase.COMPLETED
    assert cursor._statement.name is not None
    assert cursor._statement.sql_kind == "SELECT"
    assert cursor._statement.is_append_only is True
    assert cursor._statement.connection_refs == []
    assert cursor._statement.description is not None
    assert len(cursor._statement.description) == 1
    assert cursor._statement.description[0][0] == "answer"


def test_unbounded_query_with_finite_statement(cursor):
    # Even if we set `bounded` to False here, we still get
    # a bounded statement since it ends and no sources are
    # generating more input
    cursor.execute(SINGLE_COLUMN_QUERY, bounded=False)
    assert cursor._statement.is_bounded is True


def test_unbounded_query_with_data(populated_table_connection, test_table_name):
    # For an actual unbounded query, we need to use an actual table that comes from a kafka topic.
    cursor = populated_table_connection.cursor()
    cursor.execute(f"SELECT * FROM {test_table_name}", bounded=False)
    assert cursor._statement.is_bounded is False


def test_cursor_description_connection_closed_raises(single_test_connection):
    # Test that asking for a description when the connection is closed raises an error
    cursor = single_test_connection.cursor()
    single_test_connection.close()
    pytest.raises(InterfaceError, lambda: cursor.description)


def test_cursor_description_cursor_closed_raises(connection):
    # Test that asking for a description when the cursor is closed raises an error
    cursor = connection.cursor()
    cursor.execute(SINGLE_COLUMN_QUERY)
    cursor.close()
    with pytest.raises(InterfaceError, match="Cursor is closed"):
        _ = cursor.description


def test_cursor_no_statement_executed_returns_none_description(cursor):
    # Test that asking for a description when no statement has been executed returns None
    assert cursor.description is None


def test_cursor_description_with_schema(cursor):
    cursor.execute(SINGLE_COLUMN_QUERY)
    description = cursor.description
    assert description is not None
    assert len(description) == 1
    assert description[0][0] == "answer"
    assert description[0][1] == "INTEGER"
    for idx in range(2, 7):
        assert description[0][idx] is None


def test_execute_after_close_raises(cursor):
    cursor.close()
    with pytest.raises(InterfaceError, match="Cursor is closed"):
        cursor.execute(SINGLE_COLUMN_QUERY)


@pytest.mark.parametrize("empty_query", ["   ", "\n", "\t", ""])
def test_execute_empty_query_raises(cursor, empty_query):
    with pytest.raises(ProgrammingError, match="SQL statement cannot be empty"):
        cursor.execute(empty_query)


def test_delete_statement_succeeds(cursor, connection, mocker):
    cursor.execute(SINGLE_COLUMN_QUERY)
    statement_name = cursor._statement.name
    connection_delete_statement_spy = mocker.spy(connection, "_delete_statement")

    cursor.delete_statement()

    connection_delete_statement_spy.assert_called_once_with(statement_name)
    # After deletion, the cursor's statement should remain, but smell deleted
    assert cursor._statement is not None
    assert cursor._statement.is_deleted


def test_delete_statement_no_statement_raises(cursor):
    with pytest.raises(InterfaceError, match="No statement to delete"):
        cursor.delete_statement()


def test_delete_statement_cursor_closed_raises(cursor):
    cursor.close()
    with pytest.raises(InterfaceError, match="Cursor is closed"):
        cursor.delete_statement()


def test_delete_statement_twice_is_noop(cursor):
    cursor.execute(SINGLE_COLUMN_QUERY)
    cursor.delete_statement()
    cursor.delete_statement()
    assert cursor._statement.is_deleted
