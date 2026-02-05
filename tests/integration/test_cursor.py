import re
import time

import pytest

from confluent_sql import Connection, Cursor, InterfaceError
from confluent_sql.exceptions import NotSupportedError
from confluent_sql.statement import Phase

"""A one column very fast to complete query."""
SINGLE_COLUMN_QUERY = "SELECT 42 as answer FROM `INFORMATION_SCHEMA`.`TABLES`"
# (Queries against INFORMATION_SCHEMA execute very quickly)


@pytest.mark.integration
class TestCursor:
    def test_cursor_metadata(self, cursor: Cursor):
        # 'Cursor.execute' defaults to snapshot queries
        cursor.execute(SINGLE_COLUMN_QUERY)

        assert cursor._statement is not None
        assert cursor._statement.is_bounded is True
        assert cursor._statement.phase is Phase.COMPLETED
        assert cursor._statement.name is not None
        assert cursor._statement.sql_kind == "SELECT"
        assert cursor._statement.is_append_only is True
        assert cursor._statement.description is not None
        assert len(cursor._statement.description) == 1
        assert cursor._statement.description[0][0] == "answer"

    def test_unbounded_query_with_finite_statement(self, cursor: Cursor):
        # Cursor fixture factory provides snapshot (bounded) cursors by default.
        assert cursor._execution_mode.is_snapshot is True

        cursor.execute(SINGLE_COLUMN_QUERY)

        assert cursor.statement.is_bounded is True

    @pytest.mark.slow
    def test_streaming_cursor(self, populated_table_connection: Connection, test_table_name: str):
        # For an actual unbounded query, we need to use an actual table that comes from
        # a kafka topic.
        cursor = populated_table_connection.streaming_cursor()
        # Will be an append-only unbounded query
        cursor.execute(f"SELECT * FROM {test_table_name}")
        statement = cursor.statement
        assert statement is not None
        assert statement.is_bounded is False
        assert statement.phase is Phase.RUNNING

        rows = []
        max_wait_iterations = 30  # Wait up to 30 seconds
        wait_iterations = 0
        while cursor.may_have_results and wait_iterations < max_wait_iterations:
            rows = cursor.fetchmany(10)
            if rows:
                break
            time.sleep(1)
            wait_iterations += 1

        assert len(rows) > 0, "Expected to fetch some rows from the streaming query."

        # Deleting the statement will inherently stop it. TODO need more explicit way to
        # differentiate between stopping and deleting a running statement, but that's for
        # a different test.
        cursor.delete_statement()
        cursor.close()

    @pytest.mark.slow
    def test_streaming_cursor_fetchall_raises(
        self, populated_table_connection: Connection, test_table_name: str
    ):
        """Prove that if we try to call fetchall() on an unbounded streaming statement, we get an
        error instead of hanging indefinitely."""
        cursor = populated_table_connection.streaming_cursor()
        cursor.execute(f"SELECT * FROM {test_table_name}")
        statement = cursor.statement
        assert statement is not None
        assert statement.is_bounded is False
        assert statement.phase is Phase.RUNNING

        with pytest.raises(
            NotSupportedError,
            match=re.escape("Cannot call fetchall() on an unbounded streaming statement"),
        ):
            cursor.fetchall()

        cursor.delete_statement()
        cursor.close()

    def test_cursor_description_connection_closed_raises(
        self,
        single_test_connection: Connection,
    ):
        # Test that asking for a description when the connection is closed raises an error
        cursor = single_test_connection.cursor()
        single_test_connection.close()
        with pytest.raises(InterfaceError, match="Connection is closed"):
            _ = cursor.description

    def test_cursor_description_cursor_closed_raises(self, connection):
        # Test that asking for a description when the cursor is closed raises an error
        cursor = connection.cursor()
        cursor.execute(SINGLE_COLUMN_QUERY)
        cursor.close()
        with pytest.raises(InterfaceError, match="Cursor is closed"):
            _ = cursor.description

    def test_cursor_no_statement_executed_returns_none_description(self, cursor):
        # Test that asking for a description when no statement has been executed returns None
        assert cursor.description is None

    def test_cursor_description_with_schema(self, cursor):
        cursor.execute(SINGLE_COLUMN_QUERY)
        description = cursor.description
        assert description is not None
        assert len(description) == 1
        assert description[0][0] == "answer"
        assert description[0][1] == "INTEGER"
        # display_size, internal_size, precision, scale are all None
        for idx in range(2, 6):
            assert description[0][idx] is None
        # null_ok is False
        assert description[0][6] is False

    def test_execute_after_close_raises(self, cursor):
        cursor.close()
        with pytest.raises(InterfaceError, match="Cursor is closed"):
            cursor.execute(SINGLE_COLUMN_QUERY)

    def test_delete_statement_succeeds(self, cursor, connection, mocker):
        cursor.execute(SINGLE_COLUMN_QUERY)
        statement_name = cursor._statement.name
        connection_delete_statement_spy = mocker.spy(connection, "delete_statement")

        # Delete the statement via the cursor
        cursor.delete_statement()

        # ... should cascade through to the connection
        connection_delete_statement_spy.assert_called_once_with(statement_name)

        # After deletion, the cursor's statement should remain, but smell deleted
        assert cursor._statement is not None
        assert cursor._statement.is_deleted

    def test_delete_statement_no_statement_happy(self, cursor):
        # No exception should be raised if no statement was executed.
        cursor.delete_statement()

    def test_delete_statement_cursor_closed_raises(self, cursor):
        cursor.close()
        with pytest.raises(InterfaceError, match="Cursor is closed"):
            cursor.delete_statement()

    def test_delete_statement_twice_is_noop(self, cursor):
        cursor.execute(SINGLE_COLUMN_QUERY)
        cursor.delete_statement()
        cursor.delete_statement()
        assert cursor._statement.is_deleted


@pytest.mark.integration
class TestCursorParameterInterpolation:
    def test_interpolate_with_parameters(
        self, populated_table_connection: Connection, test_table_name: str, dbname: str
    ):
        with populated_table_connection.closing_cursor(as_dict=True) as cursor:
            # Query the system catalog about the test table, using parameters
            cursor.execute(
                """
                SELECT TABLE_NAME, TABLE_SCHEMA
                FROM `INFORMATION_SCHEMA`.`TABLES`
                WHERE TABLE_NAME = %s AND TABLE_SCHEMA = %s
                """,
                (test_table_name, dbname),
            )

            results = cursor.fetchall()
            assert len(results) == 1
            row = results[0]
            assert row["TABLE_NAME"] == test_table_name  # type: ignore[index]
            assert row["TABLE_SCHEMA"] == dbname  # type: ignore[index]
