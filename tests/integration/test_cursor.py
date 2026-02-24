import re
import time
from uuid import uuid4

import pytest

from confluent_sql import Connection, Cursor, InterfaceError, StatementDeletedError
from confluent_sql.exceptions import NotSupportedError
from confluent_sql.statement import Op, Phase

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

    def test_cursor_execute_and_find_with_label(self, cursor: Cursor, connection: Connection):
        """Test over submitting and finding statements via end-user-provided label."""
        label = f"test-label-{uuid4()}"
        name = f"test-statement-{uuid4()}".lower()
        cursor.execute(SINGLE_COLUMN_QUERY, statement_name=name, statement_label=label)

        statement = cursor._statement
        assert statement is not None
        assert label in statement.end_user_labels

        # Should be able to list statements by label and find it.
        statements = connection.list_statements(label=label)

        # Should find the statement with same name by its label being present.
        # Should have found exactly one.
        assert len(statements) == 1
        assert statements[0].name == statement.name

        # But not find any statements if we filter by a different label.
        statements = connection.list_statements(label="some_other_label")
        assert all(s.name != statement.name for s in statements)

        # Cleanup.
        connection.delete_statement(statement)

    def test_cursor_description_raises_if_closed(self, cursor: Cursor):
        cursor.close()
        with pytest.raises(InterfaceError, match="Cursor is closed"):
            _ = cursor.description

    def test_cursor_description_none_if_no_statement(self, cursor: Cursor):
        assert cursor.description is None

    def test_cursor_description_after_execution(self, cursor: Cursor):
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

    def test_unbounded_query_with_finite_statement(self, cursor: Cursor):
        # Cursor fixture factory provides snapshot (bounded) cursors by default.
        assert cursor._execution_mode.is_snapshot is True

        cursor.execute(SINGLE_COLUMN_QUERY)

        assert cursor.statement.is_bounded is True

    @pytest.mark.slow
    def test_streaming_append_only_cursor(
        self, populated_table_connection: Connection, test_table_name: str
    ):
        # For an actual unbounded query, we need to use an actual table that comes from
        # a kafka topic.
        cursor = populated_table_connection.streaming_cursor()
        # Will be an append-only unbounded query
        cursor.execute(f"SELECT * FROM {test_table_name}")
        statement = cursor.statement
        assert statement is not None
        assert statement.is_bounded is False
        assert statement.is_append_only is True
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
    def test_streaming_changelog_cursor(
        self,
        populated_table_connection: Connection,
        test_table_name: str,
        populated_table_rowcount: int,
    ):
        cursor = populated_table_connection.streaming_cursor()
        # Will be a retractable changelog unbounded query. It will emit an INSERT
        # result set changelog row when the first message is observed, initial count of 1,
        # then repeatedly revise that single row with UPDATE_BEFORE and UPDATE_AFTER pairs.

        # The final observed count will be the last UPDATE_AFTER row's count value.

        cursor.execute(f"SELECT count(*) as `count` FROM {test_table_name}")
        statement = cursor.statement
        assert statement is not None
        assert statement.is_bounded is False
        assert statement.is_append_only is False
        assert statement.phase is Phase.RUNNING

        max_wait_iterations = 20  # Do up to this many iterations of fetchmany() loop.
        wait_iterations = 0

        the_count = 0

        had_insert = False
        had_update_before = False
        had_update_after = False

        # Keep consuming until we observe the expected final count.
        while cursor.may_have_results and wait_iterations < max_wait_iterations:
            # Will return a batch of up to 10 rows, where each row is a tuple of (operation, data),
            # where operation is one of Op.INSERT, Op.UPDATE_BEFORE, Op.UPDATE_AFTER, and data is
            # the row data for that operation.

            # If no rows are currently available, will return an empty list, but
            # may_have_results will still be True, so we can keep trying until we get some rows.
            rows = cursor.fetchmany(10)

            # Process each row in the batch
            for op_and_row in rows:
                op, row = op_and_row
                if op == Op.INSERT:
                    had_insert = True
                    the_count = row[0]  # type: ignore[index]
                elif op == Op.UPDATE_BEFORE:
                    had_update_before = True
                    # Will come in pairs with UPDATE_AFTER, so we don't need to do anything
                    # with the count here.
                elif op == Op.UPDATE_AFTER:
                    had_update_after = True
                    # Update the count.
                    the_count = row[0]  # type: ignore[index]

                # {test_table_name} is populated with a total of populated_table_rowcount
                # rows by the test fixture.
                if the_count == populated_table_rowcount:
                    break

            # Break outer loop if we found the expected count.
            # (A real client of a streaming query will probably want to loop forever,
            #  but we're just a test here.)
            if the_count == populated_table_rowcount:
                break

            wait_iterations += 1

        assert had_insert, "Expected to observe an INSERT changelog operation."
        assert had_update_before, "Expected to observe an UPDATE_BEFORE changelog operation."
        assert had_update_after, "Expected to observe an UPDATE_AFTER changelog operation."
        assert the_count == populated_table_rowcount, (
            f"Expected final count to be {populated_table_rowcount}, got {the_count}."
        )

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

    @pytest.mark.slow
    def test_streaming_ddl_create_table_completes(self, connection: Connection):
        """Test that executing CREATE TABLE in streaming mode waits for completion.

        This validates the fix for streaming DDL statements: pure DDL statements
        (like CREATE TABLE) in streaming mode should wait for the statement to
        reach a terminal phase (COMPLETED) before returning to the caller, not
        just return when RUNNING.

        After the CREATE TABLE completes, the table should be immediately usable.
        """
        # Create a unique table name for this test (use underscores instead of hyphens)
        table_name = f"test_streaming_ddl_{uuid4().hex[:8]}"

        # Create the table using streaming cursor mode
        # This should wait for the statement to reach COMPLETED phase
        cursor = connection.streaming_cursor()
        cursor.execute(f"CREATE TABLE {table_name} (id INT, name STRING)")
        statement = cursor.statement
        assert statement is not None

        # Verify the statement has completed (not just RUNNING)
        assert statement.phase == Phase.COMPLETED
        assert statement.is_pure_ddl is True

        # Verify the table exists and is usable by querying it
        verify_cursor = connection.cursor()
        verify_cursor.execute(f"SELECT COUNT(*) as row_count FROM {table_name}")
        results = verify_cursor.fetchall()
        assert len(results) == 1  # Should have one row with count
        verify_cursor.close()
        cursor.close()

        # Cleanup: Drop the created table
        cleanup_cursor = connection.cursor()
        cleanup_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        cleanup_cursor.close()

        # Delete the statement
        connection.delete_statement(statement)


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


@pytest.mark.integration
class TestStreamingChangelogCursor:
    @pytest.mark.slow
    @pytest.mark.parametrize("as_dict", [False, True])
    def test_changelog_compressor_with_upsert_columns(
        self,
        populated_table_connection: Connection,
        test_table_name: str,
        as_dict: bool,
    ):
        """Test changelog compressor with a GROUP BY query that has upsert columns.

        The query will ultimately produce two rows:
        - (0, 5) or {"even_odd": 0, "cnt": 5} for even values (2, 4, 6, 8, 10)
        - (1, 5) or {"even_odd": 1, "cnt": 5} for odd values (1, 3, 5, 7, 9)

        The upsert column will be index 0 (even_odd), and snapshots() should
        progressively update the counts until reaching the final state of 5 and 5.

        Tests both tuple and dict result modes.
        """

        # Define accessors based on result mode
        def get_even_odd(row):
            return row["even_odd"] if as_dict else row[0]  # type: ignore[index]

        def get_count(row):
            return row["cnt"] if as_dict else row[1]  # type: ignore[index]

        def make_expected(eo, cnt):
            return {"even_odd": eo, "cnt": cnt} if as_dict else (eo, cnt)

        cursor = populated_table_connection.streaming_cursor(as_dict=as_dict)

        cursor.execute(
            f"SELECT c1 % 2 as even_odd, count(*) as cnt FROM {test_table_name} GROUP BY c1 % 2"
        )

        statement = cursor.statement

        assert statement is not None
        assert statement.is_bounded is False
        assert statement.is_append_only is False
        assert statement.phase is Phase.RUNNING

        # Verify upsert columns trait
        assert statement.traits is not None
        assert statement.traits.upsert_columns == [0]

        # Create changelog compressor
        compressor = cursor.changelog_compressor()

        max_iterations = 20
        final_snapshot = None

        # Iterate over snapshots until we see the expected final state
        for iteration, snapshot in enumerate(compressor.snapshots()):
            # print(f"Iteration {iteration}: snapshot = {snapshot}")
            # Expect to have gotten between 0 and 2 rows in the snapshot, depending on how far
            # along we are in processing the changelog.
            assert len(snapshot) in (0, 1, 2), (
                f"Expected snapshot to have 0, 1, or 2 rows, got {len(snapshot)}"
            )

            if len(snapshot) == 2:
                # Sort by even_odd value for consistent ordering
                snapshot_sorted = sorted(snapshot, key=get_even_odd)

                even_odd_0 = get_even_odd(snapshot_sorted[0])
                count_0 = get_count(snapshot_sorted[0])
                even_odd_1 = get_even_odd(snapshot_sorted[1])
                count_1 = get_count(snapshot_sorted[1])

                assert even_odd_0 == 0, "First row should be for even values (0)"
                assert even_odd_1 == 1, "Second row should be for odd values (1)"

                if count_0 == 5 and count_1 == 5:
                    final_snapshot = snapshot_sorted
                    break

            if iteration >= max_iterations - 1:
                break
            time.sleep(1)

        assert final_snapshot is not None, (
            f"Expected to reach final state with counts (5, 5) within {max_iterations} iterations"
        )
        assert len(final_snapshot) == 2
        assert final_snapshot[0] == make_expected(0, 5)
        assert final_snapshot[1] == make_expected(1, 5)

        # Cleanup
        cursor.delete_statement()

    @pytest.mark.slow
    @pytest.mark.parametrize("as_dict", [False, True])
    def test_changelog_compressor_without_upsert_columns(
        self,
        populated_table_connection: Connection,
        test_table_name: str,
        as_dict: bool,
    ):
        """Test changelog compressor with global aggregation query without upsert columns.

        The query will ultimately produce a single row:
        - (1, 10, 10) or {"min_val": 1, "max_val": 10, "cnt": 10}

        This tests NoUpsertColumnsCompressor which uses linear scan to find rows
        for UPDATE_BEFORE operations since there are no upsert columns to use as keys.

        Tests both tuple and dict result modes.
        """

        # Define accessors based on result mode
        def get_min(row):
            return row["min_val"] if as_dict else row[0]  # type: ignore[index]

        def get_max(row):
            return row["max_val"] if as_dict else row[1]  # type: ignore[index]

        def get_count(row):
            return row["cnt"] if as_dict else row[2]  # type: ignore[index]

        def make_expected(min_val, max_val, cnt):
            if as_dict:
                return {"min_val": min_val, "max_val": max_val, "cnt": cnt}
            else:
                return (min_val, max_val, cnt)

        cursor = populated_table_connection.streaming_cursor(as_dict=as_dict)
        cursor.execute(
            f"SELECT min(c1) as min_val, max(c1) as max_val, count(*) as cnt FROM {test_table_name}"
        )

        statement = cursor.statement
        assert statement is not None
        assert statement.is_bounded is False
        assert statement.is_append_only is False
        assert statement.phase is Phase.RUNNING

        # Verify NO upsert columns (global aggregation)
        assert statement.traits is not None
        assert statement.traits.upsert_columns is None

        # Create changelog compressor
        compressor = cursor.changelog_compressor()

        max_iterations = 20
        final_snapshot = None

        # Iterate over snapshots until we see the expected final state
        for iteration, snapshot in enumerate(compressor.snapshots()):
            # Expect to have 0 or 1 row in the snapshot (global aggregation = single row)
            assert len(snapshot) in (0, 1), (
                f"Expected snapshot to have 0 or 1 row, got {len(snapshot)}"
            )

            if len(snapshot) == 1:
                row = snapshot[0]
                min_val = get_min(row)
                max_val = get_max(row)
                cnt = get_count(row)

                # Check if we've reached final state
                if min_val == 1 and max_val == 10 and cnt == 10:
                    final_snapshot = snapshot
                    break

            if iteration >= max_iterations - 1:
                break
            time.sleep(1)

        assert final_snapshot is not None, (
            f"Expected to reach final state (1, 10, 10) within {max_iterations} iterations"
        )
        assert len(final_snapshot) == 1
        assert final_snapshot[0] == make_expected(1, 10, 10)

        # Cleanup
        cursor.delete_statement()

    @pytest.mark.slow
    def test_changelog_compressor_raises_on_external_deletion(
        self,
        populated_table_connection: Connection,
        test_table_name: str,
    ):
        """Test that snapshots() raises StatementDeletedError when statement is externally deleted.

        This test verifies that if a streaming statement is deleted while iterating over
        snapshots, the generator raises StatementDeletedError (a subclass of OperationalError)
        on the next fetch attempt.
        """
        cursor = populated_table_connection.streaming_cursor()
        cursor.execute(
            f"SELECT c1 % 2 as even_odd, count(*) as cnt FROM {test_table_name} GROUP BY c1 % 2"
        )

        statement = cursor.statement
        assert statement is not None
        assert statement.name is not None
        statement_name = statement.name

        # Create changelog compressor
        compressor = cursor.changelog_compressor()

        # Get first snapshot successfully
        snapshot_iter = compressor.snapshots()
        first_snapshot = next(snapshot_iter)

        # Verify we got a valid snapshot (may be empty or have data depending on timing)
        assert isinstance(first_snapshot, list)

        # Now explicitly delete the statement using the connection API
        populated_table_connection.delete_statement(statement_name)

        # The next attempt to get a snapshot should raise StatementDeletedError
        # when fetching results (404 from the server)
        with pytest.raises(StatementDeletedError) as exc_info:
            next(snapshot_iter)
            # If we get here, the generator did not raise - it might have returned
            # another snapshot. Try one more time to ensure we hit the deletion.
            next(snapshot_iter)

        # Verify the exception details
        assert exc_info.value.statement_name == statement_name
        assert statement_name in str(exc_info.value)

        # Verify the statement is actually deleted by trying to get statement status
        with pytest.raises(StatementDeletedError):
            populated_table_connection._get_statement_results(statement_name, None)

    # NOTE: We will add a test for StatementStoppedError (the base exception) in the future
    # when we implement an explicit stop_statement() operation. That test will verify that
    # stopping a statement (without deleting it) raises StatementStoppedError with the
    # appropriate phase information, allowing users to inspect why the statement stopped.

    @pytest.mark.slow
    def test_streaming_bounded_changelog_query(
        self,
        connection: Connection,
    ):
        """Test submitting a bounded non-append-only query in streaming mode.

        This test verifies that bounded queries submitted in streaming mode with changelog
        semantics (non-append-only) are properly handled. The query uses aggregation on
        static data (not from streaming sources), resulting in a bounded statement that
        must complete before results are available.

        The fix ensures that _wait_for_statement_ready() waits for bounded non-append-only
        statements to reach a terminal phase before considering them ready, preventing
        "Statement is not ready for result fetching" race conditions.
        """
        cursor: Cursor | None = None
        try:
            cursor = connection.streaming_cursor(as_dict=True)

            query = """
                select
                    coalesce(sum(failures), 0) as failures,
                    coalesce(sum(failures), 0) <> 0 as should_warn,
                    coalesce(sum(failures), 0) <> 0 as should_error
                from (
                    select count(*) as failures from (
                        select * from ( select 1 as id ) as my_subquery where id = 2
                    ) dbt_internal_test
                    union all
                    select cast(0 as bigint) as failures
                ) dbt_internal_test_with_fallback
            """

            cursor.execute(query)

            statement = cursor.statement
            assert statement is not None
            # Despite being submitted as streaming, this particular query will
            # complete as a bounded since it doesn't read from any streaming sources.
            assert statement.is_bounded
            assert (
                not statement.is_append_only
            )  # Will be a retractable changelog due to the count(*) aggregation.
            assert statement.phase in [Phase.RUNNING, Phase.COMPLETED]

            # Use changelog compressor to get the final result snapshot
            compressor = cursor.changelog_compressor()
            max_iterations = 30
            final_snapshot = None

            for iteration, snapshot in enumerate(compressor.snapshots()):
                # The query returns a single row with the aggregation result
                if len(snapshot) == 1:
                    final_snapshot = snapshot
                    break

                if iteration >= max_iterations - 1:
                    break
                time.sleep(1)

            assert final_snapshot is not None, (
                "Expected to get a final snapshot from the changelog compressor."
            )
            assert len(final_snapshot) == 1, (
                f"Expected 1 row in snapshot, got {len(final_snapshot)}"
            )
            result_row = final_snapshot[0]
            assert result_row["failures"] == 0, (
                f"Expected failures == 0, got {result_row['failures']}"
            )  # type: ignore[index]

        finally:
            if cursor is not None:
                # Cleanup
                cursor.delete_statement()
                cursor.close()
