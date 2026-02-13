import re

import pytest

from confluent_sql import Cursor, InterfaceError
from confluent_sql.changelog import ChangeloggedRow, FetchMetrics, RawChangelogProcessor
from confluent_sql.exceptions import (
    ComputePoolExhaustedError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)
from confluent_sql.execution_mode import ExecutionMode
from confluent_sql.statement import ChangelogRow, Op, Statement
from tests.unit.conftest import MockConnectionFactory, ResultRowFactory, StatementResponseFactory


@pytest.fixture()
def mock_connection_cursor(mock_connection_factory: MockConnectionFactory):
    """Fixture that provides a cursor from a mock connection. Closes the cursor after use."""
    mock_connection = mock_connection_factory(None, None)
    with mock_connection.closing_cursor() as mock_cursor:
        yield mock_cursor


@pytest.mark.unit
class TestExecute:
    """Unit tests over cusor.execute*()."""

    def test_executemany_throws(self, mock_connection_cursor: Cursor):
        """Test that executemany raises NotImplementedError (at this time)."""
        with pytest.raises(NotImplementedError):
            mock_connection_cursor.executemany("SELECT 1", [])

    def test_cursor_execute_deletes_prior_bounded_statement(
        self, mock_connection_cursor: Cursor, mocker
    ):
        """Prove that if executing a new statement after a bounded one, we delete the prior."""
        # Simulate that a prior statement for the cursor existed.
        mock_connection_cursor._statement = prior_statement = mocker.Mock()
        prior_statement.is_deletable = True
        prior_statement.is_deleted = False

        delete_statement_spy = mocker.spy(mock_connection_cursor, "delete_statement")

        mock_connection_cursor.execute("SELECT 1 AS col")

        delete_statement_spy.assert_called_once()

    def test_execute_prior_unbounded_statement_logs_warning(
        self, mock_connection_cursor: Cursor, mocker
    ):
        """Prove that if executing a new statement after an unbounded one, we get a warning."""
        # Simulate that the prior statement was unbounded.
        mock_connection_cursor._statement = mocker.Mock()
        mock_connection_cursor._statement.is_deleted = False  # type: ignore
        mock_connection_cursor._statement.is_deletable = False  # type: ignore

        with pytest.warns(
            UserWarning,
            match="Executing a new statement on a cursor with an existing active statement",
        ):
            mock_connection_cursor.execute("SELECT 1 AS col")

    @pytest.mark.parametrize("empty_query", ["   ", "\n", "\t", ""])
    def test_hates_empty_statement(self, mock_connection_cursor: Cursor, empty_query: str):
        """Prove that executing an empty statement raises."""
        with pytest.raises(
            ProgrammingError,
            match="SQL statement cannot be empty",
        ):
            mock_connection_cursor.execute(empty_query)

    def test_execute_non_append_only_statement(
        self, mock_connection_cursor: Cursor, statement_response_factory: StatementResponseFactory
    ):
        """Prove that executing a non-append-only statement does not raise."""
        # Mock the connection's _get_statement to return a non-append-only statement.
        non_append_only_statement_dict = statement_response_factory(is_append_only=False)
        mock_connection_cursor._connection._get_statement.return_value = (  # type: ignore
            non_append_only_statement_dict
        )

        mock_connection_cursor.execute("SELECT 1 AS col")

        # Prove that we set the changelog processor to a RawChangelogProcessor, which can handle
        # non-append-only statements.
        assert isinstance(mock_connection_cursor._changelog_processor, RawChangelogProcessor)

    def test_execute_calls_raise_if_statement_is_broken_for_failed_statement(
        self,
        mock_connection_cursor: Cursor,
        statement_response_factory: StatementResponseFactory,
        mocker,
    ):
        """Prove that _raise_if_statement_is_broken is called for a FAILED statement."""
        # Mock the connection's _get_statement to return a FAILED statement.
        failed_statement_dict = statement_response_factory(phase="FAILED", status_detail="Boom!")
        mock_connection_cursor._connection._get_statement.return_value = (  # type: ignore
            failed_statement_dict
        )

        # Mock _raise_if_statement_is_broken to raise OperationalError (its expected behavior)
        raise_if_broken_mock = mocker.patch.object(
            mock_connection_cursor,
            "_raise_if_statement_is_broken",
            side_effect=OperationalError("Statement failed"),
        )

        with pytest.raises(OperationalError):
            mock_connection_cursor.execute("SELECT 1 AS col")

        # Verify that _raise_if_statement_is_broken was called
        raise_if_broken_mock.assert_called_once()
        # Verify it was called with a Statement object
        called_statement = raise_if_broken_mock.call_args[0][0]
        assert isinstance(called_statement, Statement)
        assert called_statement.phase.name == "FAILED"

    def test_execute_calls_raise_if_statement_is_broken_for_degraded_statement(
        self,
        mock_connection_cursor: Cursor,
        statement_response_factory: StatementResponseFactory,
        mocker,
    ):
        """Prove that _raise_if_statement_is_broken is called for a DEGRADED statement."""
        # Mock the connection's _get_statement to return a DEGRADED statement.
        degraded_statement_dict = statement_response_factory(
            phase="DEGRADED", status_detail="Statement is ill"
        )
        mock_connection_cursor._connection._get_statement.return_value = (  # type: ignore
            degraded_statement_dict
        )

        # Mock _raise_if_statement_is_broken to raise OperationalError (its expected behavior)
        raise_if_broken_mock = mocker.patch.object(
            mock_connection_cursor,
            "_raise_if_statement_is_broken",
            side_effect=OperationalError("Statement degraded"),
        )

        with pytest.raises(OperationalError):
            mock_connection_cursor.execute("SELECT 1 AS col")

        # Verify that _raise_if_statement_is_broken was called
        raise_if_broken_mock.assert_called_once()
        # Verify it was called with a Statement object
        called_statement = raise_if_broken_mock.call_args[0][0]
        assert isinstance(called_statement, Statement)
        assert called_statement.phase.name == "DEGRADED"

    def test_execute_calls_raise_if_statement_is_broken_for_pool_exhausted(
        self,
        mock_connection_cursor: Cursor,
        statement_response_factory: StatementResponseFactory,
        mocker,
    ):
        """Prove that _raise_if_statement_is_broken is called for pool-exhausted statements."""
        # Mock the connection's _get_statement to return a pool-exhausted statement.
        pool_exhausted_dict = statement_response_factory(phase="PENDING")
        pool_exhausted_dict["status"]["scaling_status"]["scaling_state"] = "POOL_EXHAUSTED"
        mock_connection_cursor._connection._get_statement.return_value = (  # type: ignore
            pool_exhausted_dict
        )

        # Mock _raise_if_statement_is_broken to raise ComputePoolExhaustedError (its expected behavior)
        raise_if_broken_mock = mocker.patch.object(
            mock_connection_cursor,
            "_raise_if_statement_is_broken",
            side_effect=ComputePoolExhaustedError("Pool exhausted"),
        )

        with pytest.raises(ComputePoolExhaustedError):
            mock_connection_cursor.execute("SELECT 1 AS col")

        # Verify that _raise_if_statement_is_broken was called
        raise_if_broken_mock.assert_called_once()
        # Verify it was called with a Statement object
        called_statement = raise_if_broken_mock.call_args[0][0]
        assert isinstance(called_statement, Statement)
        assert called_statement.phase.name == "PENDING"
        assert called_statement.is_pool_exhausted

    def test_execute_statement_times_out(
        self,
        mock_connection_cursor: Cursor,
        statement_response_factory: StatementResponseFactory,
        mocker,
    ):
        """Prove that if a statement does not become ready in time, we raise."""
        # Mock the connection's _get_statement to always return a PENDING statement.
        pending_statement = statement_response_factory(phase="PENDING")

        mock_connection_cursor._connection._get_statement.return_value = (  # type: ignore
            pending_statement
        )

        # Mock out time.sleep to avoid actually waiting.
        sleep_mock = mocker.patch("time.sleep", return_value=None)

        # But must also mock out time.monotonic to simulate passage of time, say
        # each call to time.monotonic() returns +1 second
        start_time = 1000000.0
        time_mock = mocker.patch(
            "time.monotonic", side_effect=lambda: start_time + time_mock.call_count
        )

        with pytest.raises(
            OperationalError,
            match="Statement submission timed out",
        ):
            mock_connection_cursor.execute("SELECT 1 AS col")

        assert sleep_mock.called, "Expected time.sleep to have been called during wait loop."

    @pytest.mark.parametrize(
        "streaming_mode",
        [ExecutionMode.STREAMING_QUERY, ExecutionMode.STREAMING_DDL],
        ids=["streaming_query", "streaming_ddl"]
    )
    def test_streaming_ddl_workaround_for_bounded_running_bug(
        self,
        mock_connection_cursor: Cursor,
        statement_response_factory: StatementResponseFactory,
        mocker,
        streaming_mode: ExecutionMode,
    ):
        """Test the workaround for the Jan 2026 bug where streaming statements
        like CTAS are erroneously marked as bounded while in RUNNING state.

        This tests the specific workaround in _wait_for_statement_ready() lines 624-629
        that checks if execution_mode.is_streaming and statement.is_running to treat
        the statement as ready even though it's marked as bounded.

        Tests both STREAMING_QUERY and STREAMING_DDL modes since the workaround
        applies to any streaming mode (checks execution_mode.is_streaming).

        See: https://confluent.slack.com/archives/C044A8FNSJ0/p1768575045244419
        """
        # Choose appropriate SQL based on the mode
        if streaming_mode == ExecutionMode.STREAMING_DDL:
            sql_statement = "CREATE TABLE new_table AS SELECT * FROM source_table"
            sql_kind = "CREATE_TABLE_AS"
        else:  # STREAMING_QUERY
            sql_statement = "SELECT * FROM source_table"
            sql_kind = "SELECT"

        # Create a statement that is:
        # - Erroneously marked as bounded (the bug)
        # - In RUNNING phase
        statement_response = statement_response_factory(
            sql_statement=sql_statement,
            sql_kind=sql_kind,
            is_bounded=True,  # This is the bug - streaming statement marked as bounded
            phase="RUNNING",   # Statement is running
            is_append_only=True,
        )

        # Set execution mode to the streaming mode being tested
        mock_connection_cursor._execution_mode = streaming_mode

        # Mock the connection's _get_statement to return the buggy statement
        mock_connection_cursor._connection._get_statement.return_value = (  # type: ignore
            statement_response
        )

        # Mock time functions to avoid actual waiting
        mocker.patch("time.sleep", return_value=None)
        mocker.patch("time.monotonic", return_value=1000000.0)

        # Execute should succeed due to the workaround, not raise a timeout
        # The workaround treats it as ready because:
        # - execution_mode.is_streaming is True (for both STREAMING_QUERY and STREAMING_DDL)
        # - statement.is_running is True (phase="RUNNING")
        try:
            mock_connection_cursor.execute(sql_statement, timeout=5)
        except OperationalError as e:
            pytest.fail(
                f"Execute raised OperationalError despite workaround: {e}. "
                f"The workaround should treat {streaming_mode.name} in RUNNING state as ready."
            )

        # Verify the statement was set
        assert mock_connection_cursor._statement is not None
        assert mock_connection_cursor._statement.phase.name == "RUNNING"
        assert mock_connection_cursor._statement.is_bounded  # Verify the bug condition

    def test_bounded_running_without_streaming_mode_keeps_waiting(
        self,
        mock_connection_cursor: Cursor,
        statement_response_factory: StatementResponseFactory,
        mocker,
    ):
        """Test that the workaround for the bounded+RUNNING bug is NOT applied
        when NOT in streaming mode - it should keep waiting/timeout.

        This ensures the workaround is only applied in streaming mode.
        """
        # Create a statement that is bounded and RUNNING (similar to the bug case)
        bounded_running_statement = statement_response_factory(
            sql_statement="SELECT * FROM table",
            sql_kind="SELECT",
            is_bounded=True,
            phase="RUNNING",
            is_append_only=True,
        )

        # Set execution mode to SNAPSHOT (non-streaming)
        mock_connection_cursor._execution_mode = ExecutionMode.SNAPSHOT

        # Mock the connection to always return the bounded+RUNNING statement
        mock_connection_cursor._connection._get_statement.return_value = (  # type: ignore
            bounded_running_statement
        )

        # Mock time functions to simulate timeout
        mocker.patch("time.sleep", return_value=None)
        start_time = 1000000.0
        time_mock = mocker.patch(
            "time.monotonic", side_effect=lambda: start_time + time_mock.call_count * 10
        )

        # Execute should timeout because the workaround should NOT apply
        # (execution_mode.is_streaming is False for SNAPSHOT mode)
        with pytest.raises(
            OperationalError,
            match="Statement submission timed out",
        ):
            mock_connection_cursor.execute("SELECT * FROM table", timeout=5)


@pytest.mark.unit
class TestCursorInterpolatingParameters:
    @pytest.mark.parametrize(
        "parameters_iterable_type",
        [list, tuple],
    )
    def test_success(self, mock_connection_cursor: Cursor, parameters_iterable_type):
        """Test that parameters are properly interpolated into the statement template."""
        statement_template = "SELECT * FROM users WHERE id = %s AND active = %s and name = %s"

        # pass as list or tuple ...
        parameters = parameters_iterable_type([123, True, "O'Reilly"])

        interpolated_statement = mock_connection_cursor._interpolate_parameters(
            statement_template, parameters
        )

        expected_statement = (
            "SELECT * FROM users WHERE id = 123 AND active = TRUE and name = 'O''Reilly'"
        )
        assert interpolated_statement == expected_statement

    def test_too_few_param_count_error(self, mock_connection_cursor: Cursor):
        """Test that too few params passed for query template InterfaceError."""
        statement_template = "SELECT * FROM users WHERE id = %s AND active = %s"
        parameters = (123,)  # Missing second parameter

        with pytest.raises(
            ProgrammingError,
            match="Error interpolating parameters into statement: .*",
        ):
            mock_connection_cursor.execute(statement_template, parameters)

    def test_unsupported_type_error(self, mock_connection_cursor: Cursor):
        """Test that unsupported parameter type raises InterfaceError."""
        statement_template = "SELECT * FROM users WHERE id = %s"

        class UnsupportedType:
            pass

        parameters = (
            UnsupportedType(),
        )  # Random user types definitely not supported at this time.

        with pytest.raises(
            InterfaceError,
            match="Conversion for parameter of type <class .*> is not implemented.",
        ):
            mock_connection_cursor.execute(statement_template, parameters)  # type: ignore

    def test_params_cannot_be_bare_string(self, mock_connection_cursor: Cursor):
        """Test that passing a bare string as parameters raises InterfaceError."""
        statement_template = "SELECT * FROM users WHERE id = %s"
        # Incorrectly passing a bare string. Is itself an iterable, but not a list or tuple
        # of parameters.
        parameters = "not-a-tuple-or-list"

        with pytest.raises(
            TypeError,
            match="Parameters must be a tuple or list, got <class 'str'>",
        ):
            mock_connection_cursor.execute(statement_template, parameters)  # type: ignore


@pytest.mark.unit
class TestFetchMany:
    """Unit tests over cursor.fetchmany()."""

    def test_fetchmany_on_closed_cursor_raises(self, mock_connection_cursor: Cursor):
        """Test that calling fetchmany on a closed cursor raises."""
        mock_connection_cursor.close()
        with pytest.raises(InterfaceError, match="Cursor is closed"):
            mock_connection_cursor.fetchmany(size=10)

    def test_fetchmany_on_ddl_mode_raises(self, mock_connection_cursor: Cursor):
        """Test that calling fetchmany when execution mode is DDL raises."""
        mock_connection_cursor._execution_mode = ExecutionMode.SNAPSHOT_DDL
        with pytest.raises(
            InterfaceError,
            match="DDL statements do not produce result sets",
        ):
            mock_connection_cursor.fetchmany(size=10)

    def test_defaults_to_arraysize(self, mock_connection_cursor: Cursor, mocker):
        """Test that fetchmany with no size uses the cursor's arraysize."""
        expected_arraysize = 5
        mock_connection_cursor.arraysize = expected_arraysize

        changelog_processor_mock = mocker.Mock()
        changelog_processor_mock.fetchmany.return_value = []
        mocker.patch.object(
            mock_connection_cursor,
            "_get_changelog_processor",
            return_value=changelog_processor_mock,
        )

        mock_connection_cursor.fetchmany()
        changelog_processor_mock.fetchmany.assert_called_once_with(expected_arraysize)  # type: ignore

    def test_fetchmany_returns_buffered_rows_even_if_fewer_than_requested(
        self, mock_connection_cursor: Cursor, mocker
    ):
        """Test that fetchmany returns only buffered rows without fetching new pages.

        When the cursor's changelog processor has 3 rows cached and fetchmany(5)
        is called, only the 3 cached rows should be returned without triggering
        a new page fetch.
        """
        # Create mock changelog processor with 3 cached rows
        changelog_processor_mock = mocker.Mock()

        # The processor will return 3 rows when fetchmany(5) is called
        cached_rows = [("row1",), ("row2",), ("row3",)]
        changelog_processor_mock.fetchmany.return_value = cached_rows

        # Mock the _get_changelog_processor to return our mock processor
        mocker.patch.object(
            mock_connection_cursor,
            "_get_changelog_processor",
            return_value=changelog_processor_mock,
        )

        # Request 5 rows but only 3 are cached
        result = mock_connection_cursor.fetchmany(size=5)

        # Verify that fetchmany was called with size=5 on the processor
        changelog_processor_mock.fetchmany.assert_called_once_with(5)

        # Verify that only 3 rows were returned (the cached ones)
        assert result == cached_rows, (
            f"Expected only the 3 cached rows to be returned, got {result}"
        )
        assert len(result) == 3, f"Expected exactly 3 rows (what was cached), got {len(result)}"


@pytest.mark.unit
class TestCursorFetching:
    """Unit tests over cursor fetching methods."""

    def test_handles_insert_changelog_rows(
        self,
        mock_connection_factory: MockConnectionFactory,
        statement_response_factory: StatementResponseFactory,
        result_row_maker: ResultRowFactory,
    ):
        """Test that a cursor can handle changelog rows with INSERT or missing ops."""

        # Statement columns needs to match the result rows being returned.
        statement_response = statement_response_factory(
            sql_statement="SELECT 'Joe' as name, TRUE AS value",
            schema_columns=[
                {
                    "name": "name",
                    "type": {
                        "type": "STRING",
                        "nullable": False,
                    },
                },
                {
                    "name": "value",
                    "type": {
                        "type": "BOOLEAN",
                        "nullable": False,
                    },
                },
            ],
        )

        # As if statement results included only INSERT changelog rows and no next page.
        statement_results_return_value = (
            [
                result_row_maker(["Joe", "TRUE"], Op.INSERT),
                result_row_maker(["Joe", "FALSE"], None),  # implied insert.
            ],
            None,
        )

        mock_connection = mock_connection_factory(
            statement_response, statement_results_return_value
        )

        cursor = mock_connection.cursor()
        cursor.execute("SELECT true as value")

        row1 = cursor.fetchone()
        assert row1 == ("Joe", True)

        row2 = cursor.fetchone()
        assert row2 == (
            "Joe",
            False,
        )

        row3 = cursor.fetchone()
        assert row3 is None  # No more rows

    def test_fetchall_append_only_mode_works(
        self,
        mock_connection_factory: MockConnectionFactory,
        statement_response_factory: StatementResponseFactory,
        result_row_maker: ResultRowFactory,
    ):
        """Test that fetchall() collects all rows properly in append-only mode and tracks metrics"""

        # Statement columns needs to match the result rows being returned.
        statement_response = statement_response_factory(
            sql_statement="SELECT 'Joe' as name, TRUE AS value",
            schema_columns=[
                {
                    "name": "name",
                    "type": {
                        "type": "STRING",
                        "nullable": False,
                    },
                },
                {
                    "name": "value",
                    "type": {
                        "type": "BOOLEAN",
                        "nullable": False,
                    },
                },
            ],
        )

        # As if statement results included only INSERT changelog rows and no next page.
        statement_results_return_value = (
            [
                result_row_maker(["Joe", "TRUE"], Op.INSERT),
                result_row_maker(["Jane", "FALSE"], Op.INSERT),
            ],
            None,
        )

        mock_connection = mock_connection_factory(
            statement_response, statement_results_return_value
        )

        cursor = mock_connection.cursor()
        cursor.execute("SELECT name, value")

        # Verify metrics before fetching
        metrics_before = cursor.metrics
        assert isinstance(metrics_before, FetchMetrics)
        assert metrics_before.total_page_fetches == 0
        assert metrics_before.total_changelog_rows_fetched == 0

        all_rows = cursor.fetchall()
        assert all_rows == [("Joe", True), ("Jane", False)]

        # Verify metrics after fetching
        metrics_after = cursor.metrics
        assert metrics_after.total_page_fetches == 1, "Should have fetched one page"
        assert metrics_after.total_changelog_rows_fetched == 2, "Should have fetched 2 rows"
        assert metrics_after.empty_page_fetches == 0, "Should not have empty page fetches"
        assert metrics_after.avg_rows_per_page == pytest.approx(2.0), (
            "Average should be 2 rows per page"
        )

    def test_fetchall_unbounded_non_append_only_raises(
        self,
        mock_connection_factory: MockConnectionFactory,
        statement_response_factory: StatementResponseFactory,
    ):
        """Test that fetchall() raises if the statement is not bounded, because
        that's a nonsensical combination."""
        # Mock the connection's _get_statement to return a non-append-only statement.
        unbounded_non_append_only_statement_dict = statement_response_factory(
            is_append_only=False, is_bounded=False
        )
        mock_connection = mock_connection_factory(
            unbounded_non_append_only_statement_dict, None
        )  # No need to mock results for this test since should raise before fetching.

        cursor = mock_connection.cursor()
        cursor.execute("SELECT name, value")

        with pytest.raises(
            NotSupportedError,
            match=re.escape("Cannot call fetchall() on an unbounded streaming statement"),
        ):
            cursor.fetchall()

    @pytest.mark.parametrize("op", [Op.UPDATE_BEFORE, Op.UPDATE_AFTER, Op.DELETE])
    def test_raises_if_append_only_statement_produces_non_insert_changelog_rows(
        self,
        mock_connection_factory: MockConnectionFactory,
        result_row_maker: ResultRowFactory,
        statement_response_factory: StatementResponseFactory,
        op: Op,
    ):
        """Test that an error is raised on fetch*() if a statement
        produces non-insert changelog rows."""

        # By default, statement_response_factory() will return an append-only
        # statement response, which is what we want for this test. The associated
        # AppendOnlyChangelogProcessor will raise if it receives non-INSERT ops.

        # Statement columns needs to match the result rows being returned.
        statement_response = statement_response_factory(
            sql_statement="SELECT 'Joe' as name, TRUE AS value",
            schema_columns=[
                {
                    "name": "name",
                    "type": {
                        "type": "STRING",
                        "nullable": False,
                    },
                },
                {
                    "name": "value",
                    "type": {
                        "type": "BOOLEAN",
                        "nullable": False,
                    },
                },
            ],
        )

        # As if statement results included a non-insert changelog row + no next page.
        get_statement_results_return_value = (
            [
                result_row_maker(["Joe", "TRUE"], op),
            ],
            None,
        )

        mock_connection = mock_connection_factory(
            statement_response, get_statement_results_return_value
        )

        cursor = mock_connection.cursor()
        cursor.execute("SELECT true as value")

        with pytest.raises(
            NotSupportedError,
            match="Non-INSERT op was received by AppendOnlyChangelogProcessor",
        ):
            cursor.fetchone()

    @pytest.mark.parametrize("ddl_mode", [ExecutionMode.SNAPSHOT_DDL, ExecutionMode.STREAMING_DDL])
    def test_fetch_raises_if_ddl_mode(
        self,
        mock_connection_cursor: Cursor,
        ddl_mode: ExecutionMode,
    ):
        """Test that fetch*() raises if the execution mode is DDL."""
        mock_connection_cursor._execution_mode = ddl_mode

        expected_match = (
            f"Cannot fetch results in {ddl_mode}.*DDL statements do not produce result sets."
        )

        with pytest.raises(
            InterfaceError,
            match=expected_match,
        ):
            mock_connection_cursor.fetchone()

        with pytest.raises(
            InterfaceError,
            match=expected_match,
        ):
            mock_connection_cursor.fetchmany(size=10)

        with pytest.raises(
            InterfaceError,
            match=expected_match,
        ):
            mock_connection_cursor.fetchall()

    def test_raw_changelog_fetch(
        self,
        mock_connection_factory: MockConnectionFactory,
        result_row_maker: ResultRowFactory,
        statement_response_factory: StatementResponseFactory,
    ):
        """Prove that a cursor using RawChangelogProcessor can handle non-insert changelog rows,
        returning them as ChangeloggedRow containing the op + row tuple."""

        # Statement columns needs to match the result rows being returned.
        statement_response = statement_response_factory(
            sql_statement="SELECT 'Joe' as name, TRUE AS value",
            is_append_only=False,
            schema_columns=[
                {
                    "name": "name",
                    "type": {
                        "type": "STRING",
                        "nullable": False,
                    },
                },
                {
                    "name": "count",
                    "type": {
                        "type": "INTEGER",
                        "nullable": False,
                    },
                },
            ],
        )

        # As if statement results included some non-insert changelog rows + no next page.
        get_statement_results_return_value = (
            [
                result_row_maker(["Joe", "1"], Op.INSERT),
                result_row_maker(["Joe", "1"], Op.DELETE),
                result_row_maker(["Joe", "2"], Op.INSERT),
            ],
            None,
        )

        mock_connection = mock_connection_factory(
            statement_response, get_statement_results_return_value
        )

        cursor = mock_connection.cursor()
        cursor.execute("SELECT name, count(*) as count from mytab group by name")

        res1 = cursor.fetchone()
        assert isinstance(res1, ChangeloggedRow)
        assert res1.row == ("Joe", 1)
        assert res1.op == Op.INSERT

        rest = cursor.fetchmany(2)
        assert len(rest) == 2
        assert isinstance(rest[0], ChangeloggedRow)
        assert rest[0].row == ("Joe", 1)
        assert rest[0].op == Op.DELETE

        assert isinstance(rest[1], ChangeloggedRow)
        assert rest[1].row == ("Joe", 2)
        assert rest[1].op == Op.INSERT

    def test_raw_changelog_fetch_dict_cursor(
        self,
        mock_connection_factory: MockConnectionFactory,
        result_row_maker: ResultRowFactory,
        statement_response_factory: StatementResponseFactory,
    ):
        """Prove that a cursor using RawChangelogProcessor can handle non-insert changelog rows,
        returning them as ChangeloggedRow containing the op + row-as-dict."""

        # Statement columns needs to match the result rows being returned.
        statement_response = statement_response_factory(
            sql_statement="SELECT 'Joe' as name, TRUE AS value",
            is_append_only=False,
            schema_columns=[
                {
                    "name": "name",
                    "type": {
                        "type": "STRING",
                        "nullable": False,
                    },
                },
                {
                    "name": "count",
                    "type": {
                        "type": "INTEGER",
                        "nullable": False,
                    },
                },
            ],
        )

        # As if statement results included some non-insert changelog rows + no next page.
        get_statement_results_return_value = (
            [
                result_row_maker(["Joe", "1"], Op.INSERT),
                result_row_maker(["Joe", "1"], Op.DELETE),
                result_row_maker(["Joe", "2"], Op.INSERT),
            ],
            None,
        )

        mock_connection = mock_connection_factory(
            statement_response, get_statement_results_return_value
        )

        cursor = mock_connection.cursor(as_dict=True)
        cursor.execute("SELECT name, count(*) as count from mytab group by name")

        res1 = cursor.fetchone()
        assert isinstance(res1, ChangeloggedRow)
        assert res1.row == {"name": "Joe", "count": 1}
        assert res1.op == Op.INSERT

        rest = cursor.fetchmany(2)
        assert len(rest) == 2
        assert isinstance(rest[0], ChangeloggedRow)
        assert rest[0].row == {"name": "Joe", "count": 1}
        assert rest[0].op == Op.DELETE

        assert isinstance(rest[1], ChangeloggedRow)
        assert rest[1].row == {"name": "Joe", "count": 2}
        assert rest[1].op == Op.INSERT


@pytest.mark.unit
def test_close_handles_statement_delete_error(mock_connection_cursor: Cursor, mocker):
    """Test that if deleting the statement on close raises, we log but do not raise."""

    # As if had some results fetched.
    mock_connection_cursor.rowcount = 100
    mock_connection_cursor._results = [{"row": (1, 2, 3)}] * 100

    # Simulate that the prior statement was deletable.
    mock_connection_cursor._statement = mocker.Mock()
    mock_connection_cursor._statement.is_deletable = True  # type: ignore

    # Set up that the delete_statement will raise.
    delete_statement_spy = mocker.spy(mock_connection_cursor, "delete_statement")
    # Make the delete_statement raise an error.
    delete_statement_spy.side_effect = Exception("Delete failed")

    # Closing the cursor should not raise despite the delete_statement error.
    mock_connection_cursor.close()

    delete_statement_spy.assert_called_once()

    assert mock_connection_cursor.is_closed is True
    assert mock_connection_cursor.rowcount == -1
    assert mock_connection_cursor._results == []


@pytest.mark.unit
class TestIteration:
    def test_iteration_on_ddl_mode_raises(self, mock_connection_cursor: Cursor):
        """Test that iterating over the cursor when in DDL mode raises."""
        mock_connection_cursor._execution_mode = ExecutionMode.SNAPSHOT_DDL

        with pytest.raises(
            InterfaceError,
            match="DDL statements do not produce result sets",
        ):
            iter(mock_connection_cursor)

    def test_iteration_next_on_closed_cursor_raises(self, mock_connection_cursor: Cursor):
        """Test that calling next() on a closed cursor raises -- after getting the iterator"""
        it = iter(mock_connection_cursor)
        mock_connection_cursor.close()
        with pytest.raises(InterfaceError, match="Cursor is closed"):
            next(it)

    def test_iteration_success(
        self,
        mock_connection_factory: MockConnectionFactory,
        statement_response_factory: StatementResponseFactory,
    ):
        """Test that iterating over the cursor yields results as expected."""
        statement_response = statement_response_factory(
            sql_statement="SELECT 1 AS col",
            schema_columns=[
                {
                    "name": "col",
                    "type": {
                        "type": "INTEGER",
                        "nullable": False,
                    },
                },
            ],
        )

        # Simulate a single page of results with 3 rows.
        statement_results_return_value = (
            [
                ChangelogRow(Op.INSERT.value, ["1"]),
                ChangelogRow(Op.INSERT.value, ["2"]),
                ChangelogRow(Op.INSERT.value, ["3"]),
            ],
            None,
        )

        mock_connection = mock_connection_factory(
            statement_response, statement_results_return_value
        )

        cursor = mock_connection.cursor()
        cursor.execute("SELECT 1 AS col")

        # Drives iteration and exhausts the results.
        results = list(cursor)

        assert results == [(1,), (2,), (3,)]


@pytest.mark.unit
class TestCursorStatementProperty:
    """Unit tests over the cursor.statement property."""

    def test_statement_property_after_execute(
        self,
        mock_connection_cursor: Cursor,
        statement_response_factory: StatementResponseFactory,
    ):
        """Test that after executing a statement, the cursor.statement property
        reflects the statement used."""
        # Mock the connection's _get_statement to return a specific statement.
        expected_statement_dict = statement_response_factory(sql_statement="SELECT 1 AS col")
        mock_connection_cursor._connection._get_statement.return_value = (  # type: ignore
            expected_statement_dict
        )

        mock_connection_cursor.execute("SELECT 1 AS col")

        statement = mock_connection_cursor.statement

        assert statement is not None
        assert statement.name == expected_statement_dict["name"]

    def test_statement_property_no_execute_returns_none(self, mock_connection_cursor: Cursor):
        """Test that if no statement has been executed, cursor.statement is None."""
        with pytest.raises(
            InterfaceError,
            match="No statement has been executed yet",
        ):
            _ = mock_connection_cursor.statement


@pytest.mark.unit
class TestCursorResultTypeProperties:
    """Test the cursor properties for determining result types."""

    def test_as_dict_property(self, mock_connection_factory: MockConnectionFactory):
        """Test that as_dict property reflects cursor configuration."""
        mock_connection = mock_connection_factory(None, None)

        # Test with as_dict=False (default)
        cursor_tuple = mock_connection.cursor(as_dict=False)
        assert cursor_tuple.as_dict is False
        cursor_tuple.close()

        # Test with as_dict=True
        cursor_dict = mock_connection.cursor(as_dict=True)
        assert cursor_dict.as_dict is True
        cursor_dict.close()

    def test_execution_mode_property(self, mock_connection_factory: MockConnectionFactory):
        """Test that execution_mode property reflects cursor configuration."""
        mock_connection = mock_connection_factory(None, None)

        # Test snapshot mode (default)
        cursor_snapshot = mock_connection.cursor()
        assert cursor_snapshot.execution_mode == ExecutionMode.SNAPSHOT
        cursor_snapshot.close()

        # Test streaming mode
        cursor_streaming = mock_connection.cursor(mode=ExecutionMode.STREAMING_QUERY)
        assert cursor_streaming.execution_mode == ExecutionMode.STREAMING_QUERY
        cursor_streaming.close()

    def test_is_streaming_property(self, mock_connection_factory: MockConnectionFactory):
        """Test that is_streaming property correctly identifies streaming mode."""
        mock_connection = mock_connection_factory(None, None)

        # Test snapshot mode
        cursor_snapshot = mock_connection.cursor()
        assert cursor_snapshot.is_streaming is False
        assert cursor_snapshot.execution_mode == ExecutionMode.SNAPSHOT
        cursor_snapshot.close()

        # Test streaming mode
        cursor_streaming = mock_connection.cursor(mode=ExecutionMode.STREAMING_QUERY)
        assert cursor_streaming.is_streaming is True
        assert cursor_streaming.execution_mode == ExecutionMode.STREAMING_QUERY
        cursor_streaming.close()

    def test_returns_changelog_property(
        self,
        mock_connection_factory: MockConnectionFactory,
        statement_response_factory: StatementResponseFactory,
    ):
        """Test that returns_changelog property correctly identifies changelog results."""
        mock_connection = mock_connection_factory(None, None)

        # Test without statement - should be False
        cursor = mock_connection.cursor(mode=ExecutionMode.STREAMING_QUERY)
        assert cursor.returns_changelog is False

        # Execute an append-only streaming statement
        statement_dict = statement_response_factory(
            sql_statement="SELECT * FROM users", is_append_only=True, is_bounded=False
        )
        mock_connection._get_statement.return_value = statement_dict  # pyright: ignore[reportAttributeAccessIssue]
        cursor.execute("SELECT * FROM users")

        # Streaming but append-only should NOT return changelog
        assert cursor.is_streaming is True
        assert cursor.statement.is_append_only is True
        assert cursor.returns_changelog is False

        cursor.close()

        # Test non-append-only streaming statement
        cursor2 = mock_connection.cursor(mode=ExecutionMode.STREAMING_QUERY)
        statement_dict2 = statement_response_factory(
            sql_statement="SELECT user_id, COUNT(*) FROM orders GROUP BY user_id",
            is_append_only=False,  # Aggregation is not append-only
            is_bounded=False,
        )
        mock_connection._get_statement.return_value = statement_dict2  # pyright: ignore[reportAttributeAccessIssue]
        cursor2.execute("SELECT user_id, COUNT(*) FROM orders GROUP BY user_id")

        # Streaming and not append-only SHOULD return changelog
        assert cursor2.is_streaming is True
        assert cursor2.statement.is_append_only is False
        assert cursor2.returns_changelog is True

        cursor2.close()

        # Test snapshot mode never returns changelog
        cursor3 = mock_connection.cursor()  # Snapshot mode
        statement_dict3 = statement_response_factory(
            sql_statement="SELECT user_id, COUNT(*) FROM orders GROUP BY user_id",
            is_append_only=False,
            is_bounded=True,  # Snapshot query
        )
        mock_connection._get_statement.return_value = statement_dict3  # pyright: ignore[reportAttributeAccessIssue]
        cursor3.execute("SELECT user_id, COUNT(*) FROM orders GROUP BY user_id")

        # Snapshot mode should NEVER return changelog even if not append-only
        assert cursor3.is_streaming is False
        assert cursor3.statement.is_append_only is False
        assert cursor3.returns_changelog is False  # Snapshot never returns changelog

        cursor3.close()

    def test_all_four_result_type_combinations(
        self,
        mock_connection_factory: MockConnectionFactory,
        statement_response_factory: StatementResponseFactory,
    ):
        """Test all four possible result type combinations based on properties."""
        mock_connection = mock_connection_factory(None, None)

        # 1. Snapshot + tuples (standard DB-API)
        cursor1 = mock_connection.cursor(as_dict=False)
        assert cursor1.as_dict is False
        assert cursor1.is_streaming is False
        assert cursor1.returns_changelog is False
        # Result type would be: plain tuples
        cursor1.close()

        # 2. Snapshot + dicts
        cursor2 = mock_connection.cursor(as_dict=True)
        assert cursor2.as_dict is True
        assert cursor2.is_streaming is False
        assert cursor2.returns_changelog is False
        # Result type would be: plain dicts
        cursor2.close()

        # 3. Streaming changelog + tuples
        cursor3 = mock_connection.cursor(mode=ExecutionMode.STREAMING_QUERY, as_dict=False)
        statement_dict = statement_response_factory(
            sql_statement="SELECT COUNT(*) FROM orders GROUP BY user_id",
            is_append_only=False,
            is_bounded=False,
        )
        mock_connection._get_statement.return_value = statement_dict  # type: ignore
        cursor3.execute("SELECT COUNT(*) FROM orders GROUP BY user_id")
        assert cursor3.as_dict is False
        assert cursor3.is_streaming is True
        assert cursor3.returns_changelog is True
        # Result type would be: ChangeloggedRow(op=..., row=tuple)
        cursor3.close()

        # 4. Streaming changelog + dicts
        cursor4 = mock_connection.cursor(mode=ExecutionMode.STREAMING_QUERY, as_dict=True)
        cursor4.execute("SELECT COUNT(*) FROM orders GROUP BY user_id")  # Uses same mock
        assert cursor4.as_dict is True
        assert cursor4.is_streaming is True
        assert cursor4.returns_changelog is True
        # Result type would be: ChangeloggedRow(op=..., row=dict)
        cursor4.close()


@pytest.mark.unit
class TestRaiseIfStatementIsBroken:
    """Unit tests for Cursor._raise_if_statement_is_broken()."""

    def test_does_nothing_for_healthy_statement(
        self,
        mock_connection_cursor: Cursor,
        statement_factory,
    ):
        """Test that no exception is raised for a healthy statement."""
        # Create a statement that is neither failed, degraded, nor pool-exhausted
        healthy_statement = statement_factory(phase="RUNNING")

        # Should not raise any exception
        mock_connection_cursor._raise_if_statement_is_broken(healthy_statement)

    def test_raises_operational_error_for_failed_statement(
        self,
        mock_connection_cursor: Cursor,
        statement_factory,
    ):
        """Test that OperationalError is raised for a failed statement."""
        failed_statement = statement_factory(
            phase="FAILED",
            status_detail="Database connection lost",
        )

        with pytest.raises(
            OperationalError,
            match="Statement .* failed: Database connection lost",
        ):
            mock_connection_cursor._raise_if_statement_is_broken(failed_statement)

    def test_raises_operational_error_for_degraded_statement(
        self,
        mock_connection_cursor: Cursor,
        statement_factory,
    ):
        """Test that OperationalError is raised for a degraded statement."""
        degraded_statement = statement_factory(
            phase="DEGRADED",
            status_detail="Memory limit exceeded",
        )

        with pytest.raises(
            OperationalError,
            match="Statement .* is in DEGRADED state: Memory limit exceeded",
        ):
            mock_connection_cursor._raise_if_statement_is_broken(degraded_statement)

    def test_raises_compute_pool_exhausted_error_and_deletes_statement(
        self,
        mock_connection_cursor: Cursor,
        statement_response_factory: StatementResponseFactory,
        mocker,
    ):
        """Test that ComputePoolExhaustedError is raised for pool-exhausted statement
        and that the statement is deleted."""
        # Create a pool-exhausted statement response
        pool_exhausted_response = statement_response_factory(phase="PENDING")
        pool_exhausted_response["status"]["scaling_status"]["scaling_state"] = "POOL_EXHAUSTED"

        # Create the statement from response
        pool_exhausted_statement = Statement.from_response(
            mock_connection_cursor._connection,
            pool_exhausted_response,
        )

        # Spy on delete_statement to verify it's called
        delete_spy = mocker.spy(mock_connection_cursor, "delete_statement")

        with pytest.raises(
            ComputePoolExhaustedError,
            match=(
                "Statement .* was not accepted for execution due to compute pool exhaustion. "
                "The statement has been deleted. Please retry your query."
            ),
        ):
            mock_connection_cursor._raise_if_statement_is_broken(pool_exhausted_statement)

        # Verify that delete_statement was called
        delete_spy.assert_called_once()

    def test_logs_error_when_delete_fails_for_pool_exhausted(
        self,
        mock_connection_cursor: Cursor,
        statement_response_factory: StatementResponseFactory,
        mocker,
        caplog,
    ):
        """Test that an error is logged if deleting a pool-exhausted statement fails."""
        # Create a pool-exhausted statement response
        pool_exhausted_response = statement_response_factory(phase="PENDING")
        pool_exhausted_response["status"]["scaling_status"]["scaling_state"] = "POOL_EXHAUSTED"

        pool_exhausted_statement = Statement.from_response(
            mock_connection_cursor._connection,
            pool_exhausted_response,
        )

        # Mock delete_statement to raise an exception
        mocker.patch.object(
            mock_connection_cursor,
            "delete_statement",
            side_effect=Exception("Network error during delete"),
        )

        # The method should still raise ComputePoolExhaustedError even if delete fails
        with pytest.raises(
            ComputePoolExhaustedError,
            match="Statement .* was not accepted for execution due to compute pool exhaustion",
        ):
            mock_connection_cursor._raise_if_statement_is_broken(pool_exhausted_statement)

        # Check that the error was logged
        assert "Error deleting pool-exhausted statement" in caplog.text
        assert "Network error during delete" in caplog.text

    def test_pool_exhausted_check_requires_pending_phase_and_pool_exhausted_state(
        self,
        mock_connection_cursor: Cursor,
        statement_response_factory: StatementResponseFactory,
    ):
        """Test that pool exhaustion only triggers when both conditions are met:
        phase=PENDING AND scaling_state=POOL_EXHAUSTED."""

        # Test 1: RUNNING phase with POOL_EXHAUSTED scaling_state should not trigger
        running_response = statement_response_factory(phase="RUNNING")
        running_response["status"]["scaling_status"]["scaling_state"] = "POOL_EXHAUSTED"
        running_statement = Statement.from_response(
            mock_connection_cursor._connection,
            running_response,
        )
        # Should not raise (statement is not pool-exhausted because it's RUNNING)
        mock_connection_cursor._raise_if_statement_is_broken(running_statement)

        # Test 2: PENDING phase with OK scaling_state should not trigger
        pending_ok_response = statement_response_factory(phase="PENDING")
        pending_ok_response["status"]["scaling_status"]["scaling_state"] = "OK"
        pending_ok_statement = Statement.from_response(
            mock_connection_cursor._connection,
            pending_ok_response,
        )
        # Should not raise (statement is PENDING but not pool-exhausted)
        mock_connection_cursor._raise_if_statement_is_broken(pending_ok_statement)
