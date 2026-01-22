import pytest

from confluent_sql import Cursor, InterfaceError
from confluent_sql.exceptions import OperationalError, ProgrammingError
from confluent_sql.execution_mode import ExecutionMode
from confluent_sql.statement import Op
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

    def test_execute_non_append_only_statement_raises(
        self, mock_connection_cursor: Cursor, statement_response_factory: StatementResponseFactory
    ):
        """Prove that executing a non-append-only statement raises NotImplementedError
        until our changelog parser were to improve."""
        # Mock the connection's _get_statement to return a non-append-only statement.
        non_append_only_statement_dict = statement_response_factory(is_append_only=False)
        mock_connection_cursor._connection._get_statement.return_value = (  # type: ignore
            non_append_only_statement_dict
        )

        with pytest.raises(
            NotImplementedError,
            match="Only append-only statements are supported for now.",
        ):
            mock_connection_cursor.execute("SELECT 1 AS col")

    def test_execute_failed_statement_raises(
        self, mock_connection_cursor: Cursor, statement_response_factory: StatementResponseFactory
    ):
        """Prove that executing a statement that ends in FAILED phase raises."""
        # Mock the connection's _get_statement to return a FAILED statement.
        failed_statement_dict = statement_response_factory(phase="FAILED", status_detail="Boom!")
        mock_connection_cursor._connection._get_statement.return_value = (  # type: ignore
            failed_statement_dict
        )

        with pytest.raises(
            OperationalError,
            match="failed: Boom!",
        ):
            mock_connection_cursor.execute("SELECT 1 AS col")

    def test_degraded_statement_raises(
        self, mock_connection_cursor: Cursor, statement_response_factory: StatementResponseFactory
    ):
        """Prove that executing a statement that ends in DEGRADED phase raises (at this time)."""
        # Mock the connection's _get_statement to return a DEGRADED statement.
        degraded_statement_dict = statement_response_factory(
            phase="DEGRADED", status_detail="Statement is ill"
        )
        mock_connection_cursor._connection._get_statement.return_value = (  # type: ignore
            degraded_statement_dict
        )

        with pytest.raises(
            OperationalError,
            match="Statement .* is in DEGRADED state: Statement is ill",
        ):
            mock_connection_cursor.execute("SELECT 1 AS col")

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

        # But must also mock out time.time to simulate passage of time, say
        # each call to time.time() returns +1 second
        start_time = 1000000.0
        time_mock = mocker.patch("time.time", side_effect=lambda: start_time + time_mock.call_count)

        with pytest.raises(
            OperationalError,
            match="Statement submission timed out",
        ):
            mock_connection_cursor.execute("SELECT 1 AS col")

        assert sleep_mock.called, "Expected time.sleep to have been called during wait loop."


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

    def test_defaults_to_arraysize(self, mock_connection_cursor: Cursor, mocker):
        """Test that fetchmany with no size uses the cursor's arraysize."""
        expected_arraysize = 5
        mock_connection_cursor.arraysize = expected_arraysize

        get_next_results_mock = mocker.Mock()
        get_next_results_mock.return_value = None
        mocker.patch.object(mock_connection_cursor, "_get_next_results")

        mock_connection_cursor.fetchmany()
        mock_connection_cursor._get_next_results.assert_called_once_with(expected_arraysize)  # type: ignore

    def test_negative_size_raises(self, mock_connection_cursor: Cursor):
        """Test that fetchmany with negative size raises."""
        with pytest.raises(InterfaceError, match="size must be a non-negative integer, got -1"):
            mock_connection_cursor.fetchmany(size=-1)


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

    @pytest.mark.parametrize("op", [Op.UPDATE_BEFORE, Op.UPDATE_AFTER, Op.DELETE])
    def test_raises_if_statement_produces_non_insert_changelog_rows(
        self,
        mock_connection_factory: MockConnectionFactory,
        result_row_maker: ResultRowFactory,
        statement_response_factory: StatementResponseFactory,
        op: Op,
    ):
        """Test that an error is raised on fetch*() if a statement
        produces non-insert changelog rows."""

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
            NotImplementedError,
            match="Only INSERT op is supported in results for now.",
        ):
            cursor.fetchone()

    def test_fetch_next_page_exception_handling(
        self,
        mock_connection_cursor: Cursor,
        mocker,
    ):
        """Test various exception handling paths in _fetch_next_page."""
        # Simulate that there is no statement.
        mock_connection_cursor._statement = None
        with pytest.raises(InterfaceError, match="No statement was used"):
            mock_connection_cursor._fetch_next_page()

        # Simulate that the statement is not ready.
        mock_statement = mocker.Mock()
        mock_statement.is_ready = False
        mock_connection_cursor._statement = mock_statement
        with pytest.raises(InterfaceError, match="Statement is not ready"):
            mock_connection_cursor._fetch_next_page()

        # No schema attached to the statement.
        mock_statement.is_ready = True
        mock_statement.schema = None
        with pytest.raises(
            InterfaceError, match="Trying to fetch results for a non-query statement"
        ):
            mock_connection_cursor._fetch_next_page()

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
