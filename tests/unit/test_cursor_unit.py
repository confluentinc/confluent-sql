import pytest

from confluent_sql import Connection, Cursor, InterfaceError
from confluent_sql.statement import Op
from tests.unit.conftest import ResultRowFactory


@pytest.fixture()
def mock_connection_cursor(mock_connection: Connection):
    """Fixture that provides a cursor from a mock connection. Closes the cursor after use."""
    with mock_connection.closing_cursor() as mock_cursor:
        yield mock_cursor


@pytest.mark.unit
class TestExecute:
    """Unit tests over cusor.execute*()."""

    def test_execute_with_parameters_throws(self, mock_connection_cursor: Cursor):
        """Test that executing with parameters raises NotImplementedError (at this time)."""
        with pytest.raises(NotImplementedError):
            mock_connection_cursor.execute("SELECT 1", parameters={"param": "value"})

    def test_executemany_throws(self, mock_connection_cursor: Cursor):
        """Test that executemany raises NotImplementedError (at this time)."""
        with pytest.raises(NotImplementedError):
            mock_connection_cursor.executemany("SELECT 1", [])

    def test_cursor_execute_deletes_prior_bounded_statement(
        self, mock_connection_cursor: Cursor, mocker
    ):
        """Prove that if executing a new statement after a bounded one, we delete the prior."""
        # Simulate that the prior statement was bounded.
        mock_connection_cursor._statement = mocker.Mock()
        mock_connection_cursor._statement.is_bounded = True  # type: ignore

        delete_statement_spy = mocker.spy(mock_connection_cursor, "delete_statement")

        mock_connection_cursor.execute("SELECT 1 AS col")

        delete_statement_spy.assert_called_once()

    def test_execute_prior_unbounded_statement_logs_warning(
        self, mock_connection_cursor: Cursor, mocker
    ):
        """Prove that if executing a new statement after an unbounded one, we get a warning."""
        # Simulate that the prior statement was unbounded.
        mock_connection_cursor._statement = mocker.Mock()
        mock_connection_cursor._statement.is_bounded = False  # type: ignore

        with pytest.warns(
            UserWarning,
            match="Executing a new statement on a cursor with an existing"
            " unbounded/streaming statement",
        ):
            mock_connection_cursor.execute("SELECT 1 AS col")


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
        self, mock_connection: Connection, result_row_maker: ResultRowFactory
    ):
        """Test that a cursor can handle changelog rows with INSERT or missing ops."""

        # As if statement results included only INSERT changelog rows and no next page.
        mock_connection._get_statement_results.return_value = (  # pyright: ignore[reportAttributeAccessIssue]
            [
                result_row_maker([1, "test"], Op.INSERT),
                result_row_maker([2, "example"], None),  # implied insert.
            ],
            None,
        )

        cursor = mock_connection.cursor()
        cursor.execute("SELECT 1 AS col1, 'test' AS col2")

        row1 = cursor.fetchone()
        assert row1 == (1, "test")

        row2 = cursor.fetchone()
        assert row2 == (2, "example")

        row3 = cursor.fetchone()
        assert row3 is None  # No more rows

    @pytest.mark.parametrize("op", [Op.UPDATE_BEFORE, Op.UPDATE_AFTER, Op.DELETE])
    def test_raises_if_statement_produces_non_insert_changelog_rows(
        self, mock_connection: Connection, result_row_maker: ResultRowFactory, op: Op
    ):
        """Test that an error is raised on fetch*() if a statement
        produces non-insert changelog rows."""

        # As if statement results included a non-insert changelog row + no next page.
        mock_connection._get_statement_results.return_value = (  # type: ignore
            [
                result_row_maker([1, "test"], op),
            ],
            None,
        )

        cursor = mock_connection.cursor()
        cursor.execute("SELECT 1 AS col")

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


@pytest.mark.unit
def test_close_handles_statement_delete_error(mock_connection_cursor: Cursor, mocker):
    """Test that if deleting the statement on close raises, we log but do not raise."""
    # Simulate that the prior statement was bounded.
    mock_connection_cursor._statement = mocker.Mock()
    mock_connection_cursor._statement.is_bounded = True  # type: ignore

    delete_statement_spy = mocker.spy(mock_connection_cursor, "delete_statement")
    # Make the delete_statement raise an error.
    delete_statement_spy.side_effect = Exception("Delete failed")

    # Closing the cursor should not raise despite the delete_statement error.
    mock_connection_cursor.close()
    assert mock_connection_cursor.is_closed is True
    assert mock_connection_cursor._statement is None

    delete_statement_spy.assert_called_once()
