"""Unit tests for result readers module (RowFormatter family)."""

import time
from dataclasses import dataclass
from unittest.mock import MagicMock

import pytest

from confluent_sql.exceptions import InterfaceError, OperationalError, RequestTimeoutError
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


@pytest.fixture
def mock_connection():
    """Create a mock connection for testing."""
    return MagicMock()


@pytest.fixture
def mock_statement_with_schema(schema_fixture):
    """Create a mock statement with schema."""
    statement = MagicMock(spec=Statement)
    statement.name = "test-query"
    statement.schema = schema_fixture
    statement.type_converter = MagicMock()
    return statement


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

    def test_iteration_handles_empty_first_page_with_next_page_available(
        self, mock_connection, mock_statement_with_schema
    ):
        """Test that iteration doesn't short-circuit when the first page is empty but more pages
        exist.

        This test reproduces the bug where iteration would raise StopIteration immediately
        after fetching an empty page, even though _next_page indicates more pages are available.

        In streaming mode, the server may return an empty initial page while data is still arriving.
        The iterator should keep trying to fetch data instead of giving up prematurely.

        Scenario:
        1. First __next__() call fetches page 1: empty results, but _next_page="page_2_token"
        2. Second __next__() call should fetch page 2: has actual row data
        3. Iterator should return rows, not raise StopIteration after empty page
        """
        # Setup: Mock the server to return empty first page, then results on second page
        mock_statement_with_schema.can_fetch_results.return_value = True
        mock_statement_with_schema.type_converter.to_python_row.side_effect = [
            # Second fetch will return actual rows
            (1, "alice", 100),
            (2, "bob", 200),
        ]

        # Mock the connection's pause configuration (no pause between fetches)
        mock_connection.statement_results_page_fetch_pause_secs = 0.0

        # First call to _get_statement_results returns empty page with next_page token
        # Second call returns actual rows with no next page
        mock_connection._get_statement_results.side_effect = [
            ([], "page_2_token"),  # Empty first page, but more to come
            (
                [
                    ChangeloggedRow(Op.INSERT, (1, "alice", 100)),
                    ChangeloggedRow(Op.INSERT, (2, "bob", 200)),
                ],
                None,
            ),  # Rows on second page, no more after
        ]

        reader = AppendOnlyResultReader(
            connection=mock_connection,
            statement=mock_statement_with_schema,
            execution_mode=ExecutionMode.STREAMING_QUERY,
            as_dict=False,
        )

        # Attempt to iterate and collect all rows
        collected_rows = []
        try:
            for row in reader:
                collected_rows.append(row)
        except StopIteration:
            # If we get StopIteration before collecting rows, the bug is present
            pass

        # EXPECTED: Should have collected both rows (1, "alice", 100) and (2, "bob", 200)
        # ACTUAL WITH BUG: Iterator raises StopIteration after empty first page, never fetching
        # page 2
        assert len(collected_rows) == 2, (
            f"Expected 2 rows but got {len(collected_rows)}. "
            "Bug present: __next__() raises StopIteration after empty page, "
            "even though _next_page indicates more pages are available."
        )
        assert collected_rows[0] == (1, "alice", 100)
        assert collected_rows[1] == (2, "bob", 200)


@pytest.mark.unit
class TestIsExhausted:
    """Tests for the _is_exhausted() helper method."""

    def test_is_exhausted_when_fetch_called_and_no_next_page(
        self, mock_connection, mock_statement_with_schema
    ):
        """Test that _is_exhausted() returns True when we've fetched and there's no next page."""
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
            as_dict=False,
        )

        # Fetch once, which sets _fetch_next_page_called=True and _next_page=None
        reader._fetch_next_page()

        # Should be exhausted
        assert reader._is_exhausted() is True

    def test_is_exhausted_false_when_never_fetched(
        self, mock_connection, mock_statement_with_schema
    ):
        """Test that _is_exhausted() returns False when we've never fetched yet."""
        reader = AppendOnlyResultReader(
            connection=mock_connection,
            statement=mock_statement_with_schema,
            execution_mode=ExecutionMode.SNAPSHOT,
            as_dict=False,
        )

        # Should not be exhausted (we haven't fetched yet)
        assert reader._is_exhausted() is False

    def test_is_exhausted_false_when_next_page_available(
        self, mock_connection, mock_statement_with_schema
    ):
        """Test that _is_exhausted() returns False when there's a next page available."""
        mock_statement_with_schema.can_fetch_results.return_value = True
        mock_statement_with_schema.type_converter.to_python_row.return_value = (1, "test", 3.14)

        mock_connection._get_statement_results.return_value = (
            [ChangeloggedRow(Op.INSERT, (1, "test", 3.14))],
            "next_page_token",  # Next page available
        )

        reader = AppendOnlyResultReader(
            connection=mock_connection,
            statement=mock_statement_with_schema,
            execution_mode=ExecutionMode.SNAPSHOT,
            as_dict=False,
        )

        # Fetch once, which sets _fetch_next_page_called=True and _next_page="next_page_token"
        reader._fetch_next_page()

        # Should not be exhausted (more pages available)
        assert reader._is_exhausted() is False


@pytest.mark.unit
class TestBlockingIterationToleratesPageTimeouts:
    """One slow results GET (an httpx timeout, surfaced as RequestTimeoutError) is "no news yet" to
    a reader that is blocking by contract -- iteration, fetchall(), snapshot fetches -- not a
    failure (#236's shape, for result pages). The non-blocking streaming fetch path makes at most
    one request and keeps surfacing the error, since its caller owns the polling loop."""

    PAGE_TOKEN = "page-2"

    @staticmethod
    def _rows(*ids: int) -> list[ChangeloggedRow]:
        return [ChangeloggedRow(Op.INSERT, (i, f"n{i}", i * 100)) for i in ids]

    def _reader(self, mock_connection, mock_statement_with_schema, mode, *pages):
        """A reader whose result-page fetches replay `pages`: each an exception to raise or a
        (rows, next_page) result to return."""
        mock_statement_with_schema.can_fetch_results.return_value = True
        mock_statement_with_schema.type_converter.to_python_row.side_effect = lambda row: row
        mock_connection.statement_results_page_fetch_pause_secs = 0.0
        mock_connection.timeouts.statement_results_response_wallclock_secs = self.WINDOW_SECS
        mock_connection._get_statement_results.side_effect = list(pages)
        return AppendOnlyResultReader(
            connection=mock_connection,
            statement=mock_statement_with_schema,
            execution_mode=mode,
            as_dict=False,
        )

    def test_iteration_survives_a_timed_out_page_and_refetches_the_same_page(
        self, mock_connection, mock_statement_with_schema
    ):
        reader = self._reader(
            mock_connection,
            mock_statement_with_schema,
            ExecutionMode.STREAMING_QUERY,
            ([], self.PAGE_TOKEN),
            RequestTimeoutError("slow"),
            (self._rows(1, 2), None),
        )

        assert [row for row in reader] == [(1, "n1", 100), (2, "n2", 200)]

        calls = mock_connection._get_statement_results.call_args_list
        assert [c.args for c in calls] == [
            ("test-query", None),
            ("test-query", self.PAGE_TOKEN),
            ("test-query", self.PAGE_TOKEN),
        ]

    # The tolerance is a wall-clock window, not an attempt count: how many slow pages fit in it
    # depends only on how long each one took.
    WINDOW_SECS = 30.0

    @dataclass
    class _SlowPage:
        """A page fetch that takes `secs` of (fake) time and then times out."""

        secs: float
        error: RequestTimeoutError

    @pytest.fixture(autouse=True)
    def fake_clock(self, monkeypatch):
        clock = [1000.0]
        monkeypatch.setattr(time, "monotonic", lambda: clock[0])
        return clock

    def _timing_out_reader(self, mock_connection, mock_statement_with_schema, clock, mode, *pages):
        """Like `_reader`, but a `_SlowPage` advances the fake clock before raising."""
        replay = iter(pages)
        self.fetch_remaining_secs: list[float | None] = []

        def fetch(*_args, ctx):
            self.fetch_remaining_secs.append(ctx.remaining_secs())
            page = next(replay)
            if isinstance(page, self._SlowPage):
                clock[0] += page.secs
                raise page.error
            return page

        reader = self._reader(mock_connection, mock_statement_with_schema, mode)
        mock_connection._get_statement_results.side_effect = fetch
        return reader

    def _slow(self, count: int, secs: float) -> list:
        return [self._SlowPage(secs, RequestTimeoutError(f"slow-{i}")) for i in range(count)]

    def test_many_quick_timeouts_inside_the_window_are_tolerated(
        self, mock_connection, mock_statement_with_schema, fake_clock
    ):
        # Ten timeouts in a row would have blown the old three-attempt limit.
        reader = self._timing_out_reader(
            mock_connection,
            mock_statement_with_schema,
            fake_clock,
            ExecutionMode.SNAPSHOT,
            *self._slow(10, 1.0),
            (self._rows(1), None),
        )

        assert list(reader) == [(1, "n1", 100)]

    def test_timeouts_spanning_the_window_raise_the_last_one(
        self, mock_connection, mock_statement_with_schema, fake_clock
    ):
        slow = self._slow(5, 10.0)
        reader = self._timing_out_reader(
            mock_connection, mock_statement_with_schema, fake_clock, ExecutionMode.SNAPSHOT, *slow
        )

        with pytest.raises(RequestTimeoutError) as exc_info:
            next(reader)

        # 10s + 10s + 10s reaches the 30s window on the third slow page.
        assert exc_info.value is slow[2].error
        assert mock_connection._get_statement_results.call_count == 3

    def test_every_fetch_in_a_run_is_bounded_by_the_runs_deadline(
        self, mock_connection, mock_statement_with_schema, fake_clock
    ):
        """The window is a `Context` handed to each page fetch, so the request itself is capped at
        the time left: a fetch started near the end of the window cannot run past it."""
        reader = self._timing_out_reader(
            mock_connection,
            mock_statement_with_schema,
            fake_clock,
            ExecutionMode.SNAPSHOT,
            *self._slow(2, 10.0),
            (self._rows(1), None),
        )

        assert list(reader) == [(1, "n1", 100)]
        assert self.fetch_remaining_secs == [self.WINDOW_SECS, 20.0, 10.0]

    def test_a_response_starts_a_fresh_window_for_the_next_fetch(
        self, mock_connection, mock_statement_with_schema, fake_clock
    ):
        reader = self._timing_out_reader(
            mock_connection,
            mock_statement_with_schema,
            fake_clock,
            ExecutionMode.STREAMING_QUERY,
            *self._slow(1, 10.0),
            ([], self.PAGE_TOKEN),  # an empty page is still a response: it ends the run
            *self._slow(1, 10.0),
            (self._rows(1), None),
        )

        assert list(reader) == [(1, "n1", 100)]
        assert self.fetch_remaining_secs == [self.WINDOW_SECS, 20.0, self.WINDOW_SECS, 20.0]

    def test_a_successful_page_restarts_the_window(
        self, mock_connection, mock_statement_with_schema, fake_clock
    ):
        streak = self._slow(2, 10.0)  # 20s of the 30s window, twice over
        reader = self._timing_out_reader(
            mock_connection,
            mock_statement_with_schema,
            fake_clock,
            ExecutionMode.STREAMING_QUERY,
            *streak,
            ([], self.PAGE_TOKEN),  # an empty page still counts as a successful fetch
            *streak,
            (self._rows(1), None),
        )

        assert list(reader) == [(1, "n1", 100)]

    def test_snapshot_fetchmany_and_fetchall_block_through_a_timeout(
        self, mock_connection, mock_statement_with_schema
    ):
        reader = self._reader(
            mock_connection,
            mock_statement_with_schema,
            ExecutionMode.SNAPSHOT,
            RequestTimeoutError("slow"),
            (self._rows(1, 2, 3), None),
        )

        assert reader.fetchmany(2) == [(1, "n1", 100), (2, "n2", 200)]
        assert reader.fetchall() == [(3, "n3", 300)]

    def test_other_operational_errors_are_not_tolerated(
        self, mock_connection, mock_statement_with_schema
    ):
        reader = self._reader(
            mock_connection,
            mock_statement_with_schema,
            ExecutionMode.SNAPSHOT,
            OperationalError("boom"),
            (self._rows(1), None),
        )

        with pytest.raises(OperationalError, match="boom") as exc_info:
            next(reader)

        assert not isinstance(exc_info.value, RequestTimeoutError)
        assert mock_connection._get_statement_results.call_count == 1

    def test_nonblocking_streaming_fetch_still_surfaces_the_timeout_and_stays_retryable(
        self, mock_connection, mock_statement_with_schema
    ):
        reader = self._reader(
            mock_connection,
            mock_statement_with_schema,
            ExecutionMode.STREAMING_QUERY,
            RequestTimeoutError("slow"),
            (self._rows(1), None),
        )

        with pytest.raises(RequestTimeoutError):
            reader.fetchone()
        assert mock_connection._get_statement_results.call_count == 1  # one request, no retry

        assert reader.fetchone() == (1, "n1", 100)  # the caller's own retry works: state intact
