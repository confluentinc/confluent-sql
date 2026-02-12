import re
from unittest.mock import patch

import pytest

from confluent_sql.changelog import (
    AppendOnlyChangelogProcessor,
    ChangeloggedRow,
    FetchMetrics,
    RawChangelogProcessor,
)
from confluent_sql.connection import Connection
from confluent_sql.exceptions import InterfaceError, NotSupportedError
from confluent_sql.execution_mode import ExecutionMode
from confluent_sql.statement import ChangelogRow, Op, Phase
from tests.unit.conftest import StatementFactory


@pytest.fixture
def append_only_processor(
    statement_factory: StatementFactory, mock_connection: Connection
) -> AppendOnlyChangelogProcessor:
    """A fixture that returns an AppendOnlyChangelogProcessor instance in streaming mode."""
    statement = statement_factory(is_append_only=True)
    return AppendOnlyChangelogProcessor(
        mock_connection, statement, ExecutionMode.STREAMING_QUERY
    )


"""
 Tests over AppendOnlyChangelogProcessor
"""


@pytest.mark.unit
@pytest.mark.parametrize(
    "next_page_called, remaining, next_page, expected",
    [
        (False, None, None, True),  # Initial state, no results fetched yet, so may have results
        (True, 5, None, True),  # Fetched a page and there are remaining results
        (
            True,
            0,
            "next_page_token",
            True,
        ),  # Fetched a page, no remaining results, but there is a next page token
        (True, 0, None, False),  # Fetched a page, no remaining results, and no next page token
    ],
)
def test_may_have_results(
    append_only_processor: AppendOnlyChangelogProcessor,
    next_page_called: bool,
    remaining: int | None,
    next_page: str | None,
    expected: bool,
):
    append_only_processor._fetch_next_page_called = next_page_called
    if remaining is not None:
        append_only_processor._results = [("mock_row",)] * remaining  # Mock some remaining results
        append_only_processor._index = 0  # Reset index to start of results
    append_only_processor._next_page = next_page

    assert append_only_processor.may_have_results is expected, (
        f"Expected may_have_results to be {expected} when"
        f" next_page_called={next_page_called}, remaining={remaining}, next_page={next_page}"
    )


@pytest.mark.unit
class TestFetchMethods:
    def test_fetchmany_with_size(self, append_only_processor: AppendOnlyChangelogProcessor):
        # Mock some results in the processor
        append_only_processor._results = [("row1",), ("row2",), ("row3",)]

        # Fetch 2 results
        fetched = append_only_processor.fetchmany(2)
        assert fetched == [("row1",), ("row2",)], f"Expected to fetch 2 rows, got {fetched}"

        # fetch last one
        fetched = append_only_processor.fetchmany(2)
        assert fetched == [("row3",)], f"Expected to fetch last row, got {fetched}"

    def test_fetchmany_with_invalid_size(self, append_only_processor: AppendOnlyChangelogProcessor):
        with pytest.raises(InterfaceError, match="size must be a positive integer"):
            append_only_processor.fetchmany(-1)

    def test_fetchmany_returns_buffered_results_without_fetching_new_page(
        self, append_only_processor: AppendOnlyChangelogProcessor
    ):
        """Test that fetchmany returns buffered results even if fewer than requested,
        without fetching a new page.

        This tests the relaxed behavior where if fewer rows are buffered than requested,
        we return what's available rather than forcing a fetch to meet the exact count.
        """
        # Mock the _fetch_next_page method to track calls
        fetch_tracker = {"called": False}

        def mock_fetch_next_page():
            fetch_tracker["called"] = True
            # Simulate fetching more results
            append_only_processor._results.extend([("fetched_row1",), ("fetched_row2",)])
            append_only_processor._next_page = None

        append_only_processor._fetch_next_page = mock_fetch_next_page

        # Set up processor with 2 buffered results and indicate more pages available
        append_only_processor._results = [("row1",), ("row2",)]
        append_only_processor._index = 0
        append_only_processor._next_page = "next_page_url"  # More pages available

        # Request 5 results - more than buffered
        fetched = append_only_processor.fetchmany(5)

        # Should return only the 2 buffered results without fetching new page
        assert fetched == [("row1",), ("row2",)], (
            f"Expected to return 2 buffered rows without fetching, got {fetched}"
        )
        assert not fetch_tracker["called"], (
            "Should not have fetched new page when buffered results are available"
        )

        # Now buffer is empty, so next fetchmany should trigger a fetch
        fetched = append_only_processor.fetchmany(1)
        assert fetch_tracker["called"], "Should have fetched new page when buffer is empty"
        assert fetched == [("fetched_row1",)], f"Expected to fetch new results, got {fetched}"

    def test_empty_buffer_triggers_fetch_on_next_call(
        self, append_only_processor: AppendOnlyChangelogProcessor
    ):
        """Test that when the buffer is completely empty after consuming data,
        the next fetch/iteration call triggers fetching more data from the connection.

        This specifically tests the cycle:
        1. Start with buffered data
        2. Consume all buffered data (buffer becomes empty)
        3. Next fetch call triggers _fetch_next_page to refill buffer
        """
        # Track calls to _fetch_next_page
        fetch_tracker = {"count": 0}

        def mock_fetch_next_page():
            fetch_tracker["count"] += 1
            # Simulate fetching a new page of 2 results each time
            append_only_processor._results.extend(
                [(f"page{fetch_tracker['count']}_row1",), (f"page{fetch_tracker['count']}_row2",)]
            )
            append_only_processor._next_page = "next_page" if fetch_tracker["count"] < 2 else None

        append_only_processor._fetch_next_page = mock_fetch_next_page

        # Start with 2 buffered results
        append_only_processor._results = [("initial_row1",), ("initial_row2",)]
        append_only_processor._index = 0
        append_only_processor._next_page = "next_page_url"

        # Consume all buffered data
        result1 = append_only_processor.fetchmany(2)
        assert result1 == [("initial_row1",), ("initial_row2",)]
        assert fetch_tracker["count"] == 0, "Should not have fetched yet"
        assert append_only_processor._remaining == 0, "Buffer should be empty"

        # Next fetchmany should trigger a fetch since buffer is empty
        result2 = append_only_processor.fetchmany(1)
        assert fetch_tracker["count"] == 1, "Should have fetched once"
        assert result2 == [("page1_row1",)]

        # Consume the rest of the current buffer
        result3 = append_only_processor.fetchone()
        assert result3 == ("page1_row2",)
        assert append_only_processor._remaining == 0, "Buffer should be empty again"

        # Another fetch should trigger another page fetch
        result4 = append_only_processor.fetchmany(2)
        assert fetch_tracker["count"] == 2, "Should have fetched twice"
        assert result4 == [("page2_row1",), ("page2_row2",)]

    def test_fetchmany_makes_at_most_one_fetch(
        self, append_only_processor: AppendOnlyChangelogProcessor
    ):
        """Test that fetchmany makes at most one fetch request per call.

        When the buffer is empty and fetchmany is called with a large size,
        it should fetch once and return whatever it got, not try to fulfill
        the entire requested count.
        """
        fetch_count = 0

        def mock_fetch_next_page():
            nonlocal fetch_count
            fetch_count += 1
            # Only return 3 rows even though more were requested
            append_only_processor._results = [("row1",), ("row2",), ("row3",)]
            append_only_processor._index = 0
            append_only_processor._next_page = "more_pages"  # More pages available

        append_only_processor._fetch_next_page = mock_fetch_next_page
        append_only_processor._results = []  # Start with empty buffer
        append_only_processor._index = 0

        # Request 10 results but only 3 are fetched
        result = append_only_processor.fetchmany(10)

        assert fetch_count == 1, "Should have fetched exactly once"
        assert len(result) == 3, f"Should return 3 rows that were fetched, got {len(result)}"
        assert result == [("row1",), ("row2",), ("row3",)]

    def test_fetchmany_returns_empty_when_no_data_available(
        self, append_only_processor: AppendOnlyChangelogProcessor
    ):
        """Test that fetchmany returns empty list when no data is available."""

        def mock_fetch_next_page():
            # Simulate no results available
            append_only_processor._results = []
            append_only_processor._index = 0
            append_only_processor._next_page = None

        append_only_processor._fetch_next_page = mock_fetch_next_page
        append_only_processor._results = []  # Empty buffer
        append_only_processor._index = 0
        append_only_processor._fetch_next_page_called = False

        result = append_only_processor.fetchmany(5)
        assert result == [], "Should return empty list when no data available"

    def test_fetchmany_blocking_in_snapshot_mode(
        self, statement_factory: StatementFactory, mock_connection: Connection
    ):
        """Test that fetchmany uses blocking behavior in snapshot mode with AppendOnlyChangelogProcessor."""
        statement = statement_factory(is_append_only=True)
        # Create processor in SNAPSHOT mode
        processor = AppendOnlyChangelogProcessor(
            mock_connection, statement, ExecutionMode.SNAPSHOT
        )

        fetch_count = 0

        def mock_fetch_next_page():
            nonlocal fetch_count
            fetch_count += 1
            if fetch_count == 1:
                # First fetch returns 2 rows
                processor._results = [("row1",), ("row2",)]
                processor._index = 0
                processor._next_page = "page2"
            elif fetch_count == 2:
                # Second fetch returns 2 more rows
                processor._results.extend([("row3",), ("row4",)])
                processor._next_page = "page3"
            elif fetch_count == 3:
                # Third fetch returns 1 row
                processor._results.extend([("row5",)])
                processor._next_page = None

        processor._fetch_next_page = mock_fetch_next_page
        processor._results = []
        processor._index = 0

        # Request 5 results - should fetch multiple pages to fulfill request
        result = processor.fetchmany(5)

        # In snapshot mode, it should have fetched 3 times to get 5 rows
        assert fetch_count == 3, f"Should have fetched 3 times in snapshot mode, got {fetch_count}"
        assert len(result) == 5, f"Should return exactly 5 rows, got {len(result)}"
        assert result == [("row1",), ("row2",), ("row3",), ("row4",), ("row5",)]

    def test_fetchmany_non_blocking_in_streaming_mode(
        self, statement_factory: StatementFactory, mock_connection: Connection
    ):
        """Test that fetchmany uses non-blocking behavior in streaming mode even with AppendOnlyChangelogProcessor."""
        statement = statement_factory(is_append_only=True)
        # Create processor in STREAMING_QUERY mode
        processor = AppendOnlyChangelogProcessor(
            mock_connection, statement, ExecutionMode.STREAMING_QUERY
        )

        fetch_count = 0

        def mock_fetch_next_page():
            nonlocal fetch_count
            fetch_count += 1
            # Only return 3 rows even though 5 were requested
            processor._results = [("row1",), ("row2",), ("row3",)]
            processor._index = 0
            processor._next_page = "more_pages"

        processor._fetch_next_page = mock_fetch_next_page
        processor._results = []
        processor._index = 0

        # Request 5 results - should only fetch once in streaming mode
        result = processor.fetchmany(5)

        # In streaming mode, it should fetch only once
        assert fetch_count == 1, f"Should have fetched only once in streaming mode, got {fetch_count}"
        assert len(result) == 3, f"Should return only 3 rows available, got {len(result)}"
        assert result == [("row1",), ("row2",), ("row3",)]

    def test_may_have_results_distinguishes_temporary_vs_permanent_emptiness(
        self, append_only_processor: AppendOnlyChangelogProcessor
    ):
        """Test that may_have_results helps distinguish between temporary and permanent emptiness."""
        # Initially, no results fetched yet
        assert append_only_processor.may_have_results is True, "Should indicate may have results initially"

        # Set up mock that returns empty on first fetch, data on second
        fetch_count = 0

        def mock_fetch_next_page():
            nonlocal fetch_count
            fetch_count += 1
            if fetch_count == 1:
                # First fetch: no data yet, but more pages available
                append_only_processor._results = []
                append_only_processor._index = 0
                append_only_processor._next_page = "more_coming"
                append_only_processor._fetch_next_page_called = True
            elif fetch_count == 2:
                # Second fetch: some data arrives
                append_only_processor._results = [("row1",)]
                append_only_processor._index = 0
                append_only_processor._next_page = None  # No more pages

        append_only_processor._fetch_next_page = mock_fetch_next_page

        # First fetchone - no data yet but more may come
        result = append_only_processor.fetchone()
        assert result is None, "Should return None when no data available"
        assert append_only_processor.may_have_results is True, (
            "Should still indicate may have results when temporary empty"
        )

        # Second fetchone - data arrives
        result = append_only_processor.fetchone()
        assert result == ("row1",), "Should return the row"

        # Third fetchone - permanently empty now
        result = append_only_processor.fetchone()
        assert result is None, "Should return None when no more data"
        assert append_only_processor.may_have_results is False, (
            "Should indicate no more results when permanently empty"
        )

    def test_fetchone(self, append_only_processor: AppendOnlyChangelogProcessor):
        # Mock some results in the processor
        append_only_processor._results = [("row1",), ("row2",)]

        # Fetch one result
        fetched = append_only_processor.fetchone()
        assert fetched == ("row1",), f"Expected to fetch first row, got {fetched}"

        # Fetch next result
        fetched = append_only_processor.fetchone()
        assert fetched == ("row2",), f"Expected to fetch second row, got {fetched}"

        # Fetch when no more results are available
        fetched = append_only_processor.fetchone()
        assert fetched is None, f"Expected to fetch None when no more results, got {fetched}"

    def test_fetchall_against_bounded_statement(
        self, append_only_processor: AppendOnlyChangelogProcessor
    ):
        # By default the statement from the fixture should be bounded.
        assert append_only_processor._statement.is_bounded, (
            "Statement should be bounded for this test"
        )

        # Mock some results in the processor
        append_only_processor._results = [("row1",), ("row2",), ("row3",)]

        # Fetch all results
        fetched = append_only_processor.fetchall()
        assert fetched == [("row1",), ("row2",), ("row3",)], (
            f"Expected to fetch all rows, got {fetched}"
        )

    def test_fetchall_vs_streaming_results(
        self, append_only_processor: AppendOnlyChangelogProcessor
    ):
        # Doctor the processor's statement to have come from a streaming query
        append_only_processor._statement.traits.is_bounded = False  # type: ignore

        # fetchall should raise an error when used against an unbounded/streaming query
        with pytest.raises(
            NotSupportedError,
            match=re.escape("Cannot call fetchall() on an unbounded streaming statement"),
        ):
            append_only_processor.fetchall()


@pytest.mark.unit
class TestIteration:
    def test_iteration_over_onhand_results(
        self, append_only_processor: AppendOnlyChangelogProcessor
    ):
        # Mock some results in the processor
        append_only_processor._results = [("row1",), ("row2",), ("row3",)]

        # Iterate over the processor and collect results
        collected = []
        for row in append_only_processor:
            collected.append(row)

        assert collected == [("row1",), ("row2",), ("row3",)], (
            f"Expected to collect all rows through iteration, got {collected}"
        )

    def test_iteration_calls_fetch_next_page(
        self, append_only_processor: AppendOnlyChangelogProcessor
    ):
        # Mock the _fetch_next_page method to track calls
        call_tracker = {"called": False}

        def mock_fetch_next_page():
            call_tracker["called"] = True
            # Simulate fetching a page of results
            append_only_processor._results = [("fetched_row1",), ("fetched_row2",)]
            append_only_processor._index = 0  # Reset index to start of new results
            append_only_processor._next_page = None  # No more pages after this

        append_only_processor._fetch_next_page = mock_fetch_next_page

        # Iterate over the processor to trigger fetching
        collected = []
        for row in append_only_processor:
            collected.append(row)

        assert call_tracker["called"], (
            "Expected _fetch_next_page to have been called during iteration"
        )
        assert collected == [("fetched_row1",), ("fetched_row2",)], (
            f"Expected to collect fetched rows through iteration, got {collected}"
        )

    def test_iteration_stops_when_no_more_results(
        self, append_only_processor: AppendOnlyChangelogProcessor
    ):
        # Mock the _fetch_next_page method to simulate no results and no next page
        def mock_fetch_next_page():
            append_only_processor._results = []
            append_only_processor._index = 0
            append_only_processor._next_page = None

        append_only_processor._fetch_next_page = mock_fetch_next_page

        # Iterate over the processor and collect results
        collected = []
        for row in append_only_processor:
            collected.append(row)

        assert collected == [], f"Expected to collect no rows, got {collected}"

    def test_iteration_with_dict_mode(
        self, statement_factory: StatementFactory, mock_connection: Connection
    ):
        # Create a processor with as_dict=True. By default, the statement factory
        # creates a statement with a simple schema that has one column named "value".
        statement = statement_factory(is_append_only=True)
        processor = AppendOnlyChangelogProcessor(
            mock_connection, statement, ExecutionMode.STREAMING_QUERY, as_dict=True
        )

        # Mock the _fetch_next_page method to simulate fetching results
        def mock_fetch_next_page():
            processor._results = [{"value": "row1"}, {"value": "row2"}]
            processor._index = 0
            processor._next_page = None

        processor._fetch_next_page = mock_fetch_next_page

        # Iterate over the processor and collect results
        collected = []
        for row in processor:
            collected.append(row)

        expected_collected = [
            {"value": "row1"},
            {"value": "row2"},
        ]  # Assuming schema has column1
        assert collected == expected_collected, (
            f"Expected to collect rows as dicts through iteration, got {collected}"
        )


@pytest.mark.unit
class TestFetchNextPage:
    def test_raises_if_statement_not_ready(
        self, append_only_processor: AppendOnlyChangelogProcessor
    ):
        # A bounded statement is ready when in the RUNNING or DONE phases, so set to a phase before
        # that.
        append_only_processor._statement._phase = Phase.RUNNING

        with pytest.raises(InterfaceError, match="Statement is not ready"):
            append_only_processor._fetch_next_page()

    def test_raises_if_no_schema(self, append_only_processor: AppendOnlyChangelogProcessor):
        # Doctor the processor's statement to have no schema
        append_only_processor._statement.traits.schema = None  # type: ignore

        with pytest.raises(
            InterfaceError, match="Trying to fetch results for a non-query statement"
        ):
            append_only_processor._fetch_next_page()

    def test_calls_next_page_if_zero_results_onhand(
        self, append_only_processor: AppendOnlyChangelogProcessor
    ):
        # Mock the connection's _get_statement_results to track calls and return some results
        call_tracker = {"called": False}

        # By default, the statement is expected to return
        # results with boolean values in the first column, which
        # come to us from Flink results as strings.
        def mock_get_statement_results(
            statement_name: str, next_url: str | None
        ) -> tuple[list[ChangelogRow], str | None]:
            call_tracker["called"] = True
            return [
                ChangelogRow(0, ["true"]),
                ChangelogRow(0, ["false"]),
            ], ""  # Mock some results and no next page

        append_only_processor._connection._get_statement_results = mock_get_statement_results

        # Ensure processor has no on-hand results to trigger fetching
        append_only_processor._results = []
        append_only_processor._index = 0
        append_only_processor._next_page = None

        # Call _fetch_next_page and check that it called the connection method and processed results
        append_only_processor._fetch_next_page()

        assert call_tracker["called"], "Expected _get_statement_results to have been called"
        assert append_only_processor._results == [(True,), (False,)], (
            "Expected fetched results to be stored in processor"
        )

    def test_fetch_next_page_called_during_iteration(
        self, append_only_processor: AppendOnlyChangelogProcessor
    ):
        # Mock the _fetch_next_page method to track calls and simulate fetching results
        call_tracker = {"called": False}

        def mock_fetch_next_page():
            call_tracker["called"] = True
            append_only_processor._results = [("fetched_row1",), ("fetched_row2",)]
            append_only_processor._index = 0
            append_only_processor._next_page = None

        append_only_processor._fetch_next_page = mock_fetch_next_page

        # Iterate over the processor to trigger fetching
        collected = []
        for row in append_only_processor:
            collected.append(row)

        assert call_tracker["called"], (
            "Expected _fetch_next_page to have been called during iteration"
        )
        assert collected == [("fetched_row1",), ("fetched_row2",)], (
            f"Expected to collect fetched rows through iteration, got {collected}"
        )

    def test_raises_if_non_insert_op_in_append_only_results(
        self, append_only_processor: AppendOnlyChangelogProcessor
    ):
        """append-only changelog processor should raise if it encounters a non-INSERT op in results
        from the server, since that violates the contract of append-only statements."""

        def mock_get_statement_results(
            statement_name: str, next_url: str | None
        ) -> tuple[list[ChangelogRow], str | None]:
            return [
                ChangelogRow(Op.DELETE.value, ["value"])
            ], None  # Mock a non-INSERT op in results

        append_only_processor._connection._get_statement_results = mock_get_statement_results

        # Ensure processor has no on-hand results to trigger fetching
        append_only_processor._results = []
        append_only_processor._index = 0
        append_only_processor._next_page = None

        with pytest.raises(NotSupportedError, match="Non-INSERT op"):
            append_only_processor._fetch_next_page()


@pytest.fixture
def raw_changelog_processor(
    statement_factory: StatementFactory, mock_connection: Connection
) -> RawChangelogProcessor:
    """A fixture that returns a RawChangelogProcessor instance in streaming mode."""
    statement = statement_factory(is_append_only=False)
    return RawChangelogProcessor(
        mock_connection, statement, ExecutionMode.STREAMING_QUERY
    )


@pytest.mark.unit
class TestRawChangelogProcessor:
    """
    Tests over RawChangelogProcessor. Only material difference
    between this and AppendOnlyChangelogProcessor is that the _retain method in this class
    converts retained rows into ChangeloggedRow named tuples that include the changelog operation
    type, so we focus tests on that behavior here.
    """

    def test_retain_converts_to_changelogged_row(
        self, raw_changelog_processor: RawChangelogProcessor
    ):
        # Call _retain a couple of times -- first with an INSERT op, then with a DELETE op.

        raw_changelog_processor._retain(
            Op.INSERT, ("value1", 123, "true")
        )  # Mock an INSERT op with some values

        raw_changelog_processor._retain(Op.DELETE, ("value1", 123, "true"))

        # Fetch the first retained row and check that it's a ChangeloggedRow with expected values
        v = raw_changelog_processor.fetchone()
        assert isinstance(v, ChangeloggedRow), (
            f"Expected fetched row to be a ChangeloggedRow, got {type(v)}"
        )
        assert v.op == Op.INSERT, f"Expected op to be Op.INSERT, got {v.op}"
        assert v.row == ("value1", 123, "true"), f"Expected row data to be unchanged, got {v.row}"

        # Fetch the second retained row and check that it's a ChangeloggedRow with expected values
        v = raw_changelog_processor.fetchone()
        assert isinstance(v, ChangeloggedRow), (
            f"Expected fetched row to be a ChangeloggedRow, got {type(v)}"
        )
        assert v.op == Op.DELETE, f"Expected op to be Op.DELETE, got {v.op}"
        assert v.row == ("value1", 123, "true"), f"Expected row data to be unchanged, got {v.row}"

    def test_raw_processor_blocking_in_snapshot_mode(
        self, statement_factory: StatementFactory, mock_connection: Connection
    ):
        """Test that RawChangelogProcessor also uses blocking behavior in snapshot mode."""
        statement = statement_factory(is_append_only=False)
        # Create processor in SNAPSHOT mode
        processor = RawChangelogProcessor(
            mock_connection, statement, ExecutionMode.SNAPSHOT
        )

        fetch_count = 0

        def mock_fetch_next_page():
            nonlocal fetch_count
            fetch_count += 1
            if fetch_count == 1:
                # First fetch returns 2 changelog rows
                processor._results = [
                    ChangeloggedRow(Op.INSERT, ("row1",)),
                    ChangeloggedRow(Op.UPDATE_BEFORE, ("row2",)),
                ]
                processor._index = 0
                processor._next_page = "page2"
            elif fetch_count == 2:
                # Second fetch returns 2 more changelog rows
                processor._results.extend([
                    ChangeloggedRow(Op.UPDATE_AFTER, ("row2_updated",)),
                    ChangeloggedRow(Op.DELETE, ("row3",)),
                ])
                processor._next_page = None

        processor._fetch_next_page = mock_fetch_next_page
        processor._results = []
        processor._index = 0

        # Request 4 results - should fetch multiple pages to fulfill request
        result = processor.fetchmany(4)

        # In snapshot mode, it should have fetched 2 times to get 4 rows
        assert fetch_count == 2, f"Should have fetched 2 times in snapshot mode, got {fetch_count}"
        assert len(result) == 4, f"Should return exactly 4 rows, got {len(result)}"
        assert all(isinstance(r, ChangeloggedRow) for r in result)


@pytest.mark.unit
class TestFetchMetrics:
    """Tests for FetchMetrics collection and the metrics property."""

    def test_metrics_initialized_to_zero(self, append_only_processor: AppendOnlyChangelogProcessor):
        """Test that metrics are initialized with zero values."""
        metrics = append_only_processor.metrics

        assert isinstance(metrics, FetchMetrics)
        assert metrics.total_page_fetches == 0
        assert metrics.total_changelog_rows_fetched == 0
        assert metrics.empty_page_fetches == 0
        assert metrics.fetch_request_secs == pytest.approx(0.0)
        assert metrics.paused_times == 0
        assert metrics.paused_secs == pytest.approx(0.0)
        assert metrics.avg_rows_per_page == pytest.approx(0.0)

    def test_metrics_updated_during_fetch(
        self, append_only_processor: AppendOnlyChangelogProcessor
    ):
        """Test that metrics are properly updated when fetching pages."""

        # Mock the connection's _get_statement_results to return results
        def mock_get_statement_results(
            statement_name: str, next_url: str | None
        ) -> tuple[list[ChangelogRow], str | None]:
            # Return 3 rows on first fetch
            return [
                ChangelogRow(Op.INSERT.value, ["value1"]),
                ChangelogRow(Op.INSERT.value, ["value2"]),
                ChangelogRow(Op.INSERT.value, ["value3"]),
            ], None

        append_only_processor._connection._get_statement_results = mock_get_statement_results

        # Ensure processor starts with no results
        append_only_processor._results = []
        append_only_processor._index = 0

        # Fetch a page
        # Need 3 time.monotonic() calls: prep_for_fetch, record_fetch_completion,
        # and setting _most_recent_results_fetch_time
        with patch("time.monotonic", side_effect=[100.0, 100.5, 100.5]):
            append_only_processor._fetch_next_page()

        metrics = append_only_processor.metrics
        assert metrics.total_page_fetches == 1
        assert metrics.total_changelog_rows_fetched == 3
        assert metrics.empty_page_fetches == 0
        assert metrics.fetch_request_secs == pytest.approx(0.5)
        assert metrics.avg_rows_per_page == pytest.approx(3.0)

    def test_metrics_with_pausing(self, append_only_processor: AppendOnlyChangelogProcessor):
        """Test that pausing metrics are tracked correctly."""
        # Set up connection pause time
        append_only_processor._connection.statement_results_page_fetch_pause_secs = 2.0

        # Mock get_statement_results to return some rows
        def mock_get_statement_results(
            statement_name: str, next_url: str | None
        ) -> tuple[list[ChangelogRow], str | None]:
            return [ChangelogRow(Op.INSERT.value, ["value1"])], None

        append_only_processor._connection._get_statement_results = mock_get_statement_results

        # Set up state as if we had fetched before (to trigger pause logic)
        append_only_processor._results = []
        append_only_processor._index = 0
        append_only_processor._most_recent_results_fetch_time = 99.0  # Previous fetch time
        append_only_processor._fetch_next_page_called = True

        # Mock time to simulate: current time is 99.5, so only 0.5 seconds elapsed
        # This means we need to pause for 1.5 seconds (2.0 - 0.5)
        # Need: elapsed check, prep_for_fetch, record_fetch_completion, setting
        # _most_recent_results_fetch_time
        with (
            patch("time.monotonic", side_effect=[99.5, 100.0, 100.1, 100.1]),
            patch("time.sleep") as mock_sleep,
        ):
            append_only_processor._fetch_next_page()
            mock_sleep.assert_called_once_with(1.5)

        metrics = append_only_processor.metrics
        assert metrics.paused_times == 1
        assert metrics.paused_secs == pytest.approx(1.5)
        assert metrics.total_page_fetches == 1
        assert metrics.fetch_request_secs == pytest.approx(0.1)  # 100.1 - 100.0

    def test_metrics_empty_page_fetch(self, append_only_processor: AppendOnlyChangelogProcessor):
        """Test that empty page fetches are tracked."""

        # Mock to return empty results
        def mock_get_statement_results(
            statement_name: str, next_url: str | None
        ) -> tuple[list[ChangelogRow], str | None]:
            return [], None  # Empty page

        append_only_processor._connection._get_statement_results = mock_get_statement_results
        append_only_processor._results = []
        append_only_processor._index = 0

        with patch("time.monotonic", side_effect=[100.0, 100.2, 100.2]):
            append_only_processor._fetch_next_page()

        metrics = append_only_processor.metrics
        assert metrics.total_page_fetches == 1
        assert metrics.total_changelog_rows_fetched == 0
        assert metrics.empty_page_fetches == 1
        assert metrics.avg_rows_per_page == pytest.approx(0.0)

    def test_metrics_accumulate_across_multiple_fetches(
        self, append_only_processor: AppendOnlyChangelogProcessor
    ):
        """Test that metrics accumulate correctly across multiple fetch operations."""
        fetch_count = 0

        def mock_get_statement_results(
            statement_name: str, next_url: str | None
        ) -> tuple[list[ChangelogRow], str | None]:
            nonlocal fetch_count
            fetch_count += 1
            if fetch_count == 1:
                # First fetch: 2 rows
                return [
                    ChangelogRow(Op.INSERT.value, ["value1"]),
                    ChangelogRow(Op.INSERT.value, ["value2"]),
                ], "next_page"
            elif fetch_count == 2:
                # Second fetch: 3 rows
                return [
                    ChangelogRow(Op.INSERT.value, ["value3"]),
                    ChangelogRow(Op.INSERT.value, ["value4"]),
                    ChangelogRow(Op.INSERT.value, ["value5"]),
                ], "next_page"
            else:
                # Third fetch: empty
                return [], None

        append_only_processor._connection._get_statement_results = mock_get_statement_results
        append_only_processor._connection.statement_results_page_fetch_pause_secs = (
            0  # No pausing for this test
        )
        append_only_processor._results = []
        append_only_processor._index = 0

        # Perform three fetches
        with patch("time.monotonic", side_effect=[100.0, 100.1, 100.1]):  # First fetch
            append_only_processor._fetch_next_page()

        # Clear results to trigger next fetch
        append_only_processor._results = []
        append_only_processor._index = 0
        append_only_processor._most_recent_results_fetch_time = 100.1

        # Second fetch (check elapsed, prep, complete, set time)
        with patch("time.monotonic", side_effect=[101.0, 101.0, 101.2, 101.2]):
            append_only_processor._fetch_next_page()

        # Clear results to trigger next fetch
        append_only_processor._results = []
        append_only_processor._index = 0
        append_only_processor._most_recent_results_fetch_time = 101.2

        # Third fetch (empty)
        with patch("time.monotonic", side_effect=[102.0, 102.0, 102.05, 102.05]):
            append_only_processor._fetch_next_page()

        metrics = append_only_processor.metrics
        assert metrics.total_page_fetches == 3
        assert metrics.total_changelog_rows_fetched == 5  # 2 + 3 + 0
        assert metrics.empty_page_fetches == 1  # Only the third fetch was empty
        assert metrics.fetch_request_secs == pytest.approx(0.35)  # 0.1 + 0.2 + 0.05
        assert metrics.avg_rows_per_page == pytest.approx(5 / 3)  # 5 rows / 3 fetches
