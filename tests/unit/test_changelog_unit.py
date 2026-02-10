import re

import pytest

from confluent_sql.changelog import (
    AppendOnlyChangelogProcessor,
    ChangeloggedRow,
    RawChangelogProcessor,
)
from confluent_sql.connection import Connection
from confluent_sql.exceptions import InterfaceError, NotSupportedError
from confluent_sql.statement import ChangelogRow, Op, Phase
from tests.unit.conftest import StatementFactory


@pytest.fixture
def append_only_processor(
    statement_factory: StatementFactory, mock_connection: Connection
) -> AppendOnlyChangelogProcessor:
    """A fixture that returns an AppendOnlyChangelogProcessor instance."""
    statement = statement_factory(is_append_only=True)
    return AppendOnlyChangelogProcessor(mock_connection, statement)


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
        processor = AppendOnlyChangelogProcessor(mock_connection, statement, as_dict=True)

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
    """A fixture that returns an AppendOnlyChangelogProcessor instance."""
    statement = statement_factory(is_append_only=False)
    return RawChangelogProcessor(mock_connection, statement)


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
