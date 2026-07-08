"""
Integration test for the confluent-sql DB-API v2 driver.

This test makes a real API call to Confluent Cloud Flink environment.
Credentials must be provided via environment variables.
"""

import os
import time

import httpx
import pytest

import confluent_sql
from confluent_sql.connection import Connection
from confluent_sql.exceptions import StatementNotFoundError
from confluent_sql.execution_mode import ExecutionMode
from confluent_sql.statement import Statement


def _start_running_streaming_statement(connection, table_name, statement_name):
    """Launch an unbounded streaming SELECT (stays RUNNING) and return the (cursor, statement)."""
    cursor = connection.cursor(mode=ExecutionMode.STREAMING_QUERY)
    cursor.execute(f"SELECT * FROM {table_name}", statement_name=statement_name)
    assert cursor.statement.is_running, (
        f"Expected streaming statement to be RUNNING, got {cursor.statement.phase.value}"
    )
    return cursor, cursor.statement


def _poll_until_stopped(connection, statement_name, timeout_seconds=60):
    """Poll get_statement until the named statement is STOPPED or timeout."""
    start = time.monotonic()
    while time.monotonic() - start < timeout_seconds:
        statement = connection.get_statement(statement_name)
        if statement.is_stopped:
            return statement
        time.sleep(0.5)
    raise TimeoutError(f"Statement '{statement_name}' not STOPPED after {timeout_seconds}s")


def _wait_for_row(cursor, timeout_seconds=5):
    """Poll fetchone() until a row arrives or timeout.

    In streaming mode, fetchone() is non-blocking and may return None even when
    more data will arrive shortly. This helper polls with a short delay until
    a row is available or the timeout is exceeded.

    Args:
        cursor: The cursor to fetch from
        timeout_seconds: Maximum time to poll (default 5 seconds)

    Returns:
        The first non-None row returned by fetchone()

    Raises:
        TimeoutError: If no row is available after timeout
    """
    start = time.monotonic()
    while time.monotonic() - start < timeout_seconds:
        row = cursor.fetchone()
        if row is not None:
            return row
        # Exit early if the cursor indicates there will be no more results.
        may_have_results = getattr(cursor, "may_have_results", None)
        if may_have_results is False:
            break
        time.sleep(0.1)
    raise TimeoutError(f"No row available after polling for {timeout_seconds} seconds")


@pytest.mark.integration
class TestConnection:
    @pytest.mark.skipif(
        os.getenv("CONFLUENT_FLINK_API_KEY") is None
        or os.getenv("CONFLUENT_FLINK_API_SECRET") is None
        or os.getenv("CONFLUENT_ENV_ID") is None
        or os.getenv("CONFLUENT_ORG_ID") is None
        or os.getenv("CONFLUENT_CLOUD_PROVIDER") is None
        or os.getenv("CONFLUENT_CLOUD_REGION") is None,
        reason="Missing confluent environment variables",
    )
    def test_confluent_sql_connection(self):
        """Test connection to Confluent Cloud Flink SQL service."""
        flink_api_key = os.getenv("CONFLUENT_FLINK_API_KEY", "")
        flink_api_secret = os.getenv("CONFLUENT_FLINK_API_SECRET", "")
        environment_id = os.getenv("CONFLUENT_ENV_ID", "")
        organization_id = os.getenv("CONFLUENT_ORG_ID", "")
        compute_pool_id = os.getenv("CONFLUENT_COMPUTE_POOL_ID") or None
        cloud_provider = os.getenv("CONFLUENT_CLOUD_PROVIDER", "")
        cloud_region = os.getenv("CONFLUENT_CLOUD_REGION", "")
        connection = confluent_sql.connect(
            flink_api_key=flink_api_key,
            flink_api_secret=flink_api_secret,
            environment_id=environment_id,
            organization_id=organization_id,
            compute_pool_id=compute_pool_id,
            cloud_region=cloud_region,
            cloud_provider=cloud_provider,
        )
        # Test cursor creation
        cursor = connection.cursor(as_dict=True)
        assert cursor is not None

        # Check that we can list the catalogs (environments) available
        cursor.execute("SHOW CATALOGS")
        result = cursor.fetchall()
        # We should always have the catalog that corresponds to the environment
        # of the connection. SHOW CATALOGS returns a list of rows with name and id.
        # We can check that the `environment_id` passed to the `connect` function is present:
        catalog_ids = [res["Catalog ID"] for res in result]  # type: ignore
        assert environment_id in catalog_ids

        # Stop + delete the statement, CCloud-Flink side.
        cursor.close()

    def test_default_compute_pool_when_none_specified(self, poolless_connection: Connection):
        """A connection without a compute pool runs statements in the environment default.

        Proves the end-to-end #108 path: no compute_pool_id is sent, yet Flink still runs the
        statement and associates it with a (default) compute pool in the response.
        """
        assert poolless_connection.compute_pool_id is None

        with poolless_connection.closing_cursor() as cursor:
            cursor.execute("SELECT 1 as answer FROM `INFORMATION_SCHEMA`.`TABLES`")
            row = cursor.fetchone()
            assert row == (1,)

            # Flink picked a default pool for us even though we named none.
            assert cursor.statement.compute_pool_id, (
                "Expected the server to associate the statement with a default compute pool"
            )

    def test_closing_cursor_after_executing_statement(self, connection: Connection, mocker):
        """Test that auto closing a cursor used for a bounded statement works as expected."""
        with connection.closing_cursor() as cursor:
            assert cursor is not None
            assert cursor.is_closed is False
            delete_statement_spy = mocker.spy(cursor, "delete_statement")
            cursor.execute("SELECT 1 as answer FROM `INFORMATION_SCHEMA`.`TABLES`")
            row = cursor.fetchone()
            assert isinstance(row, tuple), "Expected row to be a tuple"
            assert row == (1,)

        assert cursor.is_closed is True, (
            "Expected cursor to be closed after exiting context manager."
        )
        assert delete_statement_spy.call_count == 1, (
            "Expected delete_statement to be called once on cursor close."
        )

    def test_closing_cursor_honors_as_dict(self, connection: Connection):
        """Test that auto closing a cursor works as expected and respects as_dict flag."""
        with connection.closing_cursor(as_dict=True) as cursor:
            assert cursor is not None
            assert cursor.is_closed is False
            cursor.execute("SELECT 1 AS answer from `INFORMATION_SCHEMA`.`TABLES`")
            row = cursor.fetchone()
            assert isinstance(row, dict), "Expected row to be a dict when as_dict=True"
            assert row["answer"] == 1

        assert cursor.is_closed is True, (
            "Expected cursor to be closed after exiting context manager."
        )

    def test_closing_cursor_no_statement(self, connection: Connection, mocker):
        """Test that auto closing a cursor not used for any statement works as expected."""
        with connection.closing_cursor() as cursor:
            assert cursor is not None
            assert cursor.is_closed is False

        assert cursor.is_closed is True, (
            "Expected cursor to be closed after exiting context manager."
        )

    def test_closing_streaming_cursor_after_executing_statement(self, connection: Connection):
        """Test that auto closing a streaming cursor used for a statement works as expected."""
        with connection.closing_streaming_cursor() as cursor:
            assert cursor is not None
            assert cursor.is_closed is False
            assert cursor.is_streaming is True, "Expected cursor to be in streaming mode"
            cursor.execute("SELECT 1 as answer FROM `INFORMATION_SCHEMA`.`TABLES`")
            row = _wait_for_row(cursor)
            assert isinstance(row, tuple), "Expected row to be a tuple"
            assert row == (1,)

        assert cursor.is_closed is True, (
            "Expected cursor to be closed after exiting context manager."
        )

    def test_closing_streaming_cursor_honors_as_dict(self, connection: Connection):
        """Test that auto closing a streaming cursor respects as_dict flag."""
        with connection.closing_streaming_cursor(as_dict=True) as cursor:
            assert cursor is not None
            assert cursor.is_closed is False
            assert cursor.is_streaming is True, "Expected cursor to be in streaming mode"
            cursor.execute("SELECT 1 AS answer from `INFORMATION_SCHEMA`.`TABLES`")
            row = _wait_for_row(cursor)
            assert isinstance(row, dict), "Expected row to be a dict when as_dict=True"
            assert row["answer"] == 1

        assert cursor.is_closed is True, (
            "Expected cursor to be closed after exiting context manager."
        )

    def test_closing_streaming_cursor_no_statement(self, connection: Connection):
        """Test that auto closing a streaming cursor not used for any statement works."""
        with connection.closing_streaming_cursor() as cursor:
            assert cursor is not None
            assert cursor.is_closed is False
            assert cursor.is_streaming is True, "Expected cursor to be in streaming mode"

        assert cursor.is_closed is True, (
            "Expected cursor to be closed after exiting context manager."
        )

    def test_get_statement_by_name(self, connection: Connection, cleaned_up_statement_name: str):
        """Test getting a statement by its name."""
        # Create a statement with a specific name
        cursor = connection.cursor()
        cursor.execute(
            "SELECT 1 as answer FROM `INFORMATION_SCHEMA`.`TABLES`",
            statement_name=cleaned_up_statement_name,
        )
        # Use public API to get statement
        original_statement = cursor.statement
        assert original_statement.name == cleaned_up_statement_name

        # Get the statement by name
        retrieved_statement = connection.get_statement(cleaned_up_statement_name)

        # Verify we got a Statement object with correct name
        assert isinstance(retrieved_statement, Statement)
        assert retrieved_statement.name == cleaned_up_statement_name

        # Clean up cursor (statement will be cleaned up by fixture)
        cursor.close()

    def test_get_statement_refreshes_object(
        self, connection: Connection, cleaned_up_statement_name: str
    ):
        """Test that passing a Statement object refreshes its state from server."""
        # Create a statement with a specific name
        cursor = connection.cursor()
        cursor.execute(
            "SELECT 1 as answer FROM `INFORMATION_SCHEMA`.`TABLES`",
            statement_name=cleaned_up_statement_name,
        )
        # Use public API to get statement
        original_statement = cursor.statement

        # Get refreshed state
        refreshed_statement = connection.get_statement(original_statement)

        # Verify we got a new Statement object (not the same instance)
        assert isinstance(refreshed_statement, Statement)
        assert refreshed_statement.name == original_statement.name
        # The statement should have the same name but be a different object
        assert refreshed_statement is not original_statement

        # Clean up cursor (statement will be cleaned up by fixture)
        cursor.close()

    def test_get_statement_nonexistent_raises(self, connection: Connection):
        """Test that getting a non-existent statement raises StatementNotFoundError."""
        with pytest.raises(
            StatementNotFoundError,
            match="not found",
        ) as exc_info:
            connection.get_statement("non-existent-statement-name")

        # Verify exception has statement name
        assert exc_info.value.statement_name == "non-existent-statement-name"


@pytest.mark.integration
class TestRetryOnTransientResultFetchErrors:
    """Integration coverage for #137's retry-on-idempotent-GET behavior."""

    def test_result_fetch_retries_past_simulated_econnresets_then_hits_confluent_cloud_for_real(
        self, connection: Connection, mocker
    ):
        """Simulates two ECONNRESET-style drops on the results-fetch GET, then lets the third
        attempt travel all the way through to Confluent Cloud for real.

        NOTE: this is largely redundant with the fully-mocked coverage in
        tests/unit/test_retry_unit.py and TestConnectionRetriesIdempotentGets
        (tests/unit/test_connection_unit.py) -- those already prove call_with_retries' counting,
        backoff, and exception-filtering, and that Connection wires it into
        _get_statement_results correctly. It's written anyway, against a real environment, in
        the spirit of treating #137 as a release-branch bugfix: showing the retry survives an
        actual round trip is worth the extra network call, not just asserting it in the abstract.
        """
        with connection.closing_cursor() as cursor:
            cursor.execute("SELECT 1 as answer FROM `INFORMATION_SCHEMA`.`TABLES`")

            # Capture the real bound method before patching so the third call can still reach
            # Confluent Cloud for real.
            real_request = connection._get_flink_client().request
            call_count = 0

            def flaky_then_real_request(*args, **kwargs):
                nonlocal call_count
                call_count += 1
                if call_count <= 2:
                    raise httpx.ReadError("[Errno 54] Connection reset by peer")
                return real_request(*args, **kwargs)

            request_mock = mocker.patch.object(
                connection._get_flink_client(), "request", side_effect=flaky_then_real_request
            )

            row = cursor.fetchone()

            assert row == (1,)
            assert request_mock.call_count == 3


@pytest.mark.integration
class TestStopStatement:
    """Integration tests for stop_statement against a real environment."""

    def test_blocking_stop_reaches_stopped_without_deleting(
        self,
        table_connection: Connection,
        test_table_name: str,
        cleaned_up_statement_name: str,
    ):
        """Blocking stop_statement settles the statement to STOPPED and leaves it queryable."""
        cursor, _ = _start_running_streaming_statement(
            table_connection, test_table_name, cleaned_up_statement_name
        )
        try:
            stopped = table_connection.stop_statement(
                cleaned_up_statement_name, wait_for_stopped=True
            )
            assert stopped.is_stopped

            # Proof it was stopped, not deleted: it is still retrievable and STOPPED.
            still_there = table_connection.get_statement(cleaned_up_statement_name)
            assert still_there.is_stopped
        finally:
            cursor.close()

    def test_non_blocking_stop_returns_promptly_then_settles(
        self,
        table_connection: Connection,
        test_table_name: str,
        cleaned_up_statement_name: str,
    ):
        """Non-blocking stop_statement returns promptly with the stop accepted, then settles.

        The PATCH 200 response reflects the stop request (spec.stopped becomes true) but the phase
        may still be RUNNING at that instant -- the server transitions to STOPPED asynchronously.
        """
        cursor, _ = _start_running_streaming_statement(
            table_connection, test_table_name, cleaned_up_statement_name
        )
        try:
            result = table_connection.stop_statement(
                cleaned_up_statement_name, wait_for_stopped=False
            )
            assert result.stop_requested

            settled = _poll_until_stopped(table_connection, cleaned_up_statement_name)
            assert settled.is_stopped
        finally:
            cursor.close()

    def test_stopping_already_stopped_statement_is_noop(
        self,
        table_connection: Connection,
        test_table_name: str,
        cleaned_up_statement_name: str,
    ):
        """Re-stopping an already-STOPPED statement returns it and does not raise."""
        cursor, _ = _start_running_streaming_statement(
            table_connection, test_table_name, cleaned_up_statement_name
        )
        try:
            stopped = table_connection.stop_statement(
                cleaned_up_statement_name, wait_for_stopped=True
            )
            assert stopped.is_stopped

            # Passing the already-terminal Statement back short-circuits with no error.
            again = table_connection.stop_statement(stopped)
            assert again.is_stopped
        finally:
            cursor.close()

    def test_cursor_stop_statement(
        self,
        table_connection: Connection,
        test_table_name: str,
        cleaned_up_statement_name: str,
    ):
        """cursor.stop_statement stops the cursor's statement and updates its tracked state."""
        cursor = table_connection.cursor(mode=ExecutionMode.STREAMING_QUERY)
        try:
            cursor.execute(
                f"SELECT * FROM {test_table_name}", statement_name=cleaned_up_statement_name
            )
            assert cursor.statement.is_running

            stopped = cursor.stop_statement(wait_for_stopped=True)
            assert stopped.is_stopped
            assert cursor.statement.is_stopped

            # Stopped, not deleted: still retrievable.
            assert table_connection.get_statement(cleaned_up_statement_name).is_stopped
        finally:
            cursor.close()

    def test_stop_nonexistent_statement_raises(self, connection: Connection):
        """Stopping a non-existent statement raises StatementNotFoundError."""
        with pytest.raises(StatementNotFoundError) as exc_info:
            connection.stop_statement("non-existent-statement-name", wait_for_stopped=False)
        assert exc_info.value.statement_name == "non-existent-statement-name"
