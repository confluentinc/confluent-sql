"""
Integration test for the confluent-sql DB-API v2 driver.

This test makes a real API call to Confluent Cloud Flink environment.
Credentials must be provided via environment variables.
"""

import os
import time

import pytest

import confluent_sql
from confluent_sql.connection import Connection
from confluent_sql.exceptions import StatementNotFoundError
from confluent_sql.statement import Statement


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
        or os.getenv("CONFLUENT_COMPUTE_POOL_ID") is None
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
        compute_pool_id = os.getenv("CONFLUENT_COMPUTE_POOL_ID", "")
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
