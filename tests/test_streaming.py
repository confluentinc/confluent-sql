#!/usr/bin/env python3
"""
Test streaming query functionality for confluent-sql library.
"""

import os

import confluent_sql
import pytest
from confluent_sql.exceptions import InterfaceError


def test_unbounded_query_fetchall_error(connection_manager):
    """Test that fetchall() raises an error for unbounded queries."""
    with connection_manager() as connection:
        cursor = connection.cursor()

        # Use real unbounded streaming query from Confluent Cloud examples
        cursor.execute("SELECT * FROM `examples`.`marketplace`.`clicks`")

        # Should raise an error for unbounded queries
        with pytest.raises(InterfaceError) as exc_info:
            cursor.fetchall()

        assert "fetchall() is not supported for unbounded streaming queries" in str(
            exc_info.value
        )
        assert "Use fetchone() or fetchmany() in a loop" in str(exc_info.value)


def test_streaming_query_fetchone(connection_manager):
    """Test that fetchone() works for streaming queries (yielding iterator)."""
    with connection_manager() as connection:
        cursor = connection.cursor()

        # Use real unbounded streaming query from Confluent Cloud examples
        cursor.execute("SELECT * FROM `examples`.`marketplace`.`clicks`")

        # Should work for streaming queries (yielding iterator)
        # Get a few rows to test the pattern
        rows_received = 0
        max_rows = 5  # Limit to avoid infinite loop in test

        while rows_received < max_rows:
            row = cursor.fetchone()
            if row is None:
                break

            rows_received += 1

            # Verify the row structure (raw data without changelog operations)
            assert len(row) > 0  # Should have data columns
            # No changelog operations - just raw data



def test_streaming_query_fetchmany(connection_manager):
    """Test that fetchmany() works for streaming queries (yielding iterator)."""
    with connection_manager() as connection:
        cursor = connection.cursor()

        # Use real unbounded streaming query from Confluent Cloud examples
        cursor.execute("SELECT * FROM `examples`.`marketplace`.`clicks`")

        # Should work for streaming queries (yielding iterator)
        # Get a few batches to test the pattern
        total_rows = 0
        max_batches = 3  # Limit to avoid infinite loop in test
        batch_count = 0

        while batch_count < max_batches:
            batch = cursor.fetchmany(2)  # Fetch 2 rows at a time
            if not batch:
                break

            batch_count += 1
            total_rows += len(batch)

            # Verify the batch structure (raw data without changelog operations)
            for row in batch:
                assert len(row) > 0  # Should have data columns
                # No changelog operations - just raw data


def test_cursor_metadata_streaming(connection_manager):
    """Test cursor metadata for streaming queries."""
    with connection_manager() as connection:
        cursor = connection.cursor()

        # Use real unbounded streaming query from Confluent Cloud examples
        cursor.execute("SELECT * FROM `examples`.`marketplace`.`clicks`")

        # Test metadata
        assert cursor._statement.name is not None
        assert cursor._statement.sql_kind == "SELECT"
        assert cursor._statement.is_bounded is False  # This should be an unbounded streaming query
        assert cursor._statement.is_append_only is True
        assert cursor._statement.connection_refs == []  # Should be empty for this query

        # Test description - should have multiple columns from the clicks table
        assert cursor._statement.description is not None
        assert len(cursor._statement.description) > 1  # Should have multiple columns


def test_streaming_pattern_example(connection_manager):
    """Test a typical streaming pattern using fetchone() in a loop."""
    with connection_manager() as connection:
        cursor = connection.cursor()

        # Use real unbounded streaming query from Confluent Cloud examples
        cursor.execute("SELECT * FROM `examples`.`marketplace`.`clicks`")

        # Simulate streaming pattern
        print("Streaming pattern example:")
        row_count = 0
        max_rows = 10  # Limit to avoid infinite loop in test

        while row_count < max_rows:
            row = cursor.fetchone()
            if row is None:
                break
            row_count += 1
            print(f"  Received streaming row {row_count}: {row}")

        print(f"Total streaming rows received: {row_count}")


def test_streaming_batch_pattern_example(connection_manager):
    """Test a typical streaming pattern using fetchmany() in a loop."""
    with connection_manager() as connection:
        cursor = connection.cursor()

        # Use real unbounded streaming query from Confluent Cloud examples
        cursor.execute("SELECT * FROM `examples`.`marketplace`.`clicks`")

        # Simulate streaming batch pattern
        total_rows = 0
        batch_size = 3
        max_batches = 5  # Limit to avoid infinite loop in test
        batch_count = 0

        while batch_count < max_batches:
            batch = cursor.fetchmany(batch_size)
            if not batch:
                break

            batch_count += 1
            total_rows += len(batch)

            # Show first row of each batch for debugging
            if batch:
                print(f"    First row: {batch[0]}")




if __name__ == "__main__":
    # Run tests if called directly
    pytest.main([__file__, "-v"])
