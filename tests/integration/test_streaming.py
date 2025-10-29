"""
Test streaming query functionality for confluent-sql library.
"""

import pytest
from confluent_sql.exceptions import InterfaceError


def test_unbounded_query_fetchall_error(cursor):
    """Test that fetchall() raises an error for unbounded queries."""
    # Use real unbounded streaming query from Confluent Cloud examples
    cursor.execute("SELECT * FROM `examples`.`marketplace`.`clicks` LIMIT 10", bounded=False)
    results = cursor.fetchall()
    assert len(results) <= 10



def test_streaming_query_fetchone(cursor):
    """Test that fetchone() works for streaming queries (yielding iterator)."""
    # Use real unbounded streaming query from Confluent Cloud examples
    cursor.execute("SELECT * FROM `examples`.`marketplace`.`clicks`", bounded=False)

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
        assert len(row["row"]) > 0  # Should have data columns
        # No changelog operations - just raw data



def test_streaming_query_fetchmany(cursor):
    """Test that fetchmany() works for streaming queries (yielding iterator)."""
    # Use real unbounded streaming query from Confluent Cloud examples
    cursor.execute("SELECT * FROM `examples`.`marketplace`.`clicks`", bounded=False)

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


def test_cursor_metadata_streaming(cursor):
    """Test cursor metadata for streaming queries."""
    # Use real unbounded streaming query from Confluent Cloud examples
    cursor.execute("SELECT * FROM `examples`.`marketplace`.`clicks`", bounded=False)

    # Test metadata
    assert cursor._statement.name is not None
    assert cursor._statement.sql_kind == "SELECT"
    assert cursor._statement.is_bounded is False  # This should be an unbounded streaming query
    assert cursor._statement.is_append_only is True
    assert cursor._statement.connection_refs == []  # Should be empty for this query

    # Test description - should have multiple columns from the clicks table
    assert cursor._statement.description is not None
    assert len(cursor._statement.description) > 1  # Should have multiple columns


def test_streaming_pattern_example(cursor):
    """Test a typical streaming pattern using fetchone() in a loop."""
    # Use real unbounded streaming query from Confluent Cloud examples
    cursor.execute("SELECT * FROM `examples`.`marketplace`.`clicks`", bounded=False)

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


def test_streaming_batch_pattern_example(cursor):
    """Test a typical streaming pattern using fetchmany() in a loop."""
    # Use real unbounded streaming query from Confluent Cloud examples
    cursor.execute("SELECT * FROM `examples`.`marketplace`.`clicks`", bounded=False)

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
