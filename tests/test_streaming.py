#!/usr/bin/env python3
"""
Test streaming query functionality for confluent-sql library.
"""

import os
import pytest
import confluent_sql
from confluent_sql.exceptions import InterfaceError


@pytest.fixture
def connection():
    """Create a connection for testing."""
    flink_api_key = os.getenv("FLINK_API_KEY")
    flink_api_secret = os.getenv("FLINK_API_SECRET")
    environment = os.getenv("ENV_ID")
    organization_id = os.getenv("ORG_ID")
    compute_pool_id = os.getenv("COMPUTE_POOL_ID")
    cloud_provider = os.getenv("CLOUD_PROVIDER", "aws")
    cloud_region = os.getenv("CLOUD_REGION", "us-east-2")
    
    if not all([flink_api_key, flink_api_secret, environment, organization_id, compute_pool_id, cloud_region, cloud_provider]):
        pytest.skip("Missing required environment variables for integration test")
    
    return confluent_sql.connect(
        flink_api_key=flink_api_key,
        flink_api_secret=flink_api_secret,
        environment=environment,
        organization_id=organization_id,
        compute_pool_id=compute_pool_id,
        cloud_region=cloud_region,
        cloud_provider=cloud_provider
    )


def test_unbounded_query_fetchall_error(connection):
    """Test that fetchall() raises an error for unbounded queries."""
    cursor = connection.cursor()
    
    try:
        # Use real unbounded streaming query from Confluent Cloud examples
        cursor.execute("SELECT * FROM `examples`.`marketplace`.`clicks`")
        
        # Should raise an error for unbounded queries
        with pytest.raises(InterfaceError) as exc_info:
            cursor.fetchall()
        
        assert "fetchall() is not supported for unbounded streaming queries" in str(exc_info.value)
        assert "Use fetchone() or fetchmany() in a loop" in str(exc_info.value)
        
    finally:
        cursor.close()


def test_streaming_query_fetchone(connection):
    """Test that fetchone() works for streaming queries (yielding iterator)."""
    cursor = connection.cursor()
    
    try:
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
            print(f"  Received streaming row {rows_received}: {row}")
            
            # Verify the row structure (raw data without changelog operations)
            assert len(row) > 0  # Should have data columns
            # No changelog operations - just raw data
        
        print(f"Received {rows_received} streaming rows")
        
    finally:
        cursor.close()


def test_streaming_query_fetchmany(connection):
    """Test that fetchmany() works for streaming queries (yielding iterator)."""
    cursor = connection.cursor()
    
    try:
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
            print(f"  Received streaming batch {batch_count}: {len(batch)} rows")
            
            # Verify the batch structure (raw data without changelog operations)
            for row in batch:
                assert len(row) > 0  # Should have data columns
                # No changelog operations - just raw data
        
        print(f"Received {total_rows} streaming rows in {batch_count} batches")
        
    finally:
        cursor.close()


def test_cursor_metadata_streaming(connection):
    """Test cursor metadata for streaming queries."""
    cursor = connection.cursor()
    
    try:
        # Use real unbounded streaming query from Confluent Cloud examples
        cursor.execute("SELECT * FROM `examples`.`marketplace`.`clicks`")
        
        # Test metadata
        assert cursor.statement_name is not None
        assert cursor.sql_kind == "SELECT"
        assert cursor.is_bounded is False  # This should be an unbounded streaming query
        assert cursor.is_append_only is True
        assert cursor.connection_refs == []  # Should be empty for this query
        
        # Test description - should have multiple columns from the clicks table
        assert cursor.description is not None
        assert len(cursor.description) > 1  # Should have multiple columns
        
        print(f"Streaming query metadata:")
        print(f"  Statement: {cursor.statement_name}")
        print(f"  SQL Kind: {cursor.sql_kind}")
        print(f"  Is Bounded: {cursor.is_bounded}")
        print(f"  Is Append Only: {cursor.is_append_only}")
        print(f"  Connection Refs: {cursor.connection_refs}")
        print(f"  Columns: {[col[0] for col in cursor.description]}")
        
    finally:
        cursor.close()


def test_streaming_pattern_example(connection):
    """Test a typical streaming pattern using fetchone() in a loop."""
    cursor = connection.cursor()
    
    try:
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
        
    finally:
        cursor.close()


def test_streaming_batch_pattern_example(connection):
    """Test a typical streaming pattern using fetchmany() in a loop."""
    cursor = connection.cursor()
    
    try:
        # Use real unbounded streaming query from Confluent Cloud examples
        cursor.execute("SELECT * FROM `examples`.`marketplace`.`clicks`")
        
        # Simulate streaming batch pattern
        print("Streaming batch pattern example:")
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
            print(f"  Received batch {batch_count} of {len(batch)} rows")
            
            # Show first row of each batch for debugging
            if batch:
                print(f"    First row: {batch[0]}")
        
        print(f"Total streaming rows received: {total_rows} in {batch_count} batches")
        
    finally:
        cursor.close()


if __name__ == "__main__":
    # Run tests if called directly
    pytest.main([__file__, "-v"])
