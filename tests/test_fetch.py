"""
Pytest-compatible tests for pagination functionality.
"""

import os
import pytest
import confluent_sql


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


def test_one(connection):
    """Test connection and fetch."""
    cursor = connection.cursor()
    
    try:
        # Simple query
        cursor.execute("SELECT 1 as test_value")
        
        # Test metadata
        assert cursor.statement_name is not None
        assert cursor.sql_kind == "SELECT"
        assert cursor.is_bounded is True
        assert cursor.description is not None
        assert len(cursor.description) == 1
        
        # Test results
        results = cursor.fetchall()
        assert len(results) == 1
        assert results[0] == ("+I", 1)
        
    finally:
        cursor.close()


def test_pagination(connection):
    """Test pagination with multiple rows."""
    cursor = connection.cursor()
    
    try:
        # Multi-row query
        query = """
        SELECT * FROM (
            VALUES 
                (1, 'Alice'),
                (2, 'Bob'), 
                (3, 'Charlie')
        ) AS t(id, name)
        """
        
        cursor.execute(query)
        
        # Test results
        results = cursor.fetchall()
        assert len(results) == 3
        
        # Verify structure
        for row in results:
            assert len(row) == 3  # operation + id + name
            assert row[0] == "+I"  # insert operation
            assert isinstance(row[1], int)  # id
            assert isinstance(row[2], str)  # name
        
    finally:
        cursor.close()


def test_fetchone_iteration(connection):
    """Test fetchone iteration."""
    cursor = connection.cursor()
    
    try:
        cursor.execute("SELECT 1 as value")
        
        # Should get one row
        row = cursor.fetchone()
        assert row == ("+I", 1)
        
        # Should get None after that
        row = cursor.fetchone()
        assert row is None
        
    finally:
        cursor.close()


def test_fetchmany_iteration(connection):
    """Test fetchmany iteration."""
    cursor = connection.cursor()
    
    try:
        # Create multiple rows
        query = """
        SELECT * FROM (
            VALUES 
                (1, 'A'),
                (2, 'B'), 
                (3, 'C'),
                (4, 'D'),
                (5, 'E')
        ) AS t(id, name)
        """
        
        cursor.execute(query)
        
        # Fetch in batches
        batch1 = cursor.fetchmany(2)
        assert len(batch1) == 2
        
        batch2 = cursor.fetchmany(2)
        assert len(batch2) == 2
        
        batch3 = cursor.fetchmany(2)
        assert len(batch3) == 1
        
        batch4 = cursor.fetchmany(2)
        assert len(batch4) == 0
        
    finally:
        cursor.close()


def test_empty_results(connection):
    """Test empty result set."""
    cursor = connection.cursor()
    
    try:
        # Query that returns no rows
        cursor.execute("SELECT 1 WHERE 1 = 0")
        
        results = cursor.fetchall()
        assert len(results) == 0
        
        row = cursor.fetchone()
        assert row is None
        
        batch = cursor.fetchmany(5)
        assert len(batch) == 0
        
    finally:
        cursor.close()


def test_cursor_metadata(connection):
    """Test cursor metadata properties."""
    cursor = connection.cursor()
    
    try:
        cursor.execute("SELECT 42 as answer")
        
        # Test metadata
        assert cursor.statement_name is not None
        assert cursor.sql_kind == "SELECT"
        assert cursor.is_bounded is True
        assert cursor.is_append_only is True
        assert cursor.connection_refs == []
        
        # Test description
        assert cursor.description is not None
        assert len(cursor.description) == 1
        assert cursor.description[0][0] == "answer"
        
    finally:
        cursor.close()
