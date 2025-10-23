"""
Pytest-compatible tests for pagination functionality.
"""

def test_one(connection_manager):
    """Test connection and fetch."""
    with connection_manager() as connection:
        cursor = connection.cursor()
        # Simple query
        cursor.execute("SELECT 1 as test_value")

        # Test metadata
        assert cursor._statement.name is not None
        assert cursor._statement.sql_kind == "SELECT"
        assert cursor._statement.is_bounded is True
        assert cursor._statement.description is not None
        assert len(cursor._statement.description) == 1

        # Test results
        results = cursor.fetchall()
        assert len(results) == 1
        assert results[0] == ("+I", "1")


def test_pagination(connection_manager):
    """Test pagination with multiple rows."""
    with connection_manager() as connection:
        cursor = connection.cursor()
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

        results = cursor.fetchall()
        assert len(results) == 3

        assert results == [
            ("+I", "1", "Alice"),
            ("+I", "2", "Bob"),
            ("+I", "3", "Charlie"),
        ]


def test_fetchone_iteration(connection_manager):
    """Test fetchone iteration."""
    with connection_manager() as connection:
        cursor = connection.cursor()

        # This was causing the server to return a syntax error.
        # Could be used on a different test to check that we
        # report the error in a nice way whan that happens.
        # cursor.execute("SELECT 1 as value")

        cursor.execute("SELECT 1 as test_value")

        # Should get one row
        row = cursor.fetchone()
        assert row == ("+I", "1")

        # Should get None after that
        row = cursor.fetchone()
        assert row is None


def test_fetchmany_iteration(connection_manager):
    """Test fetchmany iteration."""
    with connection_manager() as connection:
        cursor = connection.cursor()

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


def test_cursor_metadata(connection_manager):
    """Test cursor metadata properties."""
    with connection_manager() as connection:
        cursor = connection.cursor()
        cursor.execute("SELECT 42 as answer")

        # Test metadata
        assert cursor._statement.name is not None
        assert cursor._statement.sql_kind == "SELECT"
        assert cursor._statement.is_bounded is True
        assert cursor._statement.is_append_only is True
        assert cursor._statement.connection_refs == []

        # Test description
        assert cursor._statement.description is not None
        assert len(cursor._statement.description) == 1
        assert cursor._statement.description[0][0] == "answer"
