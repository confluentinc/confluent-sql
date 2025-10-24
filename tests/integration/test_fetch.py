def test_select_one(connection_manager):
    """Test connection and fetch."""
    with connection_manager() as connection:
        cursor = connection.cursor()

        # Simple query
        cursor.execute("SELECT 1 as test_value")

        # Test results
        results = cursor.fetchall()
        assert len(results) == 1
        assert results == [("+I", ("1",))]


def test_select_multiple_rows(connection_manager):
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
            ("+I", ("1", "Alice")),
            ("+I", ("2", "Bob")),
            ("+I", ("3", "Charlie")),
        ]


def test_fetchone_iteration(connection_manager):
    """Test fetchone iteration."""
    with connection_manager() as connection:
        cursor = connection.cursor()

        cursor.execute("SELECT 1 as test_value")

        # Should get one row
        row = cursor.fetchone()
        assert row == ("+I", ("1", ))

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
