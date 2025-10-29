from confluent_sql.statement import Op


def test_select_one(cursor):
    """Test fetchall."""
    cursor.execute("SELECT 1 as test_value")
    results = cursor.fetchall()
    assert len(results) == 1
    assert results == [{"op": Op.INSERT, "row": {"test_value": "1"}}]


def test_select_multiple_rows(cursor):
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
        {"op": Op.INSERT, "row": {"id": "1", "name": "Alice"}},
        {"op": Op.INSERT, "row": {"id": "2", "name": "Bob"}},
        {"op": Op.INSERT, "row": {"id": "3", "name": "Charlie"}},
    ]


def test_fetchone_iteration(cursor):
    """Test fetchone iteration."""
    cursor.execute("SELECT 1 as test_value")

    # Should get one row
    row = cursor.fetchone()
    assert row == {"op": Op.INSERT, "row": {"test_value": "1",}}

    # Should get None after that
    row = cursor.fetchone()
    assert row is None


def test_fetchmany_iteration(cursor):
    """Test fetchmany iteration."""
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
