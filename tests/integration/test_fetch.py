from confluent_sql.statement import Op


def test_fetchall_gets_all_results(cursor_with_nonstreaming_data):
    results = cursor_with_nonstreaming_data.fetchall()
    assert len(results) == 10


def test_fetchone_returns_none_at_the_end(cursor_with_nonstreaming_data):
    # Exhaust all rows first
    rows = cursor_with_nonstreaming_data.fetchall()
    assert len(rows) == 10

    # Should get None after that
    row = cursor_with_nonstreaming_data.fetchone()
    assert row is None


def test_fetchmany_iteration(cursor_with_nonstreaming_data):
    # Fetch in batches
    batch1 = cursor_with_nonstreaming_data.fetchmany(4)
    assert len(batch1) == 4

    batch2 = cursor_with_nonstreaming_data.fetchmany(4)
    assert len(batch2) == 4

    batch3 = cursor_with_nonstreaming_data.fetchmany(4)
    assert len(batch3) == 2

    batch4 = cursor_with_nonstreaming_data.fetchmany(4)
    assert len(batch4) == 0


def test_cursor_as_iterator(cursor_with_nonstreaming_data):
    i = 0
    for row in cursor_with_nonstreaming_data:
        i += 1
    assert i == 10
