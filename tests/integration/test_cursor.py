from confluent_sql.statement import Phase


def test_cursor_metadata(cursor):
    # 'Cursor.execute' defaults to snapshot queries
    cursor.execute("SELECT 42 as answer")

    assert cursor._statement.is_bounded is True
    assert cursor._statement.phase is Phase.COMPLETED
    assert cursor._statement.name is not None
    assert cursor._statement.sql_kind == "SELECT"
    assert cursor._statement.is_append_only is True
    assert cursor._statement.connection_refs == []
    assert cursor._statement.description is not None
    assert len(cursor._statement.description) == 1
    assert cursor._statement.description[0][0] == "answer"


def test_unbounded_query_with_finite_statement(cursor):
    # Even if we set `bounded` to False here, we still get
    # a bounded statement since it ends and no sources are
    # generating more input
    cursor.execute("SELECT 42 as answer", bounded=False)
    assert cursor._statement.is_bounded is True

def test_unbounded_query_with_data(table_connection_with_data, test_table_name):
    # For an actual unbounded query, we need to use an actual table that comes from a kafka topic.
    cursor = table_connection_with_data.cursor()
    cursor.execute(f"SELECT * FROM {test_table_name}", bounded=False)
    assert cursor._statement.is_bounded is False

