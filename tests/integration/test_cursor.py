from confluent_sql.statement import Phase

"""A one column, one row very fast to complete query."""
SINGLE_ROW_QUERY = "SELECT 42 as answer FROM `INFORMATION_SCHEMA`.`TABLES` LIMIT 1"
# (Queries against INFORMATION_SCHEMA execute very quickly)


def test_cursor_metadata(cursor):
    # 'Cursor.execute' defaults to snapshot queries
    cursor.execute(SINGLE_ROW_QUERY)

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
    cursor.execute(SINGLE_ROW_QUERY, bounded=False)
    assert cursor._statement.is_bounded is True


def test_unbounded_query_with_data(populated_table_connection, test_table_name):
    # For an actual unbounded query, we need to use an actual table that comes from a kafka topic.
    cursor = populated_table_connection.cursor()
    cursor.execute(f"SELECT * FROM {test_table_name}", bounded=False)
    assert cursor._statement.is_bounded is False
