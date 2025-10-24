from confluent_sql.statement import Phase


def test_cursor_metadata(connection_manager):
    with connection_manager() as connection:
        cursor = connection.cursor()
        # 'Cursor.execute' defaults to bounded (snapshot) query
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

        # Streaming query
        cursor.execute("SELECT 42 as answer", bounded=False)

        assert cursor._statement.is_bounded is False
        # This should be RUNNING on a real, long running, streaming query
        assert cursor._statement.phase is Phase.COMPLETED
        assert cursor._statement.name is not None
        assert cursor._statement.sql_kind == "SELECT"
        assert cursor._statement.is_append_only is True
        assert cursor._statement.connection_refs == []
        assert cursor._statement.description is not None
        assert len(cursor._statement.description) == 1
        assert cursor._statement.description[0][0] == "answer"
