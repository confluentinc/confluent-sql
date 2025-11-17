import pytest

from confluent_sql.connection import Connection


@pytest.mark.integration
class TestCursorFetch:
    """Tests over cursor.fetch* methods returning either tuples or dicts."""

    @pytest.mark.parametrize("as_dict", [True, False])
    def test_fetchall_gets_all_results(
        self,
        as_dict: bool,
        cursor_with_nonstreaming_data_factory,
        expected_nonstreaming_results_factory,
    ):
        cursor = cursor_with_nonstreaming_data_factory(as_dict=as_dict)
        results = cursor.fetchall()

        # Will be either list of dicts or list of tuples per 'as_dict' flag.
        assert results == expected_nonstreaming_results_factory(as_dict=as_dict)

    @pytest.mark.parametrize("as_dict", [True, False])
    def test_fetchone_returns_none_at_the_end(
        self,
        cursor_with_nonstreaming_data_factory,
        expected_nonstreaming_results_factory,
        as_dict: bool,
    ):
        cursor = cursor_with_nonstreaming_data_factory(as_dict=as_dict)

        expected_results = expected_nonstreaming_results_factory(as_dict=as_dict)
        # Exhaust all rows first
        for expected_row in expected_results:
            row = cursor.fetchone()
            assert row == expected_row

        # Should get None after that
        row = cursor.fetchone()
        assert row is None

    @pytest.mark.parametrize("as_dict", [True, False])
    def test_fetchmany_iteration(
        self,
        as_dict: bool,
        cursor_with_nonstreaming_data_factory,
        expected_nonstreaming_results_factory,
    ):
        cursor = cursor_with_nonstreaming_data_factory(as_dict=as_dict)
        expected_results = expected_nonstreaming_results_factory(as_dict=as_dict)

        # Fetch in batches
        batch1 = cursor.fetchmany(4)
        assert batch1 == expected_results[:4]

        batch2 = cursor.fetchmany(4)
        assert batch2 == expected_results[4:8]

        batch3 = cursor.fetchmany(4)
        assert batch3 == expected_results[8:10]

        batch4 = cursor.fetchmany(4)
        assert len(batch4) == 0  # No more rows

    @pytest.mark.parametrize("as_dict", [True, False])
    def test_cursor_as_iterator(
        self,
        as_dict: bool,
        cursor_with_nonstreaming_data_factory,
        expected_nonstreaming_results_factory,
    ):
        cursor = cursor_with_nonstreaming_data_factory(as_dict=as_dict)
        expected_results = expected_nonstreaming_results_factory(as_dict=as_dict)

        for row in cursor:
            expected_row = expected_results.pop(0)
            assert row == expected_row

    @pytest.mark.slow
    def test_rich_result_decoding(
        self,
        connection: Connection,
        test_table_name: str,
    ):
        """Test decoding types from a query projecting rich types."""
        with connection.closing_cursor(as_dict=True) as cursor:
            # Select a single row with all of our supported types with representative variations.
            # (Even though is a simple projection, a Flink job will be created
            # to run the query, hence being marked as slow.)
            cursor.execute("""
                SELECT *
                FROM (VALUES (
                        cast(12 AS TINYINT),
                        cast(12345 AS SMALLINT),
                        cast(123 AS INT),
                        cast(12345678901 AS BIGINT),
                           
                        TRUE,
                           
                        cast ('c' as CHAR),
                        cast ('charn' as CHAR(5)),
                        cast ('string' as STRING),
                        cast ('varchar' as VARCHAR),
                        cast ('varcharn' as VARCHAR(10)),
                           
                        cast(NULL AS INTEGER),
                        cast(NULL AS BOOLEAN),
                        cast(NULL AS STRING)
                    ))
                AS t(
                        tinyint_value,
                        smallint_value,
                        int_value,
                        bigint_value,
                           
                        bool_value,
                           
                        char_value,
                        charn_value,
                        string_value,
                        varchar_value,
                        varcharn_value,
                           
                           
                        null_int_value,
                        null_bool_value,
                        null_string_value
                    )
                """)

            results = cursor.fetchone()

            assert results == {
                # Numeric types
                "tinyint_value": 12,
                "smallint_value": 12345,
                "int_value": 123,
                "bigint_value": 12345678901,
                # Boolean type
                "bool_value": True,
                # String types
                "char_value": "c",
                "charn_value": "charn",
                "string_value": "string",
                "varchar_value": "varchar",
                "varcharn_value": "varcharn",
                # Various NULLs
                "null_int_value": None,
                "null_bool_value": None,
                "null_string_value": None,
            }
