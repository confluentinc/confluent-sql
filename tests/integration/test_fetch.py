from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal

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
    @pytest.mark.typeconv
    def test_encoding_decoding_round_tripping(
        self,
        connection: Connection,
    ):
        """Test round-tripping values of all supported types."""
        with connection.closing_cursor(as_dict=True) as cursor:
            cursor.execute(
                """
                SELECT
                    cast(%s as int) AS null_value,
                    %s AS bool_value,
                    %s AS int_value,
                    %s AS bigint_value,
                    cast(%s as DEC(7,2)) as decimal_value,

                    -- These float/double casts are to ensure the types are exactly what we want in
                    -- this projection, rather than relying on implicit typing of literals. They're
                    -- not needed to test the client-side conversion per se.
                    cast(%s as float) as float_value,
                    cast(%s as double) as double_value,

                    %s AS date_value,
                    %s AS time_value,
                    %s AS time_value_micro,
                    %s AS timestamp_value,
                    %s AS timestamp_value_micros,
                    %s AS timestamp_ltz_value,
                    %s AS string_value,
                    %s as varbinary_value
                """,
                (
                    None,  # NULL
                    True,  # BOOLEAN
                    123,  # INT
                    12345678901,  # BIGINT
                    Decimal("12345.67"),  # DECIMAL(7,2)
                    12.5,  # FLOAT
                    191.2342342,  # DOUBLE
                    date(2024, 6, 15),  # DATE
                    time(12, 34, 56),  # TIME
                    time(12, 34, 56, 789000),  # TIME with microseconds
                    datetime(2025, 6, 15, 12, 34, 56),  # TIMESTAMP no microseconds
                    datetime(2024, 6, 15, 12, 34, 56, 123456),  # TIMESTAMP with microseconds
                    datetime(
                        2023, 6, 15, 12, 34, 56, tzinfo=timezone(timedelta(hours=2))
                    ),  # TIMESTAMP_LTZ
                    "test-string",  # STRING
                    b"\x01\x02\x03",  # BINARY
                ),
            )

            results = cursor.fetchone()

            assert results == {
                "null_value": None,
                "bool_value": True,
                "int_value": 123,
                "bigint_value": 12345678901,
                "decimal_value": Decimal("12345.67"),
                "float_value": 12.5,
                "double_value": 191.2342342,
                "date_value": date(2024, 6, 15),
                "time_value": time(12, 34, 56),
                "time_value_micro": time(
                    12, 34, 56
                ),  # Sigh, the microseconds get lost in the round trip, Flink-side.
                "timestamp_value": datetime(2025, 6, 15, 12, 34, 56),  # No micros to lose here.
                "timestamp_value_micros": datetime(2024, 6, 15, 12, 34, 56, 123456),
                # GMT+02:00 gets converted to UTC time in the round trip, so comes
                # out as 10:34:56 UTC.
                "timestamp_ltz_value": datetime(
                    2023, 6, 15, 10, 34, 56, tzinfo=timezone(timedelta(hours=0))
                ),
                "string_value": "test-string",
                "varbinary_value": b"\x01\x02\x03",
            }

    @pytest.mark.slow
    @pytest.mark.typeconv
    def test_rich_result_decoding(
        self,
        connection: Connection,
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
                           
                        cast ('123.323' AS DECIMAL(10,3)),
                        cast ('54.5' as DOUBLE),
                           
                        TRUE,
                           
                        DATE '2024-06-15',
                        TIME '12:34:56',
                        
                        cast('2024-06-15 12:34:56' as TIMESTAMP),
                        cast('2023-06-15 12:34:56' as TIMESTAMP_LTZ), -- will be interp as GMT.
                           
                        cast ('c' as CHAR),
                        cast ('charn' as CHAR(5)),
                        cast ('string' as STRING),
                        cast ('varchar' as VARCHAR),
                        cast ('varcharn' as VARCHAR(10)),
                        
                        cast(x'7f0203' AS VARBINARY),
                           
                        cast(NULL AS INTEGER),
                        cast(NULL AS BOOLEAN),
                        cast(NULL AS STRING)
                    ))
                AS t(
                        tinyint_value,
                        smallint_value,
                        int_value,
                        bigint_value,
                           
                        decimal_value,
                        double_value,
                           
                        bool_value,
                        date_value,
                        time_value,
                           
                        timestamp_value,
                        timestamp_ltz_value,
                           
                        char_value,
                        charn_value,
                        string_value,
                        varchar_value,
                        varcharn_value,
                           
                        varbinary_value,
                           
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
                # Fixed precision type
                "decimal_value": Decimal("123.323"),
                # Floating point type
                "double_value": 54.5,
                # Boolean type
                "bool_value": True,
                # date / time types
                "date_value": date(2024, 6, 15),
                "time_value": time(12, 34, 56),
                "timestamp_value": datetime(2024, 6, 15, 12, 34, 56),
                "timestamp_ltz_value": datetime(
                    2023, 6, 15, 12, 34, 56, tzinfo=timezone(timedelta(hours=0))
                ),
                # String types
                "char_value": "c",
                "charn_value": "charn",
                "string_value": "string",
                "varchar_value": "varchar",
                "varcharn_value": "varcharn",
                # VarBinary type
                "varbinary_value": b"\x7f\x02\x03",
                # Various NULLs
                "null_int_value": None,
                "null_bool_value": None,
                "null_string_value": None,
            }
