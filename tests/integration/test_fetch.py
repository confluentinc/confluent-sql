from collections import Counter, namedtuple
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from os import environ
from typing import NamedTuple

import pytest

from confluent_sql import Connection, SqlNone, YearMonthInterval
from confluent_sql.execution_mode import ExecutionMode


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
    def test_encoding_decoding_scalars_round_tripping(
        self,
        connection: Connection,
    ):
        """Test round-tripping values the simple scalar supported types."""
        with connection.closing_cursor(as_dict=True) as cursor:
            cursor.execute(
                """
                SELECT
                    %s AS nulled_int_value,
                    %s AS nulled_string_value,
                    %s as nulled_bool_value,
                    %s AS bool_value,
                    %s AS int_value,
                    %s AS bigint_value,
                    %s as decimal_value,

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

                    %s as simple_day_second_interval,
                    %s as microseconds_day_second_interval,
                    %s as negative_day_second_interval,

                    %s as positive_year_month_interval,
                    %s as negative_year_month_interval,
                    %s as zero_year_month_interval,

                    %s AS string_value,
                    %s as varbinary_value
                """,
                (
                    SqlNone.INTEGER,  # null-as-int-cast via constant.
                    SqlNone(str),  # null-as-string-cast via construct null from python str type
                    SqlNone("BOOLEAN"),  # null-as-bool-cast via explicit type name
                    True,  # BOOLEAN
                    123,  # INT
                    12345678901,  # BIGINT
                    Decimal("12345.67"),  # DECIMAL
                    12.5,  # FLOAT
                    191.2342342,  # DOUBLE
                    date(2024, 6, 15),  # DATE
                    time(12, 34, 56),  # TIME
                    time(12, 34, 56, 789000),  # TIME with microseconds
                    # datetimes
                    datetime(2025, 6, 15, 12, 34, 56),  # TIMESTAMP no microseconds
                    datetime(2024, 6, 15, 12, 34, 56, 123456),  # TIMESTAMP with microseconds
                    datetime(
                        2023, 6, 15, 12, 34, 56, tzinfo=timezone(timedelta(hours=2))
                    ),  # TIMESTAMP_LTZ
                    # DAY TO SECOND Intervals
                    timedelta(days=2, hours=3, minutes=4, seconds=5),  # INTERVAL DAY TO SECOND
                    timedelta(days=1, hours=2, minutes=3, seconds=4, microseconds=567890),
                    timedelta(days=-1, hours=-2, minutes=-3, seconds=-4),
                    # YEAR TO MONTH Intervals
                    YearMonthInterval(years=3, months=4),  # Positive interval
                    YearMonthInterval(years=-2, months=-5),  # Negative interval
                    YearMonthInterval(years=0, months=9),  # Zero year interval
                    # Strings
                    "test-string",  # STRING
                    b"\x01\x02\x03",  # BINARY
                ),
            )

            results = cursor.fetchone()

            assert results == {
                "nulled_int_value": None,
                "nulled_string_value": None,
                "nulled_bool_value": None,
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
                "simple_day_second_interval": timedelta(days=2, hours=3, minutes=4, seconds=5),
                # Alas, microseconds get truncated to milliseconds precision Flink-side.
                "microseconds_day_second_interval": timedelta(
                    days=1, hours=2, minutes=3, seconds=4, microseconds=567000
                ),
                "negative_day_second_interval": timedelta(
                    days=-1, hours=-2, minutes=-3, seconds=-4
                ),
                "positive_year_month_interval": YearMonthInterval(years=3, months=4),
                "negative_year_month_interval": YearMonthInterval(years=-2, months=-5),
                "zero_year_month_interval": YearMonthInterval(years=0, months=9),
                "string_value": "test-string",
                "varbinary_value": b"\x01\x02\x03",
            }

    @pytest.mark.slow
    @pytest.mark.typeconv
    def test_decoding_intervals(
        self,
        connection: Connection,
    ):
        """Test decoding for intervals, a whole big thing on their own."""
        with connection.closing_cursor(as_dict=True) as cursor:
            cursor.execute(
                """
                SELECT
                    INTERVAL '4' HOUR AS interval_hour,
                    INTERVAL '2 04:15:20' DAY TO SECOND AS interval_day_time,
                    INTERVAL '1 22:22:22.123456' DAY TO SECOND(6) AS interval_day_time_micros,
                    INTERVAL '-23' HOUR AS interval_neg_hour,
                    -- An iota longer negative interval with fractional seconds.
                    INTERVAL '-23:00:00.123' HOUR TO SECOND AS interval_neg_hour_to_second,
                    -- And now some months, years intervals.
                    INTERVAL '1' YEAR AS interval_year,
                    INTERVAL '0' MONTH AS interval_zero_month,
                    INTERVAL '7-11' YEAR TO MONTH AS interval_year_month,
                    INTERVAL '-0-4' YEAR TO MONTH AS interval_neg_zero_year_month,
                    INTERVAL '-2-11' YEAR TO MONTH AS interval_neg_overflow_year_month
                """
            )

            results = cursor.fetchone()

            assert results == {
                "interval_hour": timedelta(hours=4),
                "interval_day_time": timedelta(days=2, hours=4, minutes=15, seconds=20),
                # Sigh, the microseconds get truncated to milliseconds in the round trip,
                # Flink-side, even though we specified DAY TO SECOND(6).
                # (Expect this so that we can reëvaluate later if it improves in Flink.)
                "interval_day_time_micros": timedelta(
                    days=1, hours=22, minutes=22, seconds=22, microseconds=123000
                ),
                "interval_neg_hour": timedelta(hours=-23),
                "interval_neg_hour_to_second": (-1 * timedelta(hours=23, microseconds=123000)),
                "interval_year": YearMonthInterval(years=1, months=0),
                "interval_zero_month": YearMonthInterval(years=0, months=0),
                "interval_year_month": YearMonthInterval(years=7, months=11),
                "interval_neg_zero_year_month": YearMonthInterval(years=0, months=-4),
                "interval_neg_overflow_year_month": YearMonthInterval(years=-2, months=-11),
            }

    @pytest.mark.slow
    @pytest.mark.typeconv
    def test_decoding_multiset(
        self,
        connection: Connection,
    ):
        """Test decoding for multiset type."""
        # There is no supported way to construct multiset literals in Flink SQL
        # at this time, but we can construct one via COLLECT aggregation over
        # a set of rows.

        # The multiset is mapped to a Python-side collections.Counter.
        with connection.closing_cursor(as_dict=True) as cursor:
            # Two apples, two bananas, one orange.
            cursor.execute(
                """
                WITH fruits AS (
                    SELECT *
                    FROM (VALUES
                        ('apple'),
                        ('banana'),
                        ('apple'),
                        ('orange'),
                        ('banana')
                    ) AS t(fruit)
                )

                SELECT COLLECT (fruit) AS fruit_multiset
                FROM fruits
                """
            )

            results = cursor.fetchone()

            assert results == {
                "fruit_multiset": Counter({"apple": 2, "banana": 2, "orange": 1}),
            }

    @pytest.mark.slow
    @pytest.mark.typeconv
    def test_decoding_empty_multiset(
        self,
        connection: Connection,
    ):
        """Test decoding an empty multiset."""
        # There is no supported way to construct multiset literals in Flink SQL
        # at this time, but we can construct one via COLLECT aggregation over
        # a set of rows.

        # The multiset is mapped to an empty Python-side collections.Counter.
        with connection.closing_cursor(as_dict=True) as cursor:
            # The single NULL value will be ignored by COLLECT, resulting in an empty multiset.
            cursor.execute(
                """
                WITH fruits AS (
                    SELECT *
                    FROM (VALUES
                        (cast(NULL AS STRING))
                    ) AS t(fruit)
                )

                SELECT COLLECT (fruit) AS fruit_multiset
                FROM fruits
                """
            )

            results = cursor.fetchone()

            assert results == {
                "fruit_multiset": Counter(),
            }

    @pytest.mark.slow
    @pytest.mark.typeconv
    def test_null_multiset(
        self,
        connection: Connection,
    ):
        """Test SQLNone(MULTISET<type>) + decoding a NULL multiset."""

        null_ms = SqlNone("MULTISET<STRING>")
        with connection.closing_cursor(as_dict=True) as cursor:
            cursor.execute(
                """
                SELECT %s AS null_string_multiset
                """,
                (null_ms,),
            )

            results = cursor.fetchone()

            assert results == {
                "null_string_multiset": None,
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

    @pytest.mark.slow
    @pytest.mark.typeconv
    def test_sqlnone_constants(
        self,
        connection: Connection,
    ):
        """Test that the None -> typed NULL constants work as expected."""
        with connection.closing_cursor(as_dict=True) as cursor:
            cursor.execute(
                """
                SELECT
                    %s AS null_int,
                    %s as null_varchar,
                    %s as null_string,
                    %s as null_varbinary,
                    %s as null_bool,
                    %s as null_decimal,
                    %s as null_float,
                    %s as null_date,
                    %s as null_time,
                    %s as null_timestamp,
                    %s as null_year_month_interval,
                    %s as null_day_second_interval
                """,
                (
                    SqlNone.INTEGER,
                    SqlNone.VARCHAR,
                    SqlNone.STRING,
                    SqlNone.VARBINARY,
                    SqlNone.BOOLEAN,
                    SqlNone.DECIMAL,
                    SqlNone.FLOAT,
                    SqlNone.DATE,
                    SqlNone.TIME,
                    SqlNone.TIMESTAMP,
                    SqlNone.YEAR_MONTH_INTERVAL,
                    SqlNone.DAY_SECOND_INTERVAL,
                ),
            )

            results = cursor.fetchone()

            assert results == {
                "null_int": None,
                "null_varchar": None,
                "null_string": None,
                "null_bool": None,
                "null_varbinary": None,
                "null_decimal": None,
                "null_float": None,
                "null_date": None,
                "null_time": None,
                "null_timestamp": None,
                "null_year_month_interval": None,
                "null_day_second_interval": None,
            }


@pytest.fixture(scope="session")
def auto_dropped_table_name_factory(connection):
    """Fixture factory that returns a callable that returns table names to use in tests.
    All the generated named tables will be dropped at the end of the test session.
    """

    tables_to_drop = []
    today = date.today()
    username = environ.get("USER", "unknown_user").lower()

    def _table_name_generator() -> str:
        """Generate a unique table name for testing."""
        extension = f"{today.day}_{len(tables_to_drop) + 1}"
        tname = f"confluentsql_pytest_{username}_test_auto_drop_table_{extension}"
        tables_to_drop.append(tname)
        return tname

    yield _table_name_generator

    # Teardown: drop all created tables.
    with connection.closing_cursor() as cursor:
        for table_name in tables_to_drop:
            try:
                cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            except Exception as e:
                print(f"Error dropping table {table_name} during teardown: {e}")


AutoDroppedTableNameFactory = Callable[[], str]


@pytest.fixture()
def auto_dropped_table_name(
    auto_dropped_table_name_factory: AutoDroppedTableNameFactory,
) -> str:
    """Fixture that provides a single auto-dropped table name for a test."""
    return auto_dropped_table_name_factory()


@pytest.mark.integration
@pytest.mark.slow
class TestExecuteDDL:
    def test_execute_streaming_ddl_leaves_statement_running(
        self, connection: Connection, auto_dropped_table_name: str
    ):
        """Prove that execute_streaming_ddl() leaves the statement in running state."""

        statement = None
        try:
            # Make a CREATE TABLE AS SELECT statement that will run as a streaming DDL.
            statement_text = f"""
                CREATE TABLE `{auto_dropped_table_name}` as
                    SELECT * from `sample_data_stock_trades` where quantity > 100
            """

            # execute_streaming_ddl() should ensure to return when the statement is running,
            # and that it submits a streaming / not snapshot mode statement.
            statement = connection.execute_streaming_ddl(statement_text)

            # Returned statement be running, not snapshot, and not deleted.
            assert statement.is_running
            # Grr, Flink-side bug as of Jan 2026 means CTAS streaming DDLs are marked as bounded.
            # Hopefully we get to uncomment this later.

            # assert statement.is_unbounded

            # for now though, just as an alert when the bug gets fixed, set up a line
            # that will break the test if the statement ever becomes unbounded as expected.
            assert statement.is_bounded, (
                "Alert: Flink CTAS streaming DDL statements are now marked as unbounded;"
                " please update the codebase accordingly."
            )

            assert not statement.is_deleted
        finally:
            if statement is not None:
                # Satisfied. Now explicitly delete the statement to clean up the running job.
                connection.delete_statement(statement)
                assert statement.is_deleted

        # The table will be dropped by the fixture teardown.

    def test_execute_snapshot_ddl_ctas_submits_finite_statement(
        self, connection: Connection, auto_dropped_table_name: str
    ):
        """Prove that execute_snapshot_ddl() CTAS submits a completable statement."""

        # Make a CREATE TABLE AS SELECT statement that will run as snapshot DDL.
        statement_text = f"""
            CREATE TABLE `{auto_dropped_table_name}` as
                SELECT * from `sample_data_stock_trades` where quantity > 100
                limit 10
        """

        # execute_snapshot_ddl() should return when the statement is completed.
        statement = connection.execute_snapshot_ddl(statement_text)

        # Returned statement should be completed, since was a finite snapshot statement
        # (even though was CTAS -- submitting as snapshot should take precedence).
        assert not statement.is_running
        # And should have been a bounded statement
        assert statement.is_bounded
        # And have been deleted after completion.
        assert statement.is_deleted

        # For kicks, should have created the table and we can select from it.
        with connection.closing_cursor(as_dict=True) as cursor:
            cursor.execute(f"SELECT COUNT(*) AS row_count FROM `{auto_dropped_table_name}`")
            results = cursor.fetchone()
            assert results == {"row_count": 10}

    def test_execute_ctas_streaming_mode_returns_at_running(
        self, connection: Connection, auto_dropped_table_name: str
    ):
        """Test that CTAS in streaming mode returns when statement is RUNNING.

        This is in contrast to snapshot mode where CTAS waits until COMPLETED.
        Due to a current server bug, streaming CTAS is incorrectly reported as bounded,
        but our execution mode logic should handle this correctly.
        """
        statement = None
        try:
            # Create a CTAS statement in streaming mode (no LIMIT clause)
            statement_text = f"""
                CREATE TABLE `{auto_dropped_table_name}` AS
                SELECT * FROM `sample_data_stock_trades`
                WHERE quantity > 100
            """

            # Use streaming cursor to execute the CTAS
            with connection.closing_cursor(mode=ExecutionMode.STREAMING_QUERY) as cursor:
                cursor.execute(statement_text, timeout=10)
                statement = cursor.statement

                # In streaming mode, the cursor should return when the statement is RUNNING
                assert statement.is_running, "Statement should be in RUNNING state"
                assert statement.phase.value != "COMPLETED", "Statement should not be completed yet"

                # Due to the bug, it's currently reported as bounded
                # When the bug is fixed, this assertion should be updated to:
                # assert not statement.is_bounded
                assert statement.is_bounded, "CTAS currently misreported as bounded"

        finally:
            # Clean up: stop and delete the statement
            if statement and not statement.is_deleted:
                with suppress(Exception):
                    connection.delete_statement(statement)


@pytest.mark.integration
class TestArrayStatements:
    """Tests over ARRAY type encoding/decoding."""

    @pytest.mark.slow
    @pytest.mark.typeconv
    def test_encoding_decoding_array_round_tripping(
        self,
        connection: Connection,
    ):
        """Test round-tripping values for ARRAY type."""
        with connection.closing_cursor(as_dict=True) as cursor:
            cursor.execute(
                """
                SELECT
                    %s AS int_array_value,
                    %s AS string_array_value,
                    %s as nested_string_array,
                    %s AS null_array_value
                """,
                (
                    [1, 2, 3, 4, 5],  # ARRAY of INT
                    ["one", "two", "three"],  # ARRAY of STRING
                    [["one", "two"], ["three", "four"]],  # NESTED ARRAY of STRING
                    SqlNone("Array<int>"),  # NULL ARRAY
                ),
            )

            results = cursor.fetchone()

            assert results == {
                "int_array_value": [1, 2, 3, 4, 5],
                "string_array_value": ["one", "two", "three"],
                "nested_string_array": [["one", "two"], ["three", "four"]],
                "null_array_value": None,
            }


@pytest.mark.integration
class TestMapStatements:
    """Tests over MAP type encoding/decoding."""

    @pytest.mark.slow
    @pytest.mark.typeconv
    def test_encoding_decoding_map_round_tripping(
        self,
        connection: Connection,
    ):
        """Test round-tripping values for MAP type."""
        with connection.closing_cursor(as_dict=True) as cursor:
            cursor.execute(
                """
                SELECT
                    %s AS string_int_map,
                    %s as int_string_map,
                    %s as string_null_int_map,
                    %s AS string_array_of_string_map,
                    %s AS nested_map,
                    %s AS null_map
                """,
                (
                    {"key1": 10, "key2": 20, "key3": 30},  # MAP of string -> INT values
                    {10: "ten", 20: "twenty"},  # MAP of INT -> STRING values
                    {"key1": 10, "null_key": None},  # MAP string -> NULLABLE INT value
                    {
                        "fruits": ["apple", "banana"],
                        "vegetables": ["carrot", "broccoli"],
                    },  # MAP of string -> ARRAY of string
                    {  # NESTED MAP
                        "outer_key1": {"inner_key1": 1, "inner_key2": 2},
                        "outer_key2": {"inner_key3": 3, "inner_key4": 4},
                    },
                    SqlNone("Map<string, int>"),  # NULL MAP
                ),
            )

            results = cursor.fetchone()

            assert results == {
                "string_int_map": {"key1": 10, "key2": 20, "key3": 30},
                "int_string_map": {10: "ten", 20: "twenty"},
                "string_null_int_map": {"key1": 10, "null_key": None},
                "string_array_of_string_map": {
                    "fruits": ["apple", "banana"],
                    "vegetables": ["carrot", "broccoli"],
                },
                "nested_map": {
                    "outer_key1": {"inner_key1": 1, "inner_key2": 2},
                    "outer_key2": {"inner_key3": 3, "inner_key4": 4},
                },
                "null_map": None,
            }


@pytest.mark.integration
@pytest.mark.slow
@pytest.mark.typeconv
class TestRowConversion:
    """Tests over ROW type encoding/decoding."""

    def test_encoding_decoding_rows(
        self,
        row_table_connection: Connection,
        test_row_table_name: str,
    ):
        """Test inserting and selecting ROW type values, including arrays and nested ROW types."""

        # See fixture `row_table_connection` for the table structure with ROW columns
        # matching these types.
        NameAndAge = namedtuple("NameAndAge", ["name", "age"])
        """Example of using collections.namedtuple for ROW type."""

        class Address(NamedTuple):
            """Example of using a typing.NamedTuple for ROW type."""

            street: str
            city: str
            zip_code: int

        @dataclass
        class ContactInfo:
            """Example of using a dataclass for ROW type."""

            email: str
            phone: str

        EmbeddedRow = namedtuple("EmbeddedRow", ["id", "contact_info"])

        # Register the types so that they'll get used when deserializing. Otherwise
        # will be using auto-generated namedtuples with same field names, but won't
        # be the same exact class. The user may care and hence will do this explicitly.

        # We choose NOT to register the EmbeddedRow here, to verify that
        # namedtuples get auto-generated properly when processing statement
        # result sets.
        for nt in [NameAndAge, Address, ContactInfo]:
            row_table_connection.register_row_type(nt)

        nameAndAge = NameAndAge(name="John Doe", age=28)
        addressArray = [
            # Don't model ZIP codes as integers in real life, but for test purposes here it's fine.
            # (This is why people in Maine hate Excel auto-guessing ZIP codes as numbers, eating
            #  their leading zeros.)
            Address(street="123 Main St", city="Anytown", zip_code=12345),
            Address(street="456 Oak Ave", city="Othertown", zip_code=67890),
        ]
        embeddedRow = EmbeddedRow(
            id=1,
            contact_info=ContactInfo(email="joe@joetown.com", phone="555-1234"),
        )

        # Insert a full row value.
        with row_table_connection.closing_cursor(as_dict=True) as cursor:
            cursor.execute(
                f"""
                INSERT into {test_row_table_name} (name_and_age, address_array, embedded_row)
                VALUES (%s, %s, %s)
                """,
                (nameAndAge, addressArray, embeddedRow),
            )

        # Now select it back out and verify we get the same values.
        with row_table_connection.closing_cursor(as_dict=True) as cursor:
            cursor.execute(
                f"""
                SELECT name_and_age, address_array, embedded_row
                FROM {test_row_table_name}
                WHERE name_and_age.name = %s
                """,
                (nameAndAge.name,),
            )

            results = cursor.fetchone()

            assert results == {
                "name_and_age": nameAndAge,
                "address_array": addressArray,
                "embedded_row": embeddedRow,
            }

            # Ensure the types are as expected since we registered them.
            # (The above assert only checks value equality, not type identity, and even
            #  bare tuples can compare equal to namedtuples with same values.)
            assert isinstance(results["name_and_age"], NameAndAge)  # type: ignore
            assert isinstance(results["address_array"][0], Address)  # type: ignore
            assert isinstance(results["address_array"][1], Address)  # type: ignore

            # EmbeddedRow was not registered, so will be an auto-generated namedtuple.
            # It will have the right field names, but not be the same class.
            assert results["embedded_row"].__class__.__name__ == "Row"  # type: ignore
            # However, its nested ContactInfo was registered, so will be of that type.
            assert isinstance(results["embedded_row"].contact_info, ContactInfo)  # type: ignore

    def test_encoding_decoding_null_row(
        self,
        connection: Connection,
    ):
        """Test encoding and decoding NULLs of ROW type."""

        # Sigh, a NULL ROW type has no way to specify its complete structure.
        arbitrary_row_null = SqlNone("ROW<name STRING, age INT, `foo_bar_BLAT` BOOLEAN>")

        with connection.closing_cursor(as_dict=True) as cursor:
            cursor.execute(
                """
                SELECT %s AS null_row_value
                """,
                (arbitrary_row_null,),
            )

            results = cursor.fetchone()

            assert results == {
                "null_row_value": None,
            }
