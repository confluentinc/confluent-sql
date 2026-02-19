"""Integration test pytest configuration and fixtures."""

import logging
import os
from collections.abc import Callable, Generator
from pathlib import Path
from typing import Any

import pytest
from dotenv import load_dotenv

import confluent_sql
from confluent_sql.connection import Connection
from confluent_sql.cursor import Cursor

logger = logging.getLogger(__name__)


def pytest_runtest_setup(item):
    """Ensure that all tests within integration/ are marked as integration tests."""
    is_integration = any(item.iter_markers(name="integration"))
    if not is_integration:
        pytest.fail("Tests within 'integration/' must be marked with @pytest.mark.integration.")


@pytest.fixture(scope="session")
def load_env_file():
    """Load environment variables from .env file if it exists in the current working directory.

    This fixture runs once per test session and loads any environment variables
    from a .env file before any tests run. This allows developers to store
    integration test credentials locally without committing them to source control.
    """
    env_file = Path.cwd() / ".env"
    if env_file.exists():
        logger.info(f"Loading environment variables from {env_file}")
        load_dotenv(env_file)
    else:
        logger.debug(f"No .env file found at {env_file}")


@pytest.fixture(scope="session")
def connection(load_env_file) -> Generator[Connection, Any, None]:
    """
    Create a real connection for test suite run.

    Will automatically close at the end of the session.

    This uses real api keys (from env).
    """
    flink_api_key = os.getenv("CONFLUENT_FLINK_API_KEY", "")
    flink_api_secret = os.getenv("CONFLUENT_FLINK_API_SECRET", "")
    environment = os.getenv("CONFLUENT_ENV_ID", "")
    organization_id = os.getenv("CONFLUENT_ORG_ID", "")
    compute_pool_id = os.getenv("CONFLUENT_COMPUTE_POOL_ID", "")
    cloud_provider = os.getenv("CONFLUENT_CLOUD_PROVIDER", "")
    cloud_region = os.getenv("CONFLUENT_CLOUD_REGION", "")
    dbname = os.getenv("CONFLUENT_TEST_DBNAME", "")

    if not all(
        [
            flink_api_key,
            flink_api_secret,
            environment,
            organization_id,
            compute_pool_id,
            cloud_region,
            cloud_provider,
            dbname,
        ]
    ):
        pytest.skip("Missing required environment variables for integration test")

    conn = confluent_sql.connect(
        flink_api_key=flink_api_key,
        flink_api_secret=flink_api_secret,
        environment=environment,
        organization_id=organization_id,
        compute_pool_id=compute_pool_id,
        cloud_region=cloud_region,
        cloud_provider=cloud_provider,
        dbname=dbname,
    )

    yield conn

    conn.close()


@pytest.fixture()
def dbname():
    """Returns the database name used for integration tests."""
    dbname = os.getenv("CONFLUENT_TEST_DBNAME", "")
    if not dbname:
        pytest.skip("Missing CONFLUENT_TEST_DBNAME environment variable for integration test")

    return dbname


@pytest.fixture()
def single_test_connection(
    connection_factory: Callable[..., Connection],
) -> Connection:
    """
    Returns a new connection for a single test.

    This connection is closed at the end of the test (via the connection_factory fixture).
    """
    return connection_factory()


@pytest.fixture(scope="session")
def test_table_name():
    """Returns a table name to use for the table to populate for testing."""
    # Returns a fixed name for the test table, but if we have to adopt
    # pytest-xdist, we could include the worker id in the name to avoid collisions.
    return "pytest_table"


@pytest.fixture
def cursor(connection: Connection):
    """
    Returns a mode=ExecutionMode.SNAPSHOT tuple-returning cursor for the shared connection.
    Closes the cursor at the end of the test.

    This cursor is unique for each test, even if they all share the same connection.
    """
    cursor = connection.cursor()

    yield cursor

    try:
        cursor.close()
    except Exception as e:
        logger.error(f"Error closing cursor in fixture: {e}")


@pytest.fixture(scope="session")
def table_connection(connection: Connection, test_table_name: str):
    """
    This fixture takes the shared connection, and adds a table to it.

    This is scoped for the whole session, so the table will be created
    only once when this fixture is used for the first time.
    Drops the table at the end, but also before creating it in case
    it wasn't dropped in a previous run for any reason.
    """
    cursor = connection.cursor()

    # First delete the table if it was left here for any reason
    cursor.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    # Then create it from scratch. Will have 10 total columns.
    # (The 10-ness will be useful later when we query INFORMATION_SCHEMA.COLUMNS for the table)
    cursor.execute(
        f"""CREATE TABLE {test_table_name} (
            `c1` BIGINT,
            `c2` STRING,
            `c3` STRING,
            `c4` STRING,
            `c5` STRING,
            `c6` STRING,
            `c7` STRING,
            `c8` STRING,
            `c9` STRING,
            `c10` STRING
        )"""
    )
    cursor.close()

    yield connection

    # Drop the table at the end of the session if all went well
    cursor = connection.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {test_table_name}")
    cursor.close()


@pytest.fixture(scope="session")
def populated_table_connection(table_connection: Connection, test_table_name: str):
    """This fixture adds some data to the table before serving the shared connection."""
    with table_connection.closing_cursor() as cursor:
        cursor.execute(
            f"""
        INSERT INTO {test_table_name}
        VALUES
            (1, 'name1', 'name1', 'name1', 'name1', 'name1', 'name1', 'name1', 'name1', 'name1'),
            (2, 'name2', 'name2', 'name2', 'name2', 'name2', 'name2', 'name2', 'name2', 'name2'),
            (3, 'name3', 'name3', 'name3', 'name3', 'name3', 'name3', 'name3', 'name3', 'name3'),
            (4, 'name4', 'name4', 'name4', 'name4', 'name4', 'name4', 'name4', 'name4', 'name4'),
            (5, 'name5', 'name5', 'name5', 'name5', 'name5', 'name5', 'name5', 'name5', 'name5'),
            (6, 'name6', 'name6', 'name6', 'name6', 'name6', 'name6', 'name6', 'name6', 'name6'),
            (7, 'name7', 'name7', 'name7', 'name7', 'name7', 'name7', 'name7', 'name7', 'name7'),
            (8, 'name8', 'name8', 'name8', 'name8', 'name8', 'name8', 'name8', 'name8', 'name8'),
            (9, 'name9', 'name9', 'name9', 'name9', 'name9', 'name9', 'name9', 'name9', 'name9'),
            (10, 'name10', 'name10', 'name10', 'name10', 'name10', 'name10', 'name10', 'name10',
                        'name10')
        """
        )

    yield table_connection


@pytest.fixture(scope="session")
def populated_table_rowcount() -> int:
    """Returns the number of rows inserted into the test table by the populated_table_connection
    fixture."""
    return 10


@pytest.fixture
def cursor_with_nonstreaming_data_factory(
    table_connection: Connection, test_table_name: str
) -> Generator[Callable[[bool], Cursor], Any, None]:
    """A factory fixture that creates cursors with a ten row, two column
    non-streaming select already executed. The caller can specify if the
    returned cursor should return dictionaries or tuples.
    """
    created_cursors: list[Cursor] = []

    def _create_cursor(as_dict: bool = False) -> Cursor:
        """A dict cursor with a ten row, two column non-streaming select already executed."""
        cursor = table_connection.cursor(as_dict=as_dict)

        # Selects from INFORMATION_SCHEMA.COLUMNS are very fast to execute (no pod creation), making
        # tests faster.
        # The table {test_table_name} has 10 visible columns, so this query will return 10 rows
        # x 2 columns.
        cursor.execute(
            f"""SELECT
                    `COLUMN_NAME` as `column`,
                    `DATA_TYPE` as `type`
                FROM `INFORMATION_SCHEMA`.`COLUMNS`
                WHERE TABLE_NAME = '{test_table_name}'
                    AND TABLE_SCHEMA = '{table_connection._dbname}'
                    AND IS_HIDDEN='NO'"""
        )

        created_cursors.append(cursor)
        return cursor

    yield _create_cursor

    # Cleanup all created cursors
    for cursor in created_cursors:
        cursor.close()


@pytest.fixture()
def expected_nonstreaming_results_factory() -> Callable[
    [bool], list[tuple[Any, ...] | dict[str, Any]]
]:
    """A factory fixture that returns the expected data fetched from the
    cursor_with_nonstreaming_data fixture, either as dicts or tuples.
    """

    def _get_expected_results(as_dict: bool) -> list[Any]:
        if as_dict:
            return [
                {"column": "c1", "type": "BIGINT"},
                {"column": "c2", "type": "VARCHAR"},
                {"column": "c3", "type": "VARCHAR"},
                {"column": "c4", "type": "VARCHAR"},
                {"column": "c5", "type": "VARCHAR"},
                {"column": "c6", "type": "VARCHAR"},
                {"column": "c7", "type": "VARCHAR"},
                {"column": "c8", "type": "VARCHAR"},
                {"column": "c9", "type": "VARCHAR"},
                {"column": "c10", "type": "VARCHAR"},
            ]
        else:
            return [
                ("c1", "BIGINT"),
                ("c2", "VARCHAR"),
                ("c3", "VARCHAR"),
                ("c4", "VARCHAR"),
                ("c5", "VARCHAR"),
                ("c6", "VARCHAR"),
                ("c7", "VARCHAR"),
                ("c8", "VARCHAR"),
                ("c9", "VARCHAR"),
                ("c10", "VARCHAR"),
            ]

    return _get_expected_results


@pytest.fixture(scope="session")
def test_row_table_name():
    """Returns a table name to use for the ROW-column table to populate for testing."""
    # Returns a fixed name for the test table, but if we have to adopt
    # pytest-xdist, we could include the worker id in the name to avoid collisions.
    return "pytest_row_table"


@pytest.fixture(scope="session")
def row_table_connection(connection: Connection, test_row_table_name: str):
    """
    This fixture takes the shared connection, and adds a table with some ROW columns to it.

    This is scoped for the whole session, so the table will be created
    only once when this fixture is used for the first time.
    Drops the table at the end, but also before creating it in case
    it wasn't dropped in a previous run for any reason.
    """
    cursor = connection.cursor()

    # First delete the table if it was left here for any reason
    cursor.execute(f"DROP TABLE IF EXISTS {test_row_table_name}")

    # Then create it from scratch. Some trivial to nontrivial ROW and ARRAY<ROW> columns.
    cursor.execute(
        f"""CREATE TABLE {test_row_table_name} (
            `name_and_age` ROW<name STRING, age INT>,
            `address_array` ARRAY<ROW<street STRING, city STRING, zip_code INT>>,
            `embedded_row` ROW<id INT, contact_info ROW<email STRING, phone STRING>>
        )"""
    )
    cursor.close()

    yield connection

    # Drop the table at the end of the session if all went well
    cursor = connection.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {test_row_table_name}")
    cursor.close()
