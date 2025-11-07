import logging
import os
import types
from typing import Any, Callable, Generator

import pytest

import confluent_sql
from confluent_sql.connection import Connection
from confluent_sql.cursor import Cursor
from confluent_sql.statement import Op

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def test_table_name():
    return "pytest_table"


@pytest.fixture()
def connection_factory() -> Generator[None, Callable[..., Connection], None]:
    """
    Returns a factory function to create new connections. Connection parameters
    default from environment variables.

    This is useful for tests that need to create multiple connections.
    Each call to the factory will create a new connection.

    The connections created by this factory will be automatically closed
    at the end of the test that used them (if not closed earlier).
    """

    connections: list[Connection] = []

    def _create_connection(
        *,
        flink_api_key=os.getenv("CONFLUENT_FLINK_API_KEY"),
        flink_api_secret=os.getenv("CONFLUENT_FLINK_API_SECRET"),
        environment=os.getenv("CONFLUENT_ENV_ID"),
        organization_id=os.getenv("CONFLUENT_ORG_ID"),
        compute_pool_id=os.getenv("CONFLUENT_COMPUTE_POOL_ID"),
        cloud_provider=os.getenv("CONFLUENT_CLOUD_PROVIDER"),
        cloud_region=os.getenv("CONFLUENT_CLOUD_REGION"),
        dbname=os.getenv("CONFLUENT_TEST_DBNAME"),
    ) -> Connection:
        connection = confluent_sql.connect(
            flink_api_key=flink_api_key,
            flink_api_secret=flink_api_secret,
            environment=environment,
            organization_id=organization_id,
            compute_pool_id=compute_pool_id,
            cloud_region=cloud_region,
            cloud_provider=cloud_provider,
            dbname=dbname,
        )

        connections.append(connection)
        return connection

    yield _create_connection

    # Cleanup all created connections
    for connection in connections:
        connection.close()


@pytest.fixture()
def single_test_connection(
    connection_factory: Callable[..., Connection],
) -> Connection:
    """
    Returns a single connection for tests that need only one connection.

    This connection is closed at the end of the test.
    """
    return connection_factory()


@pytest.fixture(scope="session")
def connection() -> Generator[Connection, Any, None]:
    """
    Create a connection for testing. Will automatically close at the end of the session.

    This uses real api keys, and should only be used for integration tests.
    This fixture is execute once per testing session, so the connection is
    shared between the various tests. So, avoid running tests in parallel.
    """
    flink_api_key = os.getenv("CONFLUENT_FLINK_API_KEY")
    flink_api_secret = os.getenv("CONFLUENT_FLINK_API_SECRET")
    environment = os.getenv("CONFLUENT_ENV_ID")
    organization_id = os.getenv("CONFLUENT_ORG_ID")
    compute_pool_id = os.getenv("CONFLUENT_COMPUTE_POOL_ID")
    cloud_provider = os.getenv("CONFLUENT_CLOUD_PROVIDER")
    cloud_region = os.getenv("CONFLUENT_CLOUD_REGION")
    dbname = os.getenv("CONFLUENT_TEST_DBNAME")

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


@pytest.fixture
def cursor(connection: Connection):
    """
    Returns a tuple-returning cursor for the shared connection. Deletes the statement and closes
    the cursor at the end of the test.

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
            (10, 'name10', 'name10', 'name10', 'name10', 'name10', 'name10', 'name10', 'name10', 'name10')
        """
        )

    yield table_connection


@pytest.fixture
def cursor_with_nonstreaming_data_factory(
    table_connection: Connection, test_table_name: str
) -> Generator[Callable[[bool], Cursor], Any, None]:
    """A factory fixture that creates cursors with a ten row, two column
    non-streaming select already executed. The caller can specify if the
    cursor should return dictionaries or tuples.
    """
    created_cursors: list[Cursor] = []

    def _create_cursor(as_dict: bool = False) -> Cursor:
        """A dict cursor with a ten row, two column non-streaming select already executed."""
        cursor = table_connection.cursor(as_dict=as_dict)

        # Selects from INFORMATION_SCHEMA.COLUMNS are very fast to execute (no pod creation), making tests faster.
        # The table {test_table_name} has 10 visible columns, so this query will return 10 rows x 2 columns.
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
        cursor.delete_statement()
        cursor.close()


@pytest.fixture()
def expected_nonstreaming_results_factory() -> Callable[
    [bool], list[tuple[Any, ...] | dict[str, Any]]
]:
    """A factory fixture that returns the expected data from the
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


@pytest.fixture
def mock_connection(mocker, statement_json_factory):
    """
    Create a connection for testing cursor behavior with mocked client responses.

    Any executed statement will appear to complete immediately with no results.

    Can override statement results by replacing mocked_conn._get_statement_results.return_value
    is with the resired result rows. Use fixture result_row_maker() to assist.
    """

    # Create a mock instance with spec so attribute/method names are validated
    mock_conn = mocker.create_autospec(Connection, instance=True)
    mock_conn._closed = False
    mock_conn.is_closed = False

    # Make any executed statement return a completed statement with no results
    executed_statement_from_json = statement_json_factory()
    mock_conn._execute_statement.return_value = executed_statement_from_json
    mock_conn._get_statement.return_value = executed_statement_from_json

    # Set up default statement results to empty list.
    # (Returns a tuple of (list of result rows, possible next_page URL))
    mock_conn._get_statement_results.return_value = ([], None)

    # Replace the MagicMock cursor with the real implementation bound to the mock instance
    mock_conn.cursor = types.MethodType(Connection.cursor, mock_conn)
    # Likewise for closing_cursor
    mock_conn.closing_cursor = types.MethodType(Connection.closing_cursor, mock_conn)

    yield mock_conn


@pytest.fixture
def result_row_maker() -> Callable[[list[Any], Op | None], dict[str, Any]]:
    """A fixture that returns a helper function to create result row dictionaries.
    These are helpful for overriding the return value of
    mock_connection._get_statement_results.
    """

    def _maker(row_values: list[Any], op: Op | None = None) -> dict[str, Any]:
        """Helper to create a result row dictionary for mocking statement results.

        Args:
            row_values: The list of values for the row.
            op: The changelog operation code (0=INSERT, 1=DELETE, etc). Default is 0 (INSERT).
        Returns:
            A dictionary representing the result row.
        """
        if op is None:
            # If no op is specified, code will assume it was an INSERT (op=0)
            return {"row": row_values}

        return {"row": row_values, "op": op.value}

    return _maker


@pytest.fixture()
def statement_json_factory() -> Callable[[str, bool], dict[str, Any]]:
    """A fixture that returns a factory function to create statement JSON dictionaries with various overrides."""

    def _factory(
        *,
        sql_statement: str = "SELECT TRUE AS VALUE",
        is_bounded: bool = True,
        phase: str = "COMPLETED",
        status_detail: str = "",
        compute_pool_id: str = "lfcp-01x6d2",
        principal: str = "u-0xw9v9p",
        schema_columns: list[dict[str, Any]] | None = [
            {
                "name": "value",
                "type": {
                    "nullable": False,
                    "type": "BOOLEAN",
                },
            },
        ],
    ) -> dict[str, Any]:
        """Construct a statement v1 as from JSON dictionary."""
        return {
            "api_version": "sql/v1",
            "environment_id": "env-asdf",
            "kind": "Statement",
            "metadata": {
                "created_at": "2025-11-07T18:45:05.102478Z",
                "labels": {},
                "resource_version": "5",
                "self": "https://flink.us-east-1.aws.confluent.cloud/dbapi-d4c685ef-befe-4091-9548-05d5ccb52d4a",
                "uid": "77826b1d-314e-4c6a-b33c-46e879930160",
                "updated_at": "2025-11-07T18:45:05.989814Z",
            },
            "name": "dbapi-d4c685ef-befe-4091-9548-05d5ccb52d4a",
            "organization_id": "7c210ed4-6e1e-4355-abf9-b25e25a8b25a",
            "spec": {
                "compute_pool_id": compute_pool_id,
                "execution_mode": "BATCH",
                "principal": principal,
                "properties": {
                    "sql.current-catalog": "env-d0v2k7",
                    "sql.current-database": "python_udf",
                    "sql.snapshot.mode": "now",
                },
                "statement": sql_statement,
                "stopped": False,
            },
            "status": {
                "detail": status_detail,
                "network_kind": "PUBLIC",
                "phase": phase,
                "scaling_status": {
                    "last_updated": "2025-11-07T18:45:05Z",
                    "scaling_state": "OK",
                },
                "traits": {
                    "connection_refs": [],
                    "is_append_only": True,
                    "is_bounded": is_bounded,
                    "schema": {"columns": schema_columns},
                    "sql_kind": "SELECT",
                    "upsert_columns": None,
                },
            },
        }

    return _factory
