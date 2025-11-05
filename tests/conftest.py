import os

import confluent_sql
import pytest


@pytest.fixture(scope="session")
def test_table_name():
    return "pytest_table"


@pytest.fixture(scope="session")
def connection():
    """
    Create a connection for testing.

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
def cursor(connection):
    """
    Returns a cursor for the shared connection.

    This cursor is unique for each test, even if they all share the same connection.
    """
    cursor = connection.cursor(with_schema=True)
    yield cursor
    cursor.close()


@pytest.fixture(scope="session")
def table_connection(connection, test_table_name):
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
        f"CREATE TABLE {test_table_name} (`c1` BIGINT, `c2` STRING, `c3` STRING, `c4` STRING, `c5` STRING, `c6` STRING, `c7` STRING, `c8` STRING, `c9` STRING, `c10` STRING)"
    )
    cursor.close()

    yield connection

    cursor = connection.cursor()
    # Delete it at the end if everything went fine
    cursor.execute(f"DROP TABLE IF EXISTS {test_table_name}")
    cursor.close()


@pytest.fixture(scope="session")
def populated_table_connection(table_connection, test_table_name):
    """This fixture adds some data to the table before serving the shared connection."""
    cursor = table_connection.cursor()
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
    cursor.close()
    yield table_connection


@pytest.fixture
def cursor_with_nonstreaming_data(table_connection, test_table_name):
    """A cursor with a 10-row non-streaming select already executed."""
    cursor = table_connection.cursor(with_schema=True)

    # Selects from INFORMATION_SCHEMA.COLUMNS are very fast to execute (no pod creation), making tests faster.
    # The table {test_table_name} has 10 visible columns, so this query will return 10 rows.
    cursor.execute(
        f"""SELECT * FROM `INFORMATION_SCHEMA`.`COLUMNS`
                WHERE TABLE_NAME = '{test_table_name}'
                AND TABLE_SCHEMA = '{table_connection._dbname}'
                AND IS_HIDDEN='NO'"""
    )

    yield cursor

    cursor.close()


@pytest.fixture
def mock_connection():
    """
    Create a connection for testing, with fake parameters.

    This will fail on any real attempt to connect, so it can only be used
    if the responses from the server have been mocked.
    TODO: I don't really like this, we should probably be injecting a test
          client inside the Connection object rather than doing this...

    ```python
    def test_function(mock_connection):
        with patch.object(mock_connection._client, "request") as request_mock:
            mocked_response = Mock()
            # Implement what you need with the mock
            request_mock.return_value = mocked_response
            cursor = mock_connection.cursor()
            # Rest of the code
    ```
    """

    conn = confluent_sql.connect(
        flink_api_key="TEST",
        flink_api_secret="TEST",
        environment="TEST",
        organization_id="TEST",
        compute_pool_id="TEST",
        cloud_region="TEST",
        cloud_provider="TEST",
        dbname="TEST",
    )
    yield conn
    conn.close()
