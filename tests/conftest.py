import os
from contextlib import contextmanager

import confluent_sql
import pytest

@pytest.fixture
def mock_connection_manager():
    """
    Create a connection for testing, with fake parameters.

    This will fail on any real attempt to connect, so it can only be used
    if the responses from the server have been mocked.

    Implemented as a context manager, usage:

    ```python
    def test_function(mock_connection_manager):
        with mock_connection_manager() as conn:
            with patch.object(connection._client, "request") as request_mock:
                mocked_response = Mock()
                # Implement what you need with the mock
                request_mock.return_value = mocked_response
                cursor = conn.cursor()
                # Rest of the code
    ```
    """
    @contextmanager
    def manager():
        conn = confluent_sql.connect(
        flink_api_key="TEST",
        flink_api_secret="TEST",
        environment="TEST",
        organization_id="TEST",
        compute_pool_id="TEST",
        cloud_region="TEST",
        cloud_provider="TEST",
        )
        try:
            yield conn
        finally:
            conn.close()
    return manager

@pytest.fixture
def connection_manager():
    """
    Create a connection for testing.

    This uses real api keys, and should only be used for integration tests.
    Implemented as a context manager, usage:

    ```python
    def test_function(connection_manager):
        with connection_manager() as conn:
            cursor = conn.cursor()
            # Rest of the code
    ```
    """
    flink_api_key = os.getenv("CONFLUENT_FLINK_API_KEY")
    flink_api_secret = os.getenv("CONFLUENT_FLINK_API_SECRET")
    environment = os.getenv("CONFLUENT_ENV_ID")
    organization_id = os.getenv("CONFLUENT_ORG_ID")
    compute_pool_id = os.getenv("CONFLUENT_COMPUTE_POOL_ID")
    cloud_provider = os.getenv("CONFLUENT_CLOUD_PROVIDER", "aws")
    cloud_region = os.getenv("CONFLUENT_CLOUD_REGION", "us-east-2")

    if not all(
        [
            flink_api_key,
            flink_api_secret,
            environment,
            organization_id,
            compute_pool_id,
            cloud_region,
            cloud_provider,
        ]
    ):
        pytest.skip("Missing required environment variables for integration test")

    @contextmanager
    def manager():
        conn = confluent_sql.connect(
        flink_api_key=flink_api_key,
        flink_api_secret=flink_api_secret,
        environment=environment,
        organization_id=organization_id,
        compute_pool_id=compute_pool_id,
        cloud_region=cloud_region,
        cloud_provider=cloud_provider,
        )
        try:
            yield conn
        finally:
            conn.close()
    return manager
