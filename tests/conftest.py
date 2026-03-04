"""Fixtures and setup for all tests."""

import os
from collections.abc import Callable, Generator
from typing import TypeAlias

import pytest

from confluent_sql import Connection, connect


def pytest_runtest_setup(item):
    """Ensure that all tests are marked as either unit or integration, but never both."""
    is_unit = any(item.iter_markers(name="unit"))
    is_integration = any(item.iter_markers(name="integration"))
    if not (is_integration or is_unit):
        pytest.fail(
            "Tests must be marked with either @pytest.mark.unit or @pytest.mark.integration."
        )

    if is_unit and is_integration:
        pytest.fail(
            "Tests cannot be marked with both @pytest.mark.unit and @pytest.mark.integration."
        )


# Fixtures common to all tests ...


ConnectionFactory: TypeAlias = Callable[..., Connection]


@pytest.fixture()
def connection_factory() -> Generator[ConnectionFactory, None, None]:
    """
    Returns a factory function to create new connections. Connection parameters
    default from environment variables, and will just be blank if env
    vars are not set (and the resulting connection could be used for
    making queries, but could have mocks applied to it).

    This is useful for tests that need to create multiple connections.
    Each call to the factory will create a new connection.

    The connections created by this factory will be automatically closed
    at the end of the test that used them (if not closed earlier).
    """

    connections: list[Connection] = []

    def _create_connection(  # noqa: PLR0913
        *,
        flink_api_key: str | None = None,
        flink_api_secret: str | None = None,
        environment: str | None = None,
        organization_id: str | None = None,
        compute_pool_id: str | None = None,
        cloud_provider: str | None = None,
        cloud_region: str | None = None,
        dbname: str | None = None,
        result_page_fetch_pause_millis: int = 100,
        http_user_agent: str | None = None,
    ) -> Connection:
        if flink_api_key is None:
            flink_api_key = os.getenv("CONFLUENT_FLINK_API_KEY", "")
        if flink_api_secret is None:
            flink_api_secret = os.getenv("CONFLUENT_FLINK_API_SECRET", "")
        if environment is None:
            environment = os.getenv("CONFLUENT_ENV_ID", "")
        if organization_id is None:
            organization_id = os.getenv("CONFLUENT_ORG_ID", "")
        if compute_pool_id is None:
            compute_pool_id = os.getenv("CONFLUENT_COMPUTE_POOL_ID", "")
        if cloud_provider is None:
            cloud_provider = os.getenv("CONFLUENT_CLOUD_PROVIDER", "")
        if cloud_region is None:
            cloud_region = os.getenv("CONFLUENT_CLOUD_REGION", "")
        if dbname is None:
            dbname = os.getenv("CONFLUENT_TEST_DBNAME", "")

        connection = connect(
            flink_api_key=flink_api_key,
            flink_api_secret=flink_api_secret,
            environment=environment,
            organization_id=organization_id,
            compute_pool_id=compute_pool_id,
            cloud_region=cloud_region,
            cloud_provider=cloud_provider,
            dbname=dbname,
            result_page_fetch_pause_millis=result_page_fetch_pause_millis,
            http_user_agent=http_user_agent,
        )

        connections.append(connection)
        return connection

    yield _create_connection

    # Cleanup all created connections
    for connection in connections:
        connection.close()
