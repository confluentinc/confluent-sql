"""Fixtures and setup for all tests."""

import os
from collections.abc import Callable, Generator
from pathlib import Path
from typing import TypeAlias

import pytest

from confluent_sql import DEFAULT_TIMEOUTS, Connection, Timeouts, connect

_PATH_MARKERS = ("unit", "integration")


def _marker_for_path(rel_path: Path) -> str | None:
    """The `unit` / `integration` marker implied by the first such directory in `rel_path`."""
    return next((part for part in rel_path.parts[:-1] if part in _PATH_MARKERS), None)


def _placement_problem(collector: pytest.Collector, children: list) -> str | None:
    """What is wrong with how `collector` (or the tests it just collected) is marked or placed, or
    None if nothing: a test module outside both `unit/` and `integration/`, or any module, class
    or test function carrying either marker explicitly (even the one its directory implies)."""
    if isinstance(collector, pytest.Module):
        rel_path = collector.path.relative_to(collector.config.rootpath)
        if _marker_for_path(rel_path) is None:
            return f"{rel_path} is neither under unit/ nor integration/; every test lives in one."
    for node in (collector, *(child for child in children if isinstance(child, pytest.Item))):
        for explicit in _PATH_MARKERS:
            if any(mark.name == explicit for mark in node.own_markers):
                return (
                    f"{node.nodeid} is explicitly marked `{explicit}`; remove it. The "
                    "unit/integration marker is applied automatically from the directory, "
                    "even when the explicit mark agrees with it."
                )
    return None


@pytest.hookimpl(wrapper=True)
def pytest_make_collect_report(collector):
    """Turn a misplaced or hand-marked test into an ordinary collection error (#283).

    Nobody hand-applies the `unit` / `integration` markers. This is reported as a failed
    collection report rather than raised from `pytest_collection_modifyitems`, because under
    xdist (on by default here) an exception in a worker's `modifyitems` kills the worker: the
    controller never sees the message and trips over its own "crashed worker" assertion, blaming
    whichever unrelated test the worker happened to be on. A failed collection report is
    forwarded to the controller and shown as `ERROR collecting <file>` with this text.
    """
    report = yield
    if report.passed and (problem := _placement_problem(collector, report.result)):
        report.outcome = "failed"
        report.longrepr = problem
    return report


@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(config, items):
    """Mark every test `unit` or `integration` from the directory it lives in (#283).

    Misplaced or hand-marked tests never get here: `pytest_make_collect_report` fails their
    collection first.

    `tryfirst` matters: pytest applies `-m` selection in its own collection hook, so the markers
    must already be there for `-m unit` / `-m integration` to select anything.
    """
    for item in items:
        item.add_marker(_marker_for_path(item.path.relative_to(config.rootpath)))


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
        global_api_key: str | None = None,
        global_api_secret: str | None = None,
        flink_api_key: str | None = None,
        flink_api_secret: str | None = None,
        tableflow_api_key: str | None = None,
        tableflow_api_secret: str | None = None,
        environment_id: str | None = None,
        organization_id: str | None = None,
        compute_pool_id: str | None = None,
        cloud_provider: str | None = None,
        cloud_region: str | None = None,
        database: str | None = None,
        database_kafka_cluster_id: str | None = None,
        controlplane_endpoint: str | None = None,
        result_page_fetch_pause_millis: int = 100,
        http_user_agent: str | None = None,
        timeouts: Timeouts = DEFAULT_TIMEOUTS,
        endpoint: str | None = None,
    ) -> Connection:
        if global_api_key is None:
            global_api_key = os.getenv("CONFLUENT_GLOBAL_API_KEY", "")
        if global_api_secret is None:
            global_api_secret = os.getenv("CONFLUENT_GLOBAL_API_SECRET", "")
        if flink_api_key is None:
            flink_api_key = os.getenv("CONFLUENT_FLINK_API_KEY", "")
        if flink_api_secret is None:
            flink_api_secret = os.getenv("CONFLUENT_FLINK_API_SECRET", "")
        if environment_id is None:
            environment_id = os.getenv("CONFLUENT_ENV_ID", "")
        if organization_id is None:
            organization_id = os.getenv("CONFLUENT_ORG_ID", "")
        if compute_pool_id is None:
            compute_pool_id = os.getenv("CONFLUENT_COMPUTE_POOL_ID", "")
        if endpoint is None:
            # Only fill in cloud_provider and cloud_region from env vars if endpoint is not
            # provided, otherwise connect will raise an error.
            if cloud_provider is None:
                cloud_provider = os.getenv("CONFLUENT_CLOUD_PROVIDER", "")
            if cloud_region is None:
                cloud_region = os.getenv("CONFLUENT_CLOUD_REGION", "")
        if database is None:
            database = os.getenv("CONFLUENT_TEST_DBNAME", "")

        connection = connect(
            global_api_key=global_api_key,
            global_api_secret=global_api_secret,
            flink_api_key=flink_api_key,
            flink_api_secret=flink_api_secret,
            tableflow_api_key=tableflow_api_key,
            tableflow_api_secret=tableflow_api_secret,
            environment_id=environment_id,
            organization_id=organization_id,
            compute_pool_id=compute_pool_id,
            cloud_region=cloud_region,
            cloud_provider=cloud_provider,
            database=database,
            database_kafka_cluster_id=database_kafka_cluster_id,
            controlplane_endpoint=controlplane_endpoint,
            result_page_fetch_pause_millis=result_page_fetch_pause_millis,
            http_user_agent=http_user_agent,
            timeouts=timeouts,
            endpoint=endpoint,
        )

        connections.append(connection)
        return connection

    yield _create_connection

    # Cleanup all created connections
    for connection in connections:
        connection.close()
