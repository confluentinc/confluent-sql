"""Unit tests for Connection.close() breaking the reference cycles with its service APIs."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import pytest

from confluent_sql import InterfaceError
from confluent_sql.connection import Connection, connect
from confluent_sql.service_api import ServiceApi

pytestmark = pytest.mark.unit


def _connect() -> Connection:
    return connect(
        global_api_key="gk",
        global_api_secret="gs",
        environment_id="env-1",
        organization_id="org-1",
        cloud_provider="aws",
        cloud_region="us-east-2",
        database="",
    )


@dataclass(frozen=True)
class ServiceApiSpec:
    """How one service API hangs off a Connection."""

    name: str
    get_from: Callable[[Connection], Any]
    """Obtains (composing and caching on first use) the service API from a connection."""
    cache_attr: str
    """The Connection attribute that caches the composed service API."""
    call_needing_connection: Callable[[Any], Any]
    """A service API call that needs the host connection (fails fast, before any request)."""


@dataclass(frozen=True)
class ComposedServiceApi:
    """A service API composed on a fresh connection, with the details to inspect it."""

    conn: Connection
    api: Any
    cache_attr: str
    call_needing_connection: Callable[[Any], Any]


SERVICE_API_SPECS = [
    ServiceApiSpec(
        name="artifacts",
        get_from=lambda conn: conn.get_artifacts_api(),
        cache_attr="_artifacts_api",
        call_needing_connection=lambda api: api.get("art-1"),
    ),
    ServiceApiSpec(
        name="connectors",
        get_from=lambda conn: conn._get_connector_api(),
        cache_attr="_connector_api",
        call_needing_connection=lambda api: api.get("my-connector"),
    ),
]


@pytest.fixture(params=SERVICE_API_SPECS, ids=lambda spec: spec.name)
def service_case(request: pytest.FixtureRequest) -> ComposedServiceApi:
    spec: ServiceApiSpec = request.param
    conn = _connect()
    return ComposedServiceApi(
        conn=conn,
        api=spec.get_from(conn),
        cache_attr=spec.cache_attr,
        call_needing_connection=spec.call_needing_connection,
    )


class TestServiceApi:
    def test_get_context_returns_the_context(self) -> None:
        context = object()
        assert ServiceApi(context).get_context() is context

    def test_close_drops_the_context_and_get_context_raises(self) -> None:
        service_api = ServiceApi(object())
        service_api._close()
        assert service_api._context is None
        with pytest.raises(InterfaceError, match="Connection is closed"):
            service_api.get_context()

    def test_close_is_idempotent(self) -> None:
        service_api = ServiceApi(object())
        service_api._close()
        service_api._close()
        assert service_api._context is None


class TestConnectionCloseBreaksCycles:
    def test_close_clears_service_api_back_reference_and_cache(
        self, service_case: ComposedServiceApi
    ) -> None:
        conn, api = service_case.conn, service_case.api
        assert api._context is conn
        assert getattr(conn, service_case.cache_attr) is api
        conn.close()
        assert api._context is None
        assert getattr(conn, service_case.cache_attr) is None

    def test_held_service_api_raises_interface_error_after_close(
        self, service_case: ComposedServiceApi
    ) -> None:
        service_case.conn.close()
        with pytest.raises(InterfaceError, match="Connection is closed"):
            service_case.call_needing_connection(service_case.api)

    def test_close_without_composed_service_apis(self) -> None:
        conn = _connect()
        conn.close()
        assert conn._artifacts_api is None
        assert conn._connector_api is None

    def test_second_close_is_a_noop(self, service_case: ComposedServiceApi) -> None:
        service_case.conn.close()
        service_case.conn.close()
        assert getattr(service_case.conn, service_case.cache_attr) is None

    @pytest.mark.parametrize("spec", SERVICE_API_SPECS, ids=lambda spec: spec.name)
    def test_service_api_is_unreachable_through_connection_after_close(
        self, spec: ServiceApiSpec
    ) -> None:
        conn = _connect()
        conn.close()
        with pytest.raises(InterfaceError, match="Connection is closed"):
            spec.get_from(conn)
        assert getattr(conn, spec.cache_attr) is None

    def test_connector_call_after_close_does_not_recompose_the_api(self) -> None:
        conn = _connect()
        conn.close()
        with pytest.raises(InterfaceError, match="Connection is closed"):
            conn.get_connector("my-connector")
        assert conn._connector_api is None
