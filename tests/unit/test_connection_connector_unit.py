"""Unit tests for the Connection-level connector plumbing: credentials, client, and passthroughs."""

from __future__ import annotations

from typing import Any
from unittest.mock import Mock

import httpx
import pytest

from confluent_sql import InterfaceError, OperationalError, ProgrammingError
from confluent_sql.connection import Connection, _resolve_connect_credentials, connect

pytestmark = pytest.mark.unit


def _connect(**overrides: Any) -> Connection:
    """Build a Connection with an explicit endpoint and deterministic (non-env) credentials."""
    kwargs: dict[str, Any] = {
        "flink_api_key": "flink-k",
        "flink_api_secret": "flink-s",
        "global_api_key": "",
        "global_api_secret": "",
        "connect_api_key": "",
        "connect_api_secret": "",
        "environment_id": "env-1",
        "organization_id": "org-1",
        "endpoint": "https://flink.example.com",
        "database": "",
    }
    kwargs.update(overrides)
    return connect(**kwargs)


class TestResolveConnectCredentials:
    """The Connect control-plane credential pick: global wins, else the connect pair, else None."""

    def test_global_wins_over_connect(self) -> None:
        assert _resolve_connect_credentials("gk", "gs", "ck", "cs") == ("gk", "gs")

    def test_connect_pair_when_no_global(self) -> None:
        assert _resolve_connect_credentials("", "", "ck", "cs") == ("ck", "cs")

    def test_none_when_neither(self) -> None:
        assert _resolve_connect_credentials("", "", "", "") is None

    def test_half_connect_pair_raises(self) -> None:
        with pytest.raises(InterfaceError, match="connect_api_key and connect_api_secret"):
            _resolve_connect_credentials("", "", "ck", "")

    def test_global_short_circuits_half_connect_pair(self) -> None:
        assert _resolve_connect_credentials("gk", "gs", "ck", "") == ("gk", "gs")


class TestConnectControlplaneClient:
    """The connect-authed control-plane client is distinct from the Tableflow one."""

    def test_no_connect_credentials_raises(self) -> None:
        conn = _connect()  # flink-only creds
        with pytest.raises(ProgrammingError, match="connect_api_key"):
            conn._get_connect_controlplane_client()

    def test_connect_pair_authenticates(self, mocker) -> None:
        spy = mocker.patch("confluent_sql.connection.httpx.BasicAuth", wraps=httpx.BasicAuth)
        conn = _connect(connect_api_key="ck", connect_api_secret="cs")
        client = conn._get_connect_controlplane_client()
        assert str(client.base_url) == "https://api.confluent.cloud"
        spy.assert_called_with(username="ck", password="cs")

    def test_distinct_from_tableflow_client_with_different_creds(self) -> None:
        conn = _connect(
            tableflow_api_key="tk",
            tableflow_api_secret="ts",
            connect_api_key="ck",
            connect_api_secret="cs",
        )
        assert conn._get_connect_controlplane_client() is not conn._get_controlplane_client()

    def test_connect_client_cached(self) -> None:
        conn = _connect(connect_api_key="ck", connect_api_secret="cs")
        assert conn._get_connect_controlplane_client() is conn._get_connect_controlplane_client()

    def test_close_closes_created_connect_client(self) -> None:
        conn = _connect(connect_api_key="ck", connect_api_secret="cs")
        client = conn._get_connect_controlplane_client()
        conn.close()
        assert client.is_closed

    def test_closed_connection_raises(self) -> None:
        conn = _connect(connect_api_key="ck", connect_api_secret="cs")
        conn.close()
        with pytest.raises(InterfaceError, match="Connection is closed"):
            conn._get_connect_controlplane_client()


class TestControlPlaneContextSatisfaction:
    """Connection satisfies the ControlPlaneContext protocol ConnectorApi depends on."""

    def test_resolve_kafka_cluster_id_delegates(self) -> None:
        conn = _connect(database_kafka_cluster_id="lkc-9")
        assert conn.resolve_kafka_cluster_id() == "lkc-9"

    def test_controlplane_request_routes_through_connect_client(self) -> None:
        conn = _connect(connect_api_key="ck", connect_api_secret="cs")
        client = Mock()
        client.request = Mock(return_value=Mock(content=b"{}"))
        conn._get_connect_controlplane_client = Mock(return_value=client)  # type: ignore[method-assign]
        conn.controlplane_request("/x", method="POST", json={"a": 1}, raise_for_status=False)
        client.request.assert_called_once_with("POST", "/x", json={"a": 1})

    def test_controlplane_request_http_error_becomes_operational(self) -> None:
        conn = _connect(connect_api_key="ck", connect_api_secret="cs")
        response = Mock()
        inner = Mock()
        inner.status_code = 500
        response.raise_for_status = Mock(
            side_effect=httpx.HTTPStatusError("boom", request=Mock(), response=inner)
        )
        client = Mock()
        client.request = Mock(return_value=response)
        conn._get_connect_controlplane_client = Mock(return_value=client)  # type: ignore[method-assign]
        with pytest.raises(OperationalError):
            conn.controlplane_request("/x")


class TestConnectorPassthroughs:
    """The three Connection methods are one-line delegations to a lazily-composed ConnectorApi."""

    def test_create_connector_delegates(self) -> None:
        conn = _connect(connect_api_key="ck", connect_api_secret="cs")
        api = Mock()
        conn._connector_api = api
        config = {"connector.class": "DatagenSource"}

        result = conn.create_connector("c1", config=config, wait_for_running=False, timeout=10)

        api.create.assert_called_once_with(
            "c1", config=config, wait_for_running=False, timeout=10
        )
        assert result is api.create.return_value

    def test_get_connector_delegates(self) -> None:
        conn = _connect(connect_api_key="ck", connect_api_secret="cs")
        api = Mock()
        conn._connector_api = api

        result = conn.get_connector("c1")

        api.get.assert_called_once_with("c1")
        assert result is api.get.return_value

    def test_delete_connector_delegates(self) -> None:
        conn = _connect(connect_api_key="ck", connect_api_secret="cs")
        api = Mock()
        conn._connector_api = api

        conn.delete_connector("c1", wait_for_removal=False, timeout=10)

        api.delete.assert_called_once_with("c1", wait_for_removal=False, timeout=10)

    def test_connector_api_lazily_composed_with_self_as_context(self) -> None:
        conn = _connect(connect_api_key="ck", connect_api_secret="cs")
        first = conn._get_connector_api()
        assert first is conn._get_connector_api()
        assert first._context is conn
