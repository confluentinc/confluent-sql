"""Unit tests for the Connection-level connector plumbing: credentials, client, and passthroughs."""

from __future__ import annotations

from typing import Any
from unittest.mock import Mock

import httpx
import pytest

from confluent_sql import InterfaceError, ProgrammingError
from confluent_sql.connection import Connection, ControlPlaneSurface, connect

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


class TestConnectControlplaneClient:
    """The Connect surface's credential: its own dedicated key pair, or the global key."""

    def test_no_connect_credentials_raises(self) -> None:
        conn = _connect()  # flink-only creds
        with pytest.raises(ProgrammingError, match="connect_api_key"):
            conn._auth_for(ControlPlaneSurface.CONNECT)

    def test_connect_pair_authenticates(self, mocker) -> None:
        spy = mocker.patch("confluent_sql.connection.httpx.BasicAuth", wraps=httpx.BasicAuth)
        conn = _connect(connect_api_key="ck", connect_api_secret="cs")
        assert conn._auth_for(ControlPlaneSurface.CONNECT) is not None
        spy.assert_called_with(username="ck", password="cs")

    def test_connect_credential_differs_from_tableflow_with_different_creds(self) -> None:
        conn = _connect(
            tableflow_api_key="tk",
            tableflow_api_secret="ts",
            connect_api_key="ck",
            connect_api_secret="cs",
        )
        assert conn._auth_for(ControlPlaneSurface.CONNECT) is not conn._auth_for(
            ControlPlaneSurface.TABLEFLOW
        )

    def test_closed_connection_raises(self) -> None:
        conn = _connect(connect_api_key="ck", connect_api_secret="cs")
        conn.close()
        with pytest.raises(InterfaceError, match="Connection is closed"):
            conn._auth_for(ControlPlaneSurface.CONNECT)


class TestConnectorApiComposition:
    """What `Connection` hands the `ConnectorApi` it composes."""

    def test_api_receives_the_connections_environment_and_cluster_resolver(self) -> None:
        conn = _connect(database_kafka_cluster_id="lkc-9")
        api = conn.get_connector_api()
        assert api._connectors_path() == "/connect/v1/environments/env-1/clusters/lkc-9/connectors"

    def test_api_requests_carry_the_connect_credential(self) -> None:
        conn = _connect(connect_api_key="ck", connect_api_secret="cs")
        client = Mock()
        client.request = Mock(return_value=Mock(content=b"{}"))
        conn._controlplane._client = client
        conn.get_connector_api()._request(
            "/x", method="POST", json={"a": 1}, raise_for_status=False
        )
        client.request.assert_called_once_with(
            "POST", "/x", json={"a": 1}, auth=conn._auth_for(ControlPlaneSurface.CONNECT)
        )


class TestGetConnectorApi:
    def test_api_is_composed_lazily(self) -> None:
        assert _connect(connect_api_key="ck", connect_api_secret="cs")._connector_api is None

    def test_api_is_composed_once_and_cached(self) -> None:
        conn = _connect(connect_api_key="ck", connect_api_secret="cs")
        assert conn.get_connector_api() is conn.get_connector_api()
