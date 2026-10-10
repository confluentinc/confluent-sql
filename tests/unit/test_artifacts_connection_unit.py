"""Unit tests for the Connection-level artifact plumbing: scope resolution and delegation."""

from __future__ import annotations

from typing import Any
from unittest.mock import Mock

import pytest

from confluent_sql import InterfaceError, ProgrammingError
from confluent_sql.artifacts import ArtifactApi
from confluent_sql.connection import Connection, connect

pytestmark = pytest.mark.unit


def _connect(**overrides: Any) -> Connection:
    kwargs: dict[str, Any] = {
        "global_api_key": "gk",
        "global_api_secret": "gs",
        "environment_id": "env-1",
        "organization_id": "org-1",
        "cloud_provider": "aws",
        "cloud_region": "us-east-2",
        "database": "",
    }
    kwargs.update(overrides)
    return connect(**kwargs)


class TestArtifactScope:
    def test_cloud_is_uppercased_for_the_api(self) -> None:
        assert _connect().artifact_scope() == ("AWS", "us-east-2")

    def test_recovered_from_standard_flink_endpoint(self) -> None:
        conn = _connect(
            cloud_provider="",
            cloud_region="",
            endpoint="https://flink.eu-west-1.gcp.confluent.cloud",
        )
        assert conn.artifact_scope() == ("GCP", "eu-west-1")

    def test_unrecoverable_from_custom_endpoint(self) -> None:
        conn = _connect(
            cloud_provider="", cloud_region="", endpoint="https://flink.internal.example.com"
        )
        with pytest.raises(ProgrammingError, match="cloud_provider and cloud_region"):
            conn.artifact_scope()


class TestControlplaneAccess:
    def test_requires_a_controlplane_credential(self) -> None:
        conn = _connect(
            global_api_key="", global_api_secret="", flink_api_key="fk", flink_api_secret="fs"
        )
        with pytest.raises(ProgrammingError, match="global API key"):
            conn.list_artifacts()

    def test_closed_connection_raises(self) -> None:
        conn = _connect()
        conn.close()
        with pytest.raises(InterfaceError, match="Connection is closed"):
            conn.list_artifacts()


class TestDelegation:
    @pytest.mark.parametrize(
        ("method", "api_method", "args", "kwargs"),
        [
            ("get_artifact_upload_url", "get_upload_url", ("ZIP",), {}),
            ("get_artifact", "get", ("cfa-1",), {}),
            ("update_artifact", "update", ("cfa-1",), {"description": "d"}),
        ],
    )
    def test_passes_through_to_artifact_api(
        self, mocker, method: str, api_method: str, args: tuple, kwargs: dict
    ) -> None:
        conn = _connect()
        spy = mocker.patch.object(ArtifactApi, api_method, return_value="result")
        assert getattr(conn, method)(*args, **kwargs) == "result"
        spy.assert_called_once()

    def test_artifact_api_is_composed_once(self) -> None:
        conn = _connect()
        assert conn._get_artifact_api() is conn._get_artifact_api()

    def test_delete_defaults_to_waiting_for_removal(self, mocker) -> None:
        conn = _connect()
        spy = mocker.patch.object(ArtifactApi, "delete")
        conn.delete_artifact("cfa-1")
        spy.assert_called_once_with("cfa-1", wait_for_removal=True, timeout=300)

    def test_requests_use_the_shared_controlplane_client(self) -> None:
        conn = _connect()
        client = Mock()
        client.request.return_value = Mock(status_code=200, raise_for_status=Mock())
        conn._get_controlplane_client = Mock(return_value=client)  # type: ignore[method-assign]
        conn.artifact_controlplane_request("/artifact/v1/flink-artifacts", params={"a": "b"})
        client.request.assert_called_once_with(
            "GET", "/artifact/v1/flink-artifacts", params={"a": "b"}
        )
