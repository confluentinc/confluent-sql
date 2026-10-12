"""Unit tests for the Connection-level artifact plumbing: scope resolution and delegation."""

from __future__ import annotations

from typing import Any
from unittest.mock import Mock

import httpx
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


def _basic_authorization(key: str, secret: str) -> str:
    request = httpx.Request("GET", "https://example.invalid")
    return next(httpx.BasicAuth(key, secret).auth_flow(request)).headers["Authorization"]


def _artifact_authorization(conn: Connection) -> str:
    """The Authorization header the artifact client would send."""
    auth = conn._get_cloud_controlplane_client().auth
    assert auth is not None
    request = httpx.Request("GET", "https://example.invalid")
    return next(auth.auth_flow(request)).headers["Authorization"]


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

    def test_explicit_endpoint_wins_over_cloud_arguments(self) -> None:
        conn = _connect(endpoint="https://flink.eu-west-1.gcp.confluent.cloud")
        assert conn.artifact_scope() == ("GCP", "eu-west-1")

    def test_endpoint_host_is_matched_case_insensitively(self) -> None:
        conn = _connect(
            cloud_provider="",
            cloud_region="",
            endpoint="https://FLINK.EU-West-1.GCP.confluent.cloud",
        )
        assert conn.artifact_scope() == ("GCP", "eu-west-1")

    @pytest.mark.parametrize(
        "endpoint",
        [
            "https://flink.us/east.aws.confluent.cloud",
            "https://flink.us-east-2.aws.confluent.cloud?x=y",
            "https://flink.us-east-2.aws.confluent.cloud/path",
            "https://flink.us_east.aws.confluent.cloud",
        ],
    )
    def test_endpoint_with_non_hostname_characters_is_not_parsed(self, endpoint: str) -> None:
        conn = _connect(cloud_provider="", cloud_region="", endpoint=endpoint)
        with pytest.raises(ProgrammingError, match="cloud_provider and cloud_region"):
            conn.artifact_scope()

    def test_endpoint_with_extra_host_suffix_is_not_parsed(self) -> None:
        conn = _connect(
            cloud_provider="",
            cloud_region="",
            endpoint="https://flink.us-east-2.aws.confluent.cloud.example.com",
        )
        with pytest.raises(ProgrammingError, match="cloud_provider and cloud_region"):
            conn.artifact_scope()


class TestControlplaneAccess:
    def test_requires_a_controlplane_credential(self) -> None:
        conn = _connect(
            global_api_key="", global_api_secret="", flink_api_key="fk", flink_api_secret="fs"
        )
        with pytest.raises(ProgrammingError, match="Artifacts require a global API key"):
            conn.list_artifacts()

    def test_tableflow_only_key_is_refused_locally(self) -> None:
        conn = _connect(
            global_api_key="",
            global_api_secret="",
            flink_api_key="fk",
            flink_api_secret="fs",
            tableflow_api_key="tk",
            tableflow_api_secret="ts",
        )
        with pytest.raises(ProgrammingError, match="cloud_api_key"):
            conn.list_artifacts()

    def test_cloud_key_alone_authenticates_artifact_requests(self) -> None:
        conn = _connect(
            global_api_key="",
            global_api_secret="",
            flink_api_key="fk",
            flink_api_secret="fs",
            cloud_api_key="ck",
            cloud_api_secret="cs",
        )
        assert _artifact_authorization(conn) == _basic_authorization("ck", "cs")

    def test_global_key_alone_authenticates_artifact_requests(self) -> None:
        assert _artifact_authorization(_connect()) == _basic_authorization("gk", "gs")

    def test_global_key_wins_over_cloud_key(self) -> None:
        conn = _connect(cloud_api_key="ck", cloud_api_secret="cs")
        assert conn._cloud_auth == ("gk", "gs")

    def test_half_supplied_cloud_key_is_rejected(self) -> None:
        with pytest.raises(InterfaceError, match="cloud_api_key and cloud_api_secret"):
            _connect(
                global_api_key="",
                global_api_secret="",
                flink_api_key="fk",
                flink_api_secret="fs",
                cloud_api_key="ck",
            )

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

    def test_requests_use_the_artifact_client(self) -> None:
        conn = _connect()
        client = Mock()
        client.request.return_value = Mock(status_code=200, raise_for_status=Mock())
        conn._get_cloud_controlplane_client = Mock(return_value=client)  # type: ignore[method-assign]
        conn.artifact_controlplane_request("/artifact/v1/flink-artifacts", params={"a": "b"})
        client.request.assert_called_once_with(
            "GET", "/artifact/v1/flink-artifacts", params={"a": "b"}
        )


class TestArtifactRequestRetries:
    @staticmethod
    def _conn_with_client(*responses: Any) -> tuple[Connection, Mock]:
        conn = _connect()
        client = Mock()
        client.request.side_effect = list(responses)
        conn._get_cloud_controlplane_client = Mock(return_value=client)  # type: ignore[method-assign]
        return conn, client

    @staticmethod
    def _response(status: int) -> Mock:
        return Mock(status_code=status, content=b"")

    def test_get_retries_transient_status(self, mocker) -> None:
        mocker.patch("confluent_sql.retry.time.sleep")
        conn, client = self._conn_with_client(self._response(503), self._response(200))
        response = conn.artifact_controlplane_request("/x", raise_for_status=False)
        assert response.status_code == 200
        assert client.request.call_count == 2

    def test_get_retries_transport_error(self, mocker) -> None:
        mocker.patch("confluent_sql.retry.time.sleep")
        conn, client = self._conn_with_client(httpx.NetworkError("reset"), self._response(200))
        assert conn.artifact_controlplane_request("/x").status_code == 200
        assert client.request.call_count == 2

    def test_non_retryable_status_is_returned_for_caller_mapping(self) -> None:
        conn, client = self._conn_with_client(self._response(404))
        response = conn.artifact_controlplane_request("/x", raise_for_status=False)
        assert response.status_code == 404
        assert client.request.call_count == 1

    @pytest.mark.parametrize("method", ["POST", "PATCH", "DELETE"])
    def test_mutating_requests_are_not_retried(self, method: str) -> None:
        conn, client = self._conn_with_client(self._response(503))
        response = conn.artifact_controlplane_request("/x", method=method, raise_for_status=False)
        assert response.status_code == 503
        assert client.request.call_count == 1


class TestArtifactResponseLogging:
    @pytest.mark.parametrize("method", ["GET", "POST"])
    def test_presigned_secrets_are_not_debug_logged(self, method: str, caplog) -> None:
        secret = "X-Amz-Signature=deadbeef"
        conn = _connect()
        client = Mock()
        client.request.return_value = Mock(status_code=200, content=secret.encode())
        conn._get_cloud_controlplane_client = Mock(return_value=client)  # type: ignore[method-assign]
        with caplog.at_level("DEBUG", logger="confluent_sql"):
            conn.artifact_controlplane_request("/artifact/v1/x", method=method)
        assert secret not in caplog.text
