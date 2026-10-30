"""Unit tests for the Connection-level artifact plumbing: scope resolution, credentials, retries."""

from __future__ import annotations

from typing import Any
from unittest.mock import Mock

import httpx
import pytest

from confluent_sql import InterfaceError, ProgrammingError
from confluent_sql.connection import Connection, ControlPlaneSurface, connect

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
    """The Authorization header artifact requests would carry."""
    auth = conn._auth_for(ControlPlaneSurface.ARTIFACTS)
    request = httpx.Request("GET", "https://example.invalid")
    return next(auth.auth_flow(request)).headers["Authorization"]


class TestControlplaneAccess:
    def test_requires_a_controlplane_credential(self) -> None:
        conn = _connect(
            global_api_key="", global_api_secret="", flink_api_key="fk", flink_api_secret="fs"
        )
        with pytest.raises(ProgrammingError, match="Artifacts require a global API key"):
            conn.get_artifacts_api().list()

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
            conn.get_artifacts_api().list()

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
        assert _artifact_authorization(conn) == _basic_authorization("gk", "gs")

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
            conn.get_artifacts_api().list()


class TestGetArtifactsApi:
    def test_scope_is_recovered_from_the_connections_endpoint(self) -> None:
        conn = _connect(
            cloud_provider="",
            cloud_region="",
            endpoint="https://flink.eu-west-1.gcp.confluent.cloud",
        )
        params = conn.get_artifacts_api()._scope_params(with_environment=False)
        assert params == {"cloud": "GCP", "region": "eu-west-1"}

    def test_unrecoverable_scope_fails_on_use_not_on_composition(self) -> None:
        conn = _connect(
            cloud_provider="", cloud_region="", endpoint="https://flink.internal.example.com"
        )
        api = conn.get_artifacts_api()  # composes fine
        with pytest.raises(ProgrammingError, match="cloud_provider and cloud_region"):
            api.list()

    def test_api_is_composed_once_and_cached(self) -> None:
        conn = _connect()
        assert conn.get_artifacts_api() is conn.get_artifacts_api()

    def test_api_is_composed_lazily(self) -> None:
        assert _connect()._artifacts_api is None

    def test_api_receives_the_connections_environment_and_scope(self) -> None:
        conn = _connect()
        api = conn.get_artifacts_api()
        assert api._scope_params() == {
            "cloud": "AWS",
            "region": "us-east-2",
            "environment": "env-1",
        }


class TestArtifactResponseLogging:
    @pytest.mark.parametrize("method", ["GET", "POST"])
    def test_presigned_secrets_are_not_debug_logged(self, method: str, caplog) -> None:
        secret = "X-Amz-Signature=deadbeef"
        conn = _connect()
        client = Mock()
        client.request.return_value = Mock(status_code=200, content=secret.encode())
        conn._controlplane._client = client
        with caplog.at_level("DEBUG", logger="confluent_sql"):
            conn.get_artifacts_api()._request("/artifact/v1/x", method=method)
        assert secret not in caplog.text
