"""Unit tests for Connection-level Tableflow plumbing: control-plane auth, CMK and org lookups."""

from __future__ import annotations

from typing import Any
from unittest.mock import Mock

import httpx
import pytest

from confluent_sql import (
    AmbiguousClusterError,
    AmbiguousOrganizationError,
    InterfaceError,
    OperationalError,
    ProgrammingError,
    Timeouts,
)
from confluent_sql.connection import Connection, ControlPlaneSurface, connect

pytestmark = pytest.mark.unit


def _connect(**overrides: Any) -> Connection:
    """Build a Connection with an explicit endpoint and deterministic (non-env) credentials."""
    kwargs: dict[str, Any] = {
        "flink_api_key": "flink-k",
        "flink_api_secret": "flink-s",
        "global_api_key": "",
        "global_api_secret": "",
        "tableflow_api_key": "",
        "tableflow_api_secret": "",
        "environment_id": "env-1",
        "organization_id": "org-1",
        "endpoint": "https://flink.example.com",
        "database": "",
    }
    kwargs.update(overrides)
    return connect(**kwargs)


def _ok_response(body: dict | None = None, status_code: int = 200) -> Mock:
    """A success response mock whose raise_for_status() is a no-op and .json() returns body."""
    response = Mock()
    response.status_code = status_code
    response.raise_for_status = Mock()
    response.json.return_value = body or {}
    return response


class TestControlplaneClient:
    """Lazy control-plane client creation and the no-credentials failure path."""

    def test_no_controlplane_credentials_raises(self) -> None:
        conn = _connect()  # flink-only creds
        with pytest.raises(ProgrammingError, match="global API key or a tableflow_api_key"):
            conn._auth_for(ControlPlaneSurface.TABLEFLOW)

    def test_tableflow_pair_authenticates_controlplane(self, mocker) -> None:
        spy = mocker.patch("confluent_sql.connection.httpx.BasicAuth", wraps=httpx.BasicAuth)
        conn = _connect(tableflow_api_key="tk", tableflow_api_secret="ts")
        client = conn._controlplane.httpx_client
        assert str(client.base_url) == "https://api.confluent.cloud"
        spy.assert_called_with(username="tk", password="ts")

    def test_custom_controlplane_endpoint_trailing_slash_stripped(self) -> None:
        conn = _connect(
            tableflow_api_key="tk",
            tableflow_api_secret="ts",
            controlplane_endpoint="https://stag.example.com/",
        )
        assert str(conn._controlplane.httpx_client.base_url) == "https://stag.example.com"

    def test_controlplane_client_cached(self) -> None:
        conn = _connect(tableflow_api_key="tk", tableflow_api_secret="ts")
        assert conn._controlplane.httpx_client is conn._controlplane.httpx_client

    def test_closed_connection_raises(self) -> None:
        conn = _connect(tableflow_api_key="tk", tableflow_api_secret="ts")
        conn.close()
        with pytest.raises(InterfaceError, match="Connection is closed"):
            _ = conn._controlplane.httpx_client

    def test_http_timeout_applied_to_controlplane_client(self) -> None:
        conn = _connect(
            tableflow_api_key="tk",
            tableflow_api_secret="ts",
            timeouts=Timeouts(httpx_request_timeout_secs=10.0),
        )
        assert conn._controlplane.httpx_client.timeout.read == 10.0

    def test_close_closes_created_controlplane_client(self) -> None:
        conn = _connect(tableflow_api_key="tk", tableflow_api_secret="ts")
        client = conn._controlplane.httpx_client  # force creation
        conn.close()
        assert client.is_closed


class TestResolveKafkaClusterId:
    """Cluster-id resolution: seeded short-circuit, CMK lookup, caching, and the raise paths."""

    def test_seeded_id_skips_cmk(self) -> None:
        conn = _connect(database_kafka_cluster_id="lkc-seed")
        conn._controlplane_request = Mock(side_effect=AssertionError("CMK must not be called"))
        assert conn._resolve_kafka_cluster_id() == "lkc-seed"

    def test_lookup_single_match_and_caches(self) -> None:
        conn = _connect(global_api_key="gk", global_api_secret="gs", database="ProdCluster")
        page = _ok_response(
            {"data": [{"id": "lkc-99", "spec": {"display_name": "ProdCluster"}}], "metadata": {}}
        )
        conn._controlplane_request = Mock(return_value=page)
        assert conn._resolve_kafka_cluster_id() == "lkc-99"
        assert conn._resolve_kafka_cluster_id() == "lkc-99"
        conn._controlplane_request.assert_called_once()

    def test_zero_match_raises(self) -> None:
        conn = _connect(global_api_key="gk", global_api_secret="gs", database="Missing")
        conn._controlplane_request = Mock(return_value=_ok_response({"data": [], "metadata": {}}))
        with pytest.raises(OperationalError, match="No Kafka cluster named 'Missing'"):
            conn._resolve_kafka_cluster_id()

    def test_multi_match_raises_listing_ids(self) -> None:
        conn = _connect(global_api_key="gk", global_api_secret="gs", database="Dup")
        conn._controlplane_request = Mock(
            return_value=_ok_response(
                {
                    "data": [
                        {"id": "lkc-a", "spec": {"display_name": "Dup"}},
                        {"id": "lkc-b", "spec": {"display_name": "Dup"}},
                    ],
                    "metadata": {},
                }
            )
        )
        with pytest.raises(AmbiguousClusterError, match="lkc-a, lkc-b") as exc:
            conn._resolve_kafka_cluster_id()
        assert "database_kafka_cluster_id" in str(exc.value)

    def test_no_global_key_and_no_seed_raises(self) -> None:
        conn = _connect(tableflow_api_key="tk", tableflow_api_secret="ts", database="ProdCluster")
        with pytest.raises(ProgrammingError, match="requires a global API key"):
            conn._resolve_kafka_cluster_id()

    def test_global_key_but_no_database_raises(self) -> None:
        conn = _connect(global_api_key="gk", global_api_secret="gs", database="")
        with pytest.raises(ProgrammingError, match="without a database name"):
            conn._resolve_kafka_cluster_id()

    def test_lookup_paginates(self) -> None:
        conn = _connect(global_api_key="gk", global_api_secret="gs", database="OnPage2")
        page1 = _ok_response(
            {
                "data": [{"id": "lkc-1", "spec": {"display_name": "Other"}}],
                "metadata": {"next": "https://api.confluent.cloud/cmk/v2/clusters?page_token=tok2"},
            }
        )
        page2 = _ok_response(
            {"data": [{"id": "lkc-2", "spec": {"display_name": "OnPage2"}}], "metadata": {}}
        )
        conn._controlplane_request = Mock(side_effect=[page1, page2])
        assert conn._resolve_kafka_cluster_id() == "lkc-2"
        assert conn._controlplane_request.call_count == 2


class TestResolveOrganizationId:
    """organization_id inference via /org/v2/organizations (#132, #311): pagination, zero-org and
    multi-org error paths. All built against a Connection with a non-empty organization_id so
    construction needs no mocking -- the methods under test are called directly."""

    def test_lookup_single_org_returns_it(self) -> None:
        conn = _connect(global_api_key="gk", global_api_secret="gs")
        conn._controlplane_request = Mock(
            return_value=_ok_response({"data": [{"id": "org-99"}], "metadata": {}})
        )
        assert conn._resolve_organization_id() == "org-99"

    def test_lookup_paginates_and_reads_display_names(self) -> None:
        conn = _connect(global_api_key="gk", global_api_secret="gs")
        page1 = _ok_response(
            {
                "data": [{"id": "org-1", "display_name": "First"}],
                "metadata": {
                    "next": "https://api.confluent.cloud/org/v2/organizations?page_token=tok2"
                },
            }
        )
        page2 = _ok_response({"data": [{"id": "org-2"}], "metadata": {}})
        conn._controlplane_request = Mock(side_effect=[page1, page2])
        assert conn._lookup_organizations() == [("org-1", "First"), ("org-2", None)]
        assert conn._controlplane_request.call_count == 2

    def test_zero_orgs_raises(self) -> None:
        conn = _connect(global_api_key="gk", global_api_secret="gs")
        conn._controlplane_request = Mock(return_value=_ok_response({"data": [], "metadata": {}}))
        with pytest.raises(
            OperationalError,
            match="No organizations visible to this credential; cannot infer organization_id.",
        ) as exc:
            conn._resolve_organization_id()
        assert not isinstance(exc.value, AmbiguousOrganizationError)

    def test_multiple_orgs_raises_ambiguous_error_naming_candidates(self) -> None:
        conn = _connect(global_api_key="gk", global_api_secret="gs")
        conn._controlplane_request = Mock(
            return_value=_ok_response(
                {
                    "data": [{"id": "org-a", "display_name": "Alpha"}, {"id": "org-b"}],
                    "metadata": {},
                }
            )
        )
        with pytest.raises(AmbiguousOrganizationError) as exc:
            conn._resolve_organization_id()
        assert isinstance(exc.value, OperationalError)
        assert exc.value.organizations == [("org-a", "Alpha"), ("org-b", None)]
        assert exc.value.organization_ids == ["org-a", "org-b"]
        message = str(exc.value)
        assert 'org-a ("Alpha")' in message
        assert "org-b" in message
        assert "organization_id" in message
        assert "connect()" in message
