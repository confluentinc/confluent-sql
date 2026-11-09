"""Unit tests for inferring cloud_provider / cloud_region (and so the Flink endpoint) from the
database's Kafka cluster via CMK at connect() time (#310)."""

from __future__ import annotations

from typing import Any

import httpx
import pytest

from confluent_sql import (
    AmbiguousClusterError,
    InterfaceError,
    OperationalError,
    Timeouts,
    connect,
)
from confluent_sql.artifacts import ArtifactScope
from confluent_sql.connection import Connection, ControlPlaneSurface
from confluent_sql.service_client import apply_status_policy

pytestmark = pytest.mark.unit

ENV = "env-abc123"
DB = "my_cluster"
CLUSTERS_URL = "/cmk/v2/clusters"
GLOBAL_KEY = {"global_api_key": "gk", "global_api_secret": "gs"}


def _cluster(
    cluster_id: str = "lkc-1", name: str = DB, cloud: str = "AWS", region: str = "us-east-2"
):
    return {"id": cluster_id, "spec": {"display_name": name, "cloud": cloud, "region": region}}


class FakeCmk:
    """Stands in for `Connection._controlplane_request`, routing on the URL and counting calls."""

    def __init__(self, clusters: list[dict] | None = None, by_id_status: int = 200) -> None:
        self.clusters = clusters if clusters is not None else [_cluster()]
        self.by_id_status = by_id_status
        self.calls: list[tuple[ControlPlaneSurface, str]] = []

    def __call__(self, surface, url, method="GET", raise_for_status=True, **kwargs):
        self.calls.append((surface, url))
        if url == CLUSTERS_URL:
            return self._ok({"data": self.clusters, "metadata": {}})
        if url.startswith(CLUSTERS_URL + "/"):
            response = httpx.Response(
                self.by_id_status,
                json=self.clusters[0] if self.by_id_status == 200 else {},
                request=httpx.Request("GET", "https://api.confluent.cloud" + url),
            )
            apply_status_policy(
                response, kwargs.get("status_exceptions"), kwargs.get("error_prefix")
            )
            return response
        raise AssertionError(f"unexpected request {url}")

    @staticmethod
    def _ok(body: dict) -> httpx.Response:
        return httpx.Response(
            200,
            json=body,
            request=httpx.Request("GET", "https://api.confluent.cloud" + CLUSTERS_URL),
        )

    @property
    def urls(self) -> list[str]:
        return [url for _, url in self.calls]


@pytest.fixture
def cmk(mocker) -> FakeCmk:
    fake = FakeCmk()
    mocker.patch.object(Connection, "_controlplane_request", new=fake)
    return fake


def _connect(**overrides: Any) -> Connection:
    kwargs: dict[str, Any] = {
        "environment_id": ENV,
        "organization_id": "org-1",
        "database": DB,
        "timeouts": Timeouts(),
        **GLOBAL_KEY,
    }
    kwargs.update(overrides)
    return connect(**kwargs)


class TestInference:
    def test_global_key_infers_endpoint_from_cmk(self, cmk: FakeCmk) -> None:
        conn = _connect()
        assert conn._flink_endpoint == "https://flink.us-east-2.aws.confluent.cloud"

    @pytest.mark.parametrize(
        "cloud,region",
        [("AWS", "us-east-2"), ("GCP", "europe-west1"), ("AZURE", "eastus")],
    )
    def test_cloud_is_lowercased_in_endpoint(self, cmk: FakeCmk, cloud: str, region: str) -> None:
        cmk.clusters = [_cluster(cloud=cloud, region=region)]
        conn = _connect()
        assert conn._flink_endpoint == f"https://flink.{region}.{cloud.lower()}.confluent.cloud"

    def test_one_list_call_serves_cluster_id_and_cloud_region(self, cmk: FakeCmk) -> None:
        conn = _connect()
        assert conn._resolve_kafka_cluster_id() == "lkc-1"
        assert cmk.urls == [CLUSTERS_URL]
        assert cmk.calls[0][0] is ControlPlaneSurface.CMK

    def test_artifact_scope_uses_inferred_values(self, cmk: FakeCmk) -> None:
        api = _connect().get_artifacts_api()
        assert api._artifact_scope() == ArtifactScope("AWS", "us-east-2")

    def test_seeded_cluster_id_uses_get_by_id_and_needs_no_database(self, cmk: FakeCmk) -> None:
        conn = _connect(database="", database_kafka_cluster_id="lkc-1")
        assert conn._flink_endpoint == "https://flink.us-east-2.aws.confluent.cloud"
        assert cmk.urls == [f"{CLUSTERS_URL}/lkc-1"]
        assert conn._resolve_kafka_cluster_id() == "lkc-1"

    def test_no_match_raises_from_connect(self, cmk: FakeCmk) -> None:
        cmk.clusters = [_cluster(name="other")]
        with pytest.raises(OperationalError, match=f"No Kafka cluster named '{DB}'"):
            _connect()

    def test_multiple_match_raises_from_connect(self, cmk: FakeCmk) -> None:
        cmk.clusters = [_cluster("lkc-a"), _cluster("lkc-b", cloud="GCP", region="europe-west1")]
        with pytest.raises(AmbiguousClusterError, match="lkc-a, lkc-b") as exc:
            _connect()
        assert isinstance(exc.value, OperationalError)
        assert exc.value.cluster_ids == ["lkc-a", "lkc-b"]
        assert exc.value.clusters == [
            ("lkc-a", "aws", "us-east-2"),
            ("lkc-b", "gcp", "europe-west1"),
        ]
        assert "database_kafka_cluster_id" in str(exc.value)

    @pytest.mark.parametrize("status", [403, 404])
    def test_by_id_error_status_translated(self, cmk: FakeCmk, status: int) -> None:
        cmk.by_id_status = status
        with pytest.raises(OperationalError, match="lkc-1"):
            _connect(database="", database_kafka_cluster_id="lkc-1")

    def test_failed_inference_closes_connection(self, cmk: FakeCmk, mocker) -> None:
        cmk.clusters = []
        close_spy = mocker.spy(Connection, "close")
        with pytest.raises(OperationalError):
            _connect()
        close_spy.assert_called_once()


class TestPrecedence:
    def test_explicit_endpoint_makes_no_cmk_call(self, cmk: FakeCmk) -> None:
        conn = _connect(endpoint="https://flink.private.example.com/")
        assert conn._flink_endpoint == "https://flink.private.example.com"
        assert cmk.calls == []

    def test_explicit_pair_makes_no_cmk_call(self, cmk: FakeCmk) -> None:
        conn = _connect(cloud_provider="gcp", cloud_region="eu-west-1")
        assert conn._flink_endpoint == "https://flink.eu-west-1.gcp.confluent.cloud"
        assert cmk.calls == []

    def test_explicit_pair_beside_endpoint_wins_for_artifacts(self, cmk: FakeCmk) -> None:
        conn = _connect(
            endpoint="https://flink.dom1.us-east-2.aws.private.confluent.cloud",
            cloud_provider="gcp",
            cloud_region="eu-west-1",
        )
        assert conn.get_artifacts_api()._artifact_scope() == ArtifactScope("GCP", "eu-west-1")
        assert cmk.calls == []

    @pytest.mark.parametrize(
        "kwargs,message",
        [
            ({"cloud_provider": "aws"}, "Cloud region is required"),
            ({"cloud_region": "us-east-2"}, "Cloud provider is required"),
        ],
    )
    def test_half_supplied_pair_is_not_completed_by_inference(
        self, cmk: FakeCmk, kwargs: dict, message: str
    ) -> None:
        with pytest.raises(InterfaceError, match=message):
            _connect(**kwargs)
        assert cmk.calls == []


class TestGate:
    def test_no_database_cannot_infer(self, cmk: FakeCmk) -> None:
        with pytest.raises(InterfaceError, match="database") as exc:
            _connect(database="")
        assert "endpoint" in str(exc.value)
        assert cmk.calls == []

    def test_flink_key_only_cannot_infer(self, cmk: FakeCmk) -> None:
        with pytest.raises(InterfaceError, match="global API key"):
            _connect(
                global_api_key="",
                global_api_secret="",
                flink_api_key="fk",
                flink_api_secret="fs",
            )
        assert cmk.calls == []

    def test_byoidc_cannot_infer(self, cmk: FakeCmk) -> None:
        with pytest.raises(InterfaceError, match="Cloud provider is required"):
            _connect(
                global_api_key="",
                global_api_secret="",
                external_access_token="tok",
                identity_pool_id="pool-1",
            )
        assert cmk.calls == []
