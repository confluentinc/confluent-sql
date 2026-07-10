"""Unit tests for the Connection-level Tableflow plumbing and lifecycle methods."""

from __future__ import annotations

from typing import Any
from unittest.mock import Mock

import httpx
import pytest

from confluent_sql import (
    InterfaceError,
    ManagedStorage,
    OperationalError,
    ProgrammingError,
    TableflowPhase,
    TableflowTopicAlreadyExistsError,
    TableflowTopicNotFoundError,
    TableFormat,
)
from confluent_sql.connection import Connection, _resolve_tableflow_credentials, connect
from confluent_sql.tableflow import TableflowTopic

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


def _error_response(status_code: int) -> Mock:
    """A response mock whose raise_for_status() raises an HTTPStatusError of the given code."""
    response = Mock()
    response.status_code = status_code

    def _raise() -> None:
        inner = Mock()
        inner.status_code = status_code
        raise httpx.HTTPStatusError("boom", request=Mock(), response=inner)

    response.raise_for_status = _raise
    return response


def _topic_body(*, display_name: str = "orders", phase: str = "PENDING") -> dict:
    return {
        "metadata": {},
        "spec": {
            "display_name": display_name,
            "storage": {"kind": "Managed"},
            "table_formats": ["ICEBERG"],
            "environment": {"id": "env-1"},
            "kafka_cluster": {"id": "lkc-1"},
        },
        "status": {"phase": phase, "write_mode": "APPEND"},
    }


class TestResolveTableflowCredentials:
    """The control-plane credential pick: global wins, else the tableflow pair, else None."""

    def test_global_wins_over_tableflow(self) -> None:
        assert _resolve_tableflow_credentials("gk", "gs", "tk", "ts") == ("gk", "gs")

    def test_tableflow_pair_when_no_global(self) -> None:
        assert _resolve_tableflow_credentials("", "", "tk", "ts") == ("tk", "ts")

    def test_none_when_neither(self) -> None:
        assert _resolve_tableflow_credentials("", "", "", "") is None

    def test_half_tableflow_pair_raises(self) -> None:
        with pytest.raises(InterfaceError, match="tableflow_api_key and tableflow_api_secret"):
            _resolve_tableflow_credentials("", "", "tk", "")

    def test_global_short_circuits_half_tableflow_pair(self) -> None:
        # A usable global key wins and must not be tripped up by an incidental half-supplied
        # Tableflow pair (e.g. one leaked in from the environment).
        assert _resolve_tableflow_credentials("gk", "gs", "tk", "") == ("gk", "gs")


class TestControlplaneClient:
    """Lazy control-plane client creation and the no-credentials failure path."""

    def test_no_controlplane_credentials_raises(self) -> None:
        conn = _connect()  # flink-only creds
        with pytest.raises(ProgrammingError, match="global API key or a tableflow_api_key"):
            conn._get_controlplane_client()

    def test_tableflow_pair_authenticates_controlplane(self, mocker) -> None:
        spy = mocker.patch("confluent_sql.connection.httpx.BasicAuth", wraps=httpx.BasicAuth)
        conn = _connect(tableflow_api_key="tk", tableflow_api_secret="ts")
        client = conn._get_controlplane_client()
        assert str(client.base_url) == "https://api.confluent.cloud"
        spy.assert_called_with(username="tk", password="ts")

    def test_custom_controlplane_endpoint_trailing_slash_stripped(self) -> None:
        conn = _connect(
            tableflow_api_key="tk",
            tableflow_api_secret="ts",
            controlplane_endpoint="https://stag.example.com/",
        )
        assert str(conn._get_controlplane_client().base_url) == "https://stag.example.com"

    def test_controlplane_client_cached(self) -> None:
        conn = _connect(tableflow_api_key="tk", tableflow_api_secret="ts")
        assert conn._get_controlplane_client() is conn._get_controlplane_client()

    def test_closed_connection_raises(self) -> None:
        conn = _connect(tableflow_api_key="tk", tableflow_api_secret="ts")
        conn.close()
        with pytest.raises(InterfaceError, match="Connection is closed"):
            conn._get_controlplane_client()

    def test_http_timeout_applied_to_controlplane_client(self) -> None:
        conn = _connect(tableflow_api_key="tk", tableflow_api_secret="ts", http_timeout_secs=10.0)
        assert conn._get_controlplane_client().timeout.read == 10.0

    def test_close_closes_created_controlplane_client(self) -> None:
        conn = _connect(tableflow_api_key="tk", tableflow_api_secret="ts")
        client = conn._get_controlplane_client()  # force creation
        conn.close()
        assert client.is_closed


@pytest.mark.parametrize(
    "method_name", ["_tableflow_request", "_cmk_request", "_organization_lookup_request"]
)
class TestControlplaneRequestWrappers:
    """_tableflow_request / _cmk_request / _organization_lookup_request are structurally
    identical thin wrappers sharing the control-plane client -- each must independently round-trip
    through it and map HTTP/network errors, rather than trusting the others "by similarity"."""

    def test_success_returns_response(self, method_name: str) -> None:
        conn = _connect(tableflow_api_key="tk", tableflow_api_secret="ts")
        client = Mock()
        client.request = Mock(return_value=_ok_response({"ok": True}))
        conn._get_controlplane_client = Mock(return_value=client)  # type: ignore[method-assign]
        assert getattr(conn, method_name)("/x").json() == {"ok": True}
        client.request.assert_called_once()

    def test_http_error_becomes_operational_error(self, method_name: str) -> None:
        conn = _connect(tableflow_api_key="tk", tableflow_api_secret="ts")
        client = Mock()
        client.request = Mock(return_value=_error_response(500))
        conn._get_controlplane_client = Mock(return_value=client)  # type: ignore[method-assign]
        with pytest.raises(OperationalError) as exc:
            getattr(conn, method_name)("/x")
        assert exc.value.http_status_code == 500

    def test_network_error_becomes_operational_error(self, method_name: str) -> None:
        # A network-level failure (no HTTP response) must surface as a DB-API OperationalError,
        # not leak the raw httpx.RequestError -- and carry no http_status_code.
        conn = _connect(tableflow_api_key="tk", tableflow_api_secret="ts")
        client = Mock()
        client.request = Mock(side_effect=httpx.ConnectError("connection refused"))
        conn._get_controlplane_client = Mock(return_value=client)  # type: ignore[method-assign]
        with pytest.raises(OperationalError) as exc:
            getattr(conn, method_name)("/x")
        assert exc.value.http_status_code is None


class TestResolveKafkaClusterId:
    """Cluster-id resolution: seeded short-circuit, CMK lookup, caching, and the raise paths."""

    def test_seeded_id_skips_cmk(self) -> None:
        conn = _connect(database_kafka_cluster_id="lkc-seed")
        conn._cmk_request = Mock(
            side_effect=AssertionError("CMK must not be called")
        )
        assert conn._resolve_kafka_cluster_id() == "lkc-seed"

    def test_lookup_single_match_and_caches(self) -> None:
        conn = _connect(global_api_key="gk", global_api_secret="gs", database="ProdCluster")
        page = _ok_response(
            {"data": [{"id": "lkc-99", "spec": {"display_name": "ProdCluster"}}], "metadata": {}}
        )
        conn._cmk_request = Mock(return_value=page)
        assert conn._resolve_kafka_cluster_id() == "lkc-99"
        assert conn._resolve_kafka_cluster_id() == "lkc-99"
        conn._cmk_request.assert_called_once()

    def test_zero_match_raises(self) -> None:
        conn = _connect(global_api_key="gk", global_api_secret="gs", database="Missing")
        conn._cmk_request = Mock(
            return_value=_ok_response({"data": [], "metadata": {}})
        )
        with pytest.raises(OperationalError, match="No Kafka cluster named 'Missing'"):
            conn._resolve_kafka_cluster_id()

    def test_multi_match_raises_listing_ids(self) -> None:
        conn = _connect(global_api_key="gk", global_api_secret="gs", database="Dup")
        conn._cmk_request = Mock(
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
        with pytest.raises(OperationalError, match="lkc-a, lkc-b") as exc:
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
        conn._cmk_request = Mock(side_effect=[page1, page2])
        assert conn._resolve_kafka_cluster_id() == "lkc-2"
        assert conn._cmk_request.call_count == 2


class TestResolveOrganizationId:
    """organization_id inference via /org/v2/organizations (#132): pagination, zero-org and
    multi-org error paths. All built against a Connection with a non-empty organization_id so
    construction needs no mocking -- the methods under test are called directly."""

    def test_lookup_single_org_returns_it(self) -> None:
        conn = _connect(global_api_key="gk", global_api_secret="gs")
        conn._organization_lookup_request = Mock(
            return_value=_ok_response({"data": [{"id": "org-99"}], "metadata": {}})
        )
        assert conn._resolve_organization_id() == "org-99"

    def test_lookup_paginates(self) -> None:
        conn = _connect(global_api_key="gk", global_api_secret="gs")
        page1 = _ok_response(
            {
                "data": [{"id": "org-1"}],
                "metadata": {
                    "next": "https://api.confluent.cloud/org/v2/organizations?page_token=tok2"
                },
            }
        )
        page2 = _ok_response({"data": [{"id": "org-2"}], "metadata": {}})
        conn._organization_lookup_request = Mock(side_effect=[page1, page2])
        assert conn._lookup_organization_ids() == ["org-1", "org-2"]
        assert conn._organization_lookup_request.call_count == 2

    def test_zero_orgs_raises(self) -> None:
        conn = _connect(global_api_key="gk", global_api_secret="gs")
        conn._organization_lookup_request = Mock(
            return_value=_ok_response({"data": [], "metadata": {}})
        )
        with pytest.raises(
            OperationalError,
            match="No organizations visible to this API key; cannot infer organization_id.",
        ):
            conn._resolve_organization_id()

    def test_multiple_orgs_raises_naming_ambiguity(self) -> None:
        conn = _connect(global_api_key="gk", global_api_secret="gs")
        conn._organization_lookup_request = Mock(
            return_value=_ok_response({"data": [{"id": "org-a"}, {"id": "org-b"}], "metadata": {}})
        )
        with pytest.raises(
            OperationalError,
            match="Multiple organizations visible to this API key",
        ) as exc:
            conn._resolve_organization_id()
        assert "organization_id" in str(exc.value)
        assert "connect()" in str(exc.value)


class TestEnableTableflow:
    """enable_tableflow request shaping, 409 mapping, and the wait-for-running behavior."""

    def test_posts_expected_body(self) -> None:
        conn = _connect(database_kafka_cluster_id="lkc-1")
        conn._tableflow_request = Mock(
            return_value=_ok_response(_topic_body(), status_code=202)
        )
        topic = conn.enable_tableflow(
            "orders",
            tableflow_formats=TableFormat.ICEBERG,
            storage=ManagedStorage(),
            wait_for_running=False,  # this test asserts request shape, not the wait loop
        )
        args, kwargs = conn._tableflow_request.call_args
        assert args[0] == "/tableflow/v1/tableflow-topics"
        assert kwargs["method"] == "POST"
        assert kwargs["json"]["spec"]["display_name"] == "orders"
        assert kwargs["json"]["spec"]["table_formats"] == ["ICEBERG"]
        assert kwargs["json"]["spec"]["kafka_cluster"] == {"id": "lkc-1"}
        assert isinstance(topic, TableflowTopic)
        assert topic.phase is TableflowPhase.PENDING

    def test_both_formats_collection_reaches_body(self) -> None:
        conn = _connect(database_kafka_cluster_id="lkc-1")
        conn._tableflow_request = Mock(
            return_value=_ok_response(_topic_body(), status_code=202)
        )
        conn.enable_tableflow(
            "orders",
            tableflow_formats={TableFormat.DELTA, TableFormat.ICEBERG},
            storage=ManagedStorage(),
            wait_for_running=False,  # this test asserts request shape, not the wait loop
        )
        _, kwargs = conn._tableflow_request.call_args
        assert kwargs["json"]["spec"]["table_formats"] == ["ICEBERG", "DELTA"]

    def test_409_raises_already_exists(self) -> None:
        conn = _connect(database_kafka_cluster_id="lkc-1")
        conn._tableflow_request = Mock(return_value=_error_response(409))
        with pytest.raises(TableflowTopicAlreadyExistsError) as exc:
            conn.enable_tableflow(
                "orders", tableflow_formats=TableFormat.ICEBERG, storage=ManagedStorage()
            )
        assert exc.value.table_name == "orders"

    def test_blocks_for_running_by_default(self, mocker) -> None:
        # No wait_for_running argument -> default (True) must poll to RUNNING, not return PENDING.
        conn = _connect(database_kafka_cluster_id="lkc-1")
        conn._tableflow_request = Mock(
            return_value=_ok_response(_topic_body(), status_code=202)
        )
        mocker.patch("confluent_sql.connection.sleep_with_backoff", return_value=iter([None]))
        conn.get_tableflow = Mock(  # type: ignore[method-assign]
            return_value=TableflowTopic.from_response(_topic_body(phase="RUNNING"))
        )
        topic = conn.enable_tableflow(
            "orders", tableflow_formats=TableFormat.ICEBERG, storage=ManagedStorage()
        )
        assert topic.phase is TableflowPhase.RUNNING
        conn.get_tableflow.assert_called()

    def test_wait_for_running_polls_to_running(self, mocker) -> None:
        conn = _connect(database_kafka_cluster_id="lkc-1")
        conn._tableflow_request = Mock(
            return_value=_ok_response(_topic_body(), status_code=202)
        )
        mocker.patch("confluent_sql.connection.sleep_with_backoff", return_value=iter([None]))
        conn.get_tableflow = Mock(  # type: ignore[method-assign]
            return_value=TableflowTopic.from_response(_topic_body(phase="RUNNING"))
        )
        topic = conn.enable_tableflow(
            "orders",
            tableflow_formats=TableFormat.ICEBERG,
            storage=ManagedStorage(),
            wait_for_running=True,
        )
        assert topic.phase is TableflowPhase.RUNNING

    def test_wait_for_running_raises_on_failed(self, mocker) -> None:
        conn = _connect(database_kafka_cluster_id="lkc-1")
        conn._tableflow_request = Mock(
            return_value=_ok_response(_topic_body(), status_code=202)
        )
        failed = _topic_body(phase="FAILED")
        failed["status"]["error_message"] = "schema boom"
        failed["status"]["failing_table_formats"] = [
            {"format": "ICEBERG", "error_message": "bad schema"}
        ]
        mocker.patch("confluent_sql.connection.sleep_with_backoff", return_value=iter([None]))
        conn.get_tableflow = Mock(return_value=TableflowTopic.from_response(failed))  # type: ignore[method-assign]
        with pytest.raises(OperationalError, match="schema boom"):
            conn.enable_tableflow(
                "orders",
                tableflow_formats=TableFormat.ICEBERG,
                storage=ManagedStorage(),
                wait_for_running=True,
            )

    def test_wait_for_running_times_out(self, mocker) -> None:
        conn = _connect(database_kafka_cluster_id="lkc-1")
        conn._tableflow_request = Mock(
            return_value=_ok_response(_topic_body(), status_code=202)
        )
        # No backoff iterations: the topic never leaves PENDING, so the wait gives up.
        mocker.patch("confluent_sql.connection.sleep_with_backoff", return_value=iter([]))
        with pytest.raises(OperationalError, match="did not reach RUNNING within"):
            conn.enable_tableflow(
                "orders",
                tableflow_formats=TableFormat.ICEBERG,
                storage=ManagedStorage(),
                wait_for_running=True,
                timeout=1,
            )

    def test_other_error_status_raises_operational(self) -> None:
        conn = _connect(database_kafka_cluster_id="lkc-1")
        conn._tableflow_request = Mock(return_value=_error_response(422))
        with pytest.raises(OperationalError) as exc:
            conn.enable_tableflow(
                "orders", tableflow_formats=TableFormat.ICEBERG, storage=ManagedStorage()
            )
        assert exc.value.http_status_code == 422

    def test_wait_for_running_returns_immediately_if_already_running(self) -> None:
        conn = _connect(database_kafka_cluster_id="lkc-1")
        conn._tableflow_request = Mock(
            return_value=_ok_response(_topic_body(phase="RUNNING"), status_code=202)
        )
        conn.get_tableflow = Mock(side_effect=AssertionError("should not poll"))  # type: ignore[method-assign]
        topic = conn.enable_tableflow(
            "orders",
            tableflow_formats=TableFormat.ICEBERG,
            storage=ManagedStorage(),
            wait_for_running=True,
        )
        assert topic.phase is TableflowPhase.RUNNING


class TestGetTableflow:
    """get_tableflow request shaping and 404 mapping."""

    def test_get_shapes_request(self) -> None:
        conn = _connect(database_kafka_cluster_id="lkc-1")
        conn._tableflow_request = Mock(return_value=_ok_response(_topic_body()))
        conn.get_tableflow("orders")
        args, kwargs = conn._tableflow_request.call_args
        assert args[0] == "/tableflow/v1/tableflow-topics/orders"
        assert kwargs["params"] == {"environment": "env-1", "spec.kafka_cluster": "lkc-1"}

    def test_404_raises_not_found(self) -> None:
        conn = _connect(database_kafka_cluster_id="lkc-1")
        conn._tableflow_request = Mock(return_value=_error_response(404))
        with pytest.raises(TableflowTopicNotFoundError) as exc:
            conn.get_tableflow("orders")
        assert exc.value.table_name == "orders"

    def test_other_error_status_raises_operational(self) -> None:
        conn = _connect(database_kafka_cluster_id="lkc-1")
        conn._tableflow_request = Mock(return_value=_error_response(500))
        with pytest.raises(OperationalError) as exc:
            conn.get_tableflow("orders")
        assert exc.value.http_status_code == 500


class TestDisableTableflow:
    """disable_tableflow request shaping, 404 mapping, and wait-for-removal polling."""

    def test_delete_shapes_request(self) -> None:
        conn = _connect(database_kafka_cluster_id="lkc-1")
        conn._tableflow_request = Mock(return_value=_ok_response(status_code=204))
        conn.disable_tableflow("orders", wait_for_removal=False)  # asserts request shape only
        args, kwargs = conn._tableflow_request.call_args
        assert args[0] == "/tableflow/v1/tableflow-topics/orders"
        assert kwargs["method"] == "DELETE"
        assert kwargs["params"] == {"environment": "env-1", "spec.kafka_cluster": "lkc-1"}

    def test_404_raises_not_found(self) -> None:
        conn = _connect(database_kafka_cluster_id="lkc-1")
        conn._tableflow_request = Mock(return_value=_error_response(404))
        with pytest.raises(TableflowTopicNotFoundError):
            conn.disable_tableflow("orders")

    def test_other_error_status_raises_operational(self) -> None:
        conn = _connect(database_kafka_cluster_id="lkc-1")
        conn._tableflow_request = Mock(return_value=_error_response(500))
        with pytest.raises(OperationalError) as exc:
            conn.disable_tableflow("orders")
        assert exc.value.http_status_code == 500

    def test_waits_for_removal_by_default(self, mocker) -> None:
        # No wait_for_removal argument -> the default (True) must poll until the topic 404s.
        conn = _connect(database_kafka_cluster_id="lkc-1")
        conn._tableflow_request = Mock(return_value=_ok_response(status_code=204))
        mocker.patch("confluent_sql.connection.sleep_with_backoff", return_value=iter([]))
        conn.get_tableflow = Mock(  # type: ignore[method-assign]
            side_effect=TableflowTopicNotFoundError("gone", table_name="orders")
        )
        conn.disable_tableflow("orders")
        conn.get_tableflow.assert_called_once()

    def test_wait_for_removal_polls_until_not_found(self, mocker) -> None:
        conn = _connect(database_kafka_cluster_id="lkc-1")
        conn._tableflow_request = Mock(return_value=_ok_response(status_code=204))
        mocker.patch("confluent_sql.connection.sleep_with_backoff", return_value=iter([None]))
        conn.get_tableflow = Mock(  # type: ignore[method-assign]
            side_effect=[
                TableflowTopic.from_response(_topic_body(phase="RUNNING")),
                TableflowTopicNotFoundError("gone", table_name="orders"),
            ]
        )
        conn.disable_tableflow("orders", wait_for_removal=True)
        assert conn.get_tableflow.call_count == 2

    def test_wait_for_removal_returns_when_already_gone(self, mocker) -> None:
        conn = _connect(database_kafka_cluster_id="lkc-1")
        conn._tableflow_request = Mock(return_value=_ok_response(status_code=204))
        mocker.patch("confluent_sql.connection.sleep_with_backoff", return_value=iter([]))
        conn.get_tableflow = Mock(  # type: ignore[method-assign]
            side_effect=TableflowTopicNotFoundError("gone", table_name="orders")
        )
        conn.disable_tableflow("orders", wait_for_removal=True)
        conn.get_tableflow.assert_called_once()

    def test_wait_for_removal_times_out(self, mocker) -> None:
        conn = _connect(database_kafka_cluster_id="lkc-1")
        conn._tableflow_request = Mock(return_value=_ok_response(status_code=204))
        mocker.patch("confluent_sql.connection.sleep_with_backoff", return_value=iter([]))
        # Topic never 404s, so removal can't be confirmed within the budget.
        conn.get_tableflow = Mock(  # type: ignore[method-assign]
            return_value=TableflowTopic.from_response(_topic_body(phase="RUNNING"))
        )
        with pytest.raises(OperationalError, match="was not removed within"):
            conn.disable_tableflow("orders", wait_for_removal=True, timeout=1)
