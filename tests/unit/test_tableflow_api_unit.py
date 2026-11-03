"""Unit tests for `TableflowApi` (lifecycle choreography) and how `Connection` composes it."""

from __future__ import annotations

from typing import Any
from unittest.mock import Mock

import httpx
import pytest

from confluent_sql import (
    InterfaceError,
    ManagedStorage,
    OperationalError,
    TableflowPhase,
    TableflowTopicAlreadyExistsError,
    TableflowTopicConfig,
    TableflowTopicNotFoundError,
    TableFormat,
)
from confluent_sql.connection import Connection, ControlPlaneSurface, connect
from confluent_sql.exceptions import RequestTimeoutError
from confluent_sql.service_client import apply_status_policy
from confluent_sql.tableflow import TableflowApi, TableflowTopic

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


def _request_answering(response: Mock) -> Mock:
    """A stand-in for the `Requester` handed to `TableflowApi` that answers with `response`,
    applying the `status_exceptions` / `error_prefix` the caller passed just as the real
    `ServiceClient.request` would."""

    def serve(*args: Any, status_exceptions=None, error_prefix=None, **kwargs: Any) -> Mock:
        apply_status_policy(response, status_exceptions, error_prefix)
        return response

    return Mock(side_effect=serve)


def _failing_request(status_code: int) -> Mock:
    return _request_answering(_error_response(status_code))


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


def _api() -> TableflowApi:
    """A `TableflowApi` over an unused `Requester`; tests replace `api._request` as they need."""
    return TableflowApi(Mock(), environment_id="env-1", resolve_kafka_cluster_id=lambda: "lkc-1")


class TestTableflowApiComposition:
    """What `Connection` hands the `TableflowApi` it composes."""

    def test_api_receives_the_connections_environment_and_cluster_resolver(self) -> None:
        conn = _connect(database_kafka_cluster_id="lkc-9")
        api = conn.get_tableflow_api()
        assert api._tableflow_topic_params() == {
            "environment": "env-1",
            "spec.kafka_cluster": "lkc-9",
        }

    def test_api_requests_carry_the_tableflow_credential(self) -> None:
        conn = _connect(tableflow_api_key="tk", tableflow_api_secret="ts")
        client = Mock()
        client.request = Mock(return_value=Mock(content=b"{}"))
        conn._controlplane._client = client
        conn.get_tableflow_api()._request(
            "/x", method="POST", json={"a": 1}, raise_for_status=False
        )
        client.request.assert_called_once_with(
            "POST", "/x", json={"a": 1}, auth=conn._auth_for(ControlPlaneSurface.TABLEFLOW)
        )

    def test_connection_no_longer_carries_the_lifecycle_methods(self) -> None:
        for name in ("enable_tableflow", "get_tableflow", "update_tableflow", "disable_tableflow"):
            assert not hasattr(Connection, name)


class TestGetTableflowApi:
    def test_api_is_composed_lazily(self) -> None:
        assert _connect(tableflow_api_key="tk", tableflow_api_secret="ts")._tableflow_api is None

    def test_api_is_composed_once_and_cached(self) -> None:
        conn = _connect(tableflow_api_key="tk", tableflow_api_secret="ts")
        assert conn.get_tableflow_api() is conn.get_tableflow_api()


class TestEnableTableflow:
    """`TableflowApi.enable` request shaping, 409 mapping, and the wait-for-running behavior."""

    def test_posts_expected_body(self) -> None:
        api = _api()
        api._request = Mock(return_value=_ok_response(_topic_body(), status_code=202))
        topic = api.enable(
            "orders",
            table_formats=TableFormat.ICEBERG,
            storage=ManagedStorage(),
            wait_for_running=False,  # this test asserts request shape, not the wait loop
        )
        args, kwargs = api._request.call_args
        assert args[0] == "/tableflow/v1/tableflow-topics"
        assert kwargs["method"] == "POST"
        assert kwargs["json"]["spec"]["display_name"] == "orders"
        assert kwargs["json"]["spec"]["table_formats"] == ["ICEBERG"]
        assert kwargs["json"]["spec"]["kafka_cluster"] == {"id": "lkc-1"}
        assert isinstance(topic, TableflowTopic)
        assert topic.phase is TableflowPhase.PENDING

    def test_both_formats_collection_reaches_body(self) -> None:
        api = _api()
        api._request = Mock(return_value=_ok_response(_topic_body(), status_code=202))
        api.enable(
            "orders",
            table_formats={TableFormat.DELTA, TableFormat.ICEBERG},
            storage=ManagedStorage(),
            wait_for_running=False,  # this test asserts request shape, not the wait loop
        )
        _, kwargs = api._request.call_args
        assert kwargs["json"]["spec"]["table_formats"] == ["ICEBERG", "DELTA"]

    def test_409_raises_already_exists(self) -> None:
        api = _api()
        api._request = _failing_request(409)
        with pytest.raises(TableflowTopicAlreadyExistsError) as exc:
            api.enable("orders", table_formats=TableFormat.ICEBERG, storage=ManagedStorage())
        assert exc.value.table_name == "orders"

    def test_blocks_for_running_by_default(self, mocker) -> None:
        # No wait_for_running argument -> default (True) must poll to RUNNING, not return PENDING.
        api = _api()
        api._request = Mock(return_value=_ok_response(_topic_body(), status_code=202))
        mocker.patch("confluent_sql.polling.sleep_with_backoff", return_value=iter([None]))
        api._get_topic = Mock(  # type: ignore[method-assign]
            return_value=TableflowTopic.from_response(_topic_body(phase="RUNNING"))
        )
        topic = api.enable("orders", table_formats=TableFormat.ICEBERG, storage=ManagedStorage())
        assert topic.phase is TableflowPhase.RUNNING
        api._get_topic.assert_called()

    def test_wait_for_running_polls_to_running(self, mocker) -> None:
        api = _api()
        api._request = Mock(return_value=_ok_response(_topic_body(), status_code=202))
        mocker.patch("confluent_sql.polling.sleep_with_backoff", return_value=iter([None]))
        api._get_topic = Mock(  # type: ignore[method-assign]
            return_value=TableflowTopic.from_response(_topic_body(phase="RUNNING"))
        )
        topic = api.enable(
            "orders",
            table_formats=TableFormat.ICEBERG,
            storage=ManagedStorage(),
            wait_for_running=True,
        )
        assert topic.phase is TableflowPhase.RUNNING

    def test_wait_for_running_survives_a_timed_out_poll(self, mocker) -> None:
        """One slow `get` poll is 'no news yet', not a failure (#236)."""
        api = _api()
        api._request = Mock(return_value=_ok_response(_topic_body(), status_code=202))
        mocker.patch("confluent_sql.polling.sleep_with_backoff", return_value=iter([None] * 2))
        api._get_topic = Mock(  # type: ignore[method-assign]
            side_effect=[
                RequestTimeoutError("error sending request: ReadTimeout: slow"),
                TableflowTopic.from_response(_topic_body(phase="RUNNING")),
            ]
        )
        topic = api.enable(
            "orders",
            table_formats=TableFormat.ICEBERG,
            storage=ManagedStorage(),
            wait_for_running=True,
        )
        assert topic.phase is TableflowPhase.RUNNING
        assert api._get_topic.call_count == 2

    def test_wait_for_running_raises_on_failed(self, mocker) -> None:
        api = _api()
        api._request = Mock(return_value=_ok_response(_topic_body(), status_code=202))
        failed = _topic_body(phase="FAILED")
        failed["status"]["error_message"] = "schema boom"
        failed["status"]["failing_table_formats"] = [
            {"format": "ICEBERG", "error_message": "bad schema"}
        ]
        mocker.patch("confluent_sql.polling.sleep_with_backoff", return_value=iter([None]))
        api._get_topic = Mock(return_value=TableflowTopic.from_response(failed))  # type: ignore[method-assign]
        with pytest.raises(OperationalError, match="schema boom"):
            api.enable(
                "orders",
                table_formats=TableFormat.ICEBERG,
                storage=ManagedStorage(),
                wait_for_running=True,
            )

    def test_wait_for_running_times_out(self, mocker) -> None:
        api = _api()
        api._request = Mock(return_value=_ok_response(_topic_body(), status_code=202))
        # No backoff iterations: the topic never leaves PENDING, so the wait gives up.
        mocker.patch("confluent_sql.polling.sleep_with_backoff", return_value=iter([]))
        with pytest.raises(OperationalError, match="did not reach RUNNING within"):
            api.enable(
                "orders",
                table_formats=TableFormat.ICEBERG,
                storage=ManagedStorage(),
                wait_for_running=True,
                timeout=1,
            )

    def test_other_error_status_raises_operational(self) -> None:
        api = _api()
        api._request = _failing_request(422)
        with pytest.raises(OperationalError) as exc:
            api.enable("orders", table_formats=TableFormat.ICEBERG, storage=ManagedStorage())
        assert exc.value.http_status_code == 422

    def test_other_error_status_extracts_detail(self) -> None:
        api = _api()
        response = Mock()
        response.status_code = 422

        def _raise() -> None:
            inner = Mock()
            inner.status_code = 422
            inner.json.return_value = {"errors": [{"detail": "bad storage config"}]}
            raise httpx.HTTPStatusError("boom", request=Mock(), response=inner)

        response.raise_for_status = _raise
        api._request = _request_answering(response)
        with pytest.raises(OperationalError, match="bad storage config"):
            api.enable("orders", table_formats=TableFormat.ICEBERG, storage=ManagedStorage())

    def test_wait_for_running_returns_immediately_if_already_running(self) -> None:
        api = _api()
        api._request = Mock(
            return_value=_ok_response(_topic_body(phase="RUNNING"), status_code=202)
        )
        api._get_topic = Mock(side_effect=AssertionError("should not poll"))  # type: ignore[method-assign]
        topic = api.enable(
            "orders",
            table_formats=TableFormat.ICEBERG,
            storage=ManagedStorage(),
            wait_for_running=True,
        )
        assert topic.phase is TableflowPhase.RUNNING


class TestGetTableflow:
    """`TableflowApi.get` request shaping and 404 mapping."""

    def test_get_shapes_request(self) -> None:
        api = _api()
        api._request = Mock(return_value=_ok_response(_topic_body()))
        api.get("orders")
        args, kwargs = api._request.call_args
        assert args[0] == "/tableflow/v1/tableflow-topics/orders"
        assert kwargs["params"] == {"environment": "env-1", "spec.kafka_cluster": "lkc-1"}

    def test_404_raises_not_found(self) -> None:
        api = _api()
        api._request = _failing_request(404)
        with pytest.raises(TableflowTopicNotFoundError) as exc:
            api.get("orders")
        assert exc.value.table_name == "orders"

    def test_other_error_status_raises_operational(self) -> None:
        api = _api()
        api._request = _failing_request(500)
        with pytest.raises(OperationalError) as exc:
            api.get("orders")
        assert exc.value.http_status_code == 500

    def test_other_error_status_extracts_detail(self) -> None:
        api = _api()
        response = Mock()
        response.status_code = 500

        def _raise() -> None:
            inner = Mock()
            inner.status_code = 500
            inner.json.return_value = {"errors": [{"detail": "internal server error"}]}
            raise httpx.HTTPStatusError("boom", request=Mock(), response=inner)

        response.raise_for_status = _raise
        api._request = _request_answering(response)
        with pytest.raises(OperationalError, match="internal server error"):
            api.get("orders")


class TestDisableTableflow:
    """`TableflowApi.disable` request shaping, 404 mapping, and wait-for-removal polling."""

    def test_delete_shapes_request(self) -> None:
        api = _api()
        api._request = Mock(return_value=_ok_response(status_code=204))
        api.disable("orders", wait_for_removal=False)  # asserts request shape only
        args, kwargs = api._request.call_args
        assert args[0] == "/tableflow/v1/tableflow-topics/orders"
        assert kwargs["method"] == "DELETE"
        assert kwargs["params"] == {"environment": "env-1", "spec.kafka_cluster": "lkc-1"}

    def test_404_raises_not_found(self) -> None:
        api = _api()
        api._request = _failing_request(404)
        with pytest.raises(TableflowTopicNotFoundError):
            api.disable("orders")

    def test_other_error_status_raises_operational(self) -> None:
        api = _api()
        api._request = _failing_request(500)
        with pytest.raises(OperationalError) as exc:
            api.disable("orders")
        assert exc.value.http_status_code == 500

    def test_other_error_status_extracts_detail(self) -> None:
        api = _api()
        response = Mock()
        response.status_code = 500

        def _raise() -> None:
            inner = Mock()
            inner.status_code = 500
            inner.json.return_value = {"errors": [{"detail": "cannot disable right now"}]}
            raise httpx.HTTPStatusError("boom", request=Mock(), response=inner)

        response.raise_for_status = _raise
        api._request = _request_answering(response)
        with pytest.raises(OperationalError, match="cannot disable right now"):
            api.disable("orders")

    def test_waits_for_removal_by_default(self, mocker) -> None:
        # No wait_for_removal argument -> the default (True) must poll until the topic 404s.
        api = _api()
        api._request = Mock(return_value=_ok_response(status_code=204))
        mocker.patch("confluent_sql.polling.sleep_with_backoff", return_value=iter([]))
        api._get_topic = Mock(  # type: ignore[method-assign]
            side_effect=TableflowTopicNotFoundError("gone", table_name="orders")
        )
        api.disable("orders")
        api._get_topic.assert_called_once()

    def test_wait_for_removal_polls_until_not_found(self, mocker) -> None:
        api = _api()
        api._request = Mock(return_value=_ok_response(status_code=204))
        mocker.patch("confluent_sql.polling.sleep_with_backoff", return_value=iter([None]))
        api._get_topic = Mock(  # type: ignore[method-assign]
            side_effect=[
                TableflowTopic.from_response(_topic_body(phase="RUNNING")),
                TableflowTopicNotFoundError("gone", table_name="orders"),
            ]
        )
        api.disable("orders", wait_for_removal=True)
        assert api._get_topic.call_count == 2

    def test_wait_for_removal_survives_a_timed_out_poll(self, mocker) -> None:
        """A slow `get` poll during removal confirmation is retried, not fatal (#236)."""
        api = _api()
        api._request = Mock(return_value=_ok_response(status_code=204))
        mocker.patch("confluent_sql.polling.sleep_with_backoff", return_value=iter([None] * 2))
        api._get_topic = Mock(  # type: ignore[method-assign]
            side_effect=[
                TableflowTopic.from_response(_topic_body(phase="RUNNING")),
                RequestTimeoutError("error sending request: ReadTimeout: slow"),
                TableflowTopicNotFoundError("gone", table_name="orders"),
            ]
        )
        api.disable("orders", wait_for_removal=True)
        assert api._get_topic.call_count == 3

    def test_wait_for_removal_survives_a_timed_out_initial_probe(self, mocker) -> None:
        """The unpaced probe before the polling loop is also tolerant of one slow GET (#236): the
        DELETE already succeeded, so a timeout here must fall into the loop, not abort."""
        api = _api()
        api._request = Mock(return_value=_ok_response(status_code=204))
        mocker.patch("confluent_sql.polling.sleep_with_backoff", return_value=iter([None]))
        api._get_topic = Mock(  # type: ignore[method-assign]
            side_effect=[
                RequestTimeoutError("error sending request: ReadTimeout: slow"),
                TableflowTopicNotFoundError("gone", table_name="orders"),
            ]
        )
        api.disable("orders", wait_for_removal=True)
        assert api._get_topic.call_count == 2

    def test_wait_for_removal_budget_includes_a_timed_out_initial_probe(self, mocker) -> None:
        """A first probe that times out has already spent part of the wait budget (#236): the
        origin captured before it is handed to sleep_with_backoff as started_at, so no further
        poll is issued once the probe has consumed the whole timeout."""
        before_probe, after_probe = 100.0, 160.0
        mocker.patch(
            "confluent_sql.polling.time.monotonic",
            side_effect=[before_probe, after_probe],
        )
        backoff = mocker.patch("confluent_sql.polling.sleep_with_backoff", return_value=iter([]))
        api = _api()
        api._request = Mock(return_value=_ok_response(status_code=204))
        api._get_topic = Mock(  # type: ignore[method-assign]
            side_effect=RequestTimeoutError("error sending request: ReadTimeout: slow")
        )

        with pytest.raises(OperationalError, match="was not removed within 5 seconds"):
            api.disable("orders", wait_for_removal=True, timeout=5)

        backoff.assert_called_once_with(5, started_at=before_probe)
        api._get_topic.assert_called_once()

    def test_wait_for_removal_chains_last_timeout_when_budget_is_spent(self, mocker) -> None:
        """Probe and polls all time out until the budget is gone: the ordinary 'not removed' error
        is raised with the last RequestTimeoutError chained as its cause (#236)."""
        mocker.patch("confluent_sql.polling.sleep_with_backoff", return_value=iter([None]))
        api = _api()
        api._request = Mock(return_value=_ok_response(status_code=204))
        slow = RequestTimeoutError("error sending request: ReadTimeout: slow")
        api._get_topic = Mock(side_effect=slow)  # type: ignore[method-assign]

        with pytest.raises(OperationalError, match="was not removed within") as exc_info:
            api.disable("orders", wait_for_removal=True, timeout=5)

        assert exc_info.value.__cause__ is slow

    def test_wait_for_removal_returns_when_already_gone(self, mocker) -> None:
        api = _api()
        api._request = Mock(return_value=_ok_response(status_code=204))
        mocker.patch("confluent_sql.polling.sleep_with_backoff", return_value=iter([]))
        api._get_topic = Mock(  # type: ignore[method-assign]
            side_effect=TableflowTopicNotFoundError("gone", table_name="orders")
        )
        api.disable("orders", wait_for_removal=True)
        api._get_topic.assert_called_once()

    def test_wait_for_removal_times_out(self, mocker) -> None:
        api = _api()
        api._request = Mock(return_value=_ok_response(status_code=204))
        mocker.patch("confluent_sql.polling.sleep_with_backoff", return_value=iter([]))
        # Topic never 404s, so removal can't be confirmed within the budget.
        api._get_topic = Mock(  # type: ignore[method-assign]
            return_value=TableflowTopic.from_response(_topic_body(phase="RUNNING"))
        )
        with pytest.raises(OperationalError, match="was not removed within"):
            api.disable("orders", wait_for_removal=True, timeout=1)


class TestUpdateTableflow:
    """`TableflowApi.update` request shaping, the empty-update guard, 404 mapping, error-detail
    extraction, and the wait-for-running behavior."""

    def test_table_formats_only_reaches_body_without_config(self) -> None:
        api = _api()
        api._request = Mock(return_value=_ok_response(_topic_body(), status_code=202))
        api.update(
            "orders",
            table_formats={TableFormat.DELTA, TableFormat.ICEBERG},
            wait_for_running=False,  # this test asserts request shape, not the wait loop
        )
        args, kwargs = api._request.call_args
        assert args[0] == "/tableflow/v1/tableflow-topics/orders"
        assert kwargs["method"] == "PATCH"
        assert kwargs["json"]["spec"]["table_formats"] == ["ICEBERG", "DELTA"]
        assert kwargs["json"]["spec"]["kafka_cluster"] == {"id": "lkc-1"}
        assert "config" not in kwargs["json"]["spec"]

    def test_config_only_reaches_body_without_table_formats(self) -> None:
        api = _api()
        api._request = Mock(return_value=_ok_response(_topic_body(), status_code=202))
        api.update(
            "orders",
            config=TableflowTopicConfig(retention_ms=604800000),
            wait_for_running=False,
        )
        _, kwargs = api._request.call_args
        assert kwargs["json"]["spec"]["config"] == {"retention_ms": "604800000"}
        assert "table_formats" not in kwargs["json"]["spec"]

    def test_neither_argument_raises_interface_error(self) -> None:
        api = _api()
        api._request = Mock(side_effect=AssertionError("should not make a request"))
        with pytest.raises(InterfaceError, match="at least one of table_formats/config"):
            api.update("orders")

    def test_empty_config_and_no_formats_raises_interface_error(self) -> None:
        # A config with nothing actually set on it renders to an empty spec -- same as
        # omitting config entirely, so this must be treated as no update at all.
        api = _api()
        api._request = Mock(side_effect=AssertionError("should not make a request"))
        with pytest.raises(InterfaceError, match="at least one of table_formats/config"):
            api.update("orders", config=TableflowTopicConfig())

    def test_404_raises_not_found(self) -> None:
        api = _api()
        api._request = _failing_request(404)
        with pytest.raises(TableflowTopicNotFoundError) as exc:
            api.update("orders", table_formats=TableFormat.ICEBERG)
        assert exc.value.table_name == "orders"

    def test_other_error_status_extracts_detail(self) -> None:
        api = _api()
        response = Mock()
        response.status_code = 422

        def _raise() -> None:
            inner = Mock()
            inner.status_code = 422
            inner.json.return_value = {"errors": [{"detail": "bad retention_ms"}]}
            raise httpx.HTTPStatusError("boom", request=Mock(), response=inner)

        response.raise_for_status = _raise
        api._request = _request_answering(response)
        with pytest.raises(OperationalError, match="bad retention_ms") as exc:
            api.update("orders", table_formats=TableFormat.ICEBERG)
        assert exc.value.http_status_code == 422

    def test_other_error_status_falls_back_when_errors_list_empty(self) -> None:
        # A parseable body with an empty (or detail-less) errors list must still fall back to
        # "no more details" -- not silently produce a message with nothing after the colon.
        api = _api()
        response = Mock()
        response.status_code = 422

        def _raise() -> None:
            inner = Mock()
            inner.status_code = 422
            inner.json.return_value = {"errors": []}
            raise httpx.HTTPStatusError("boom", request=Mock(), response=inner)

        response.raise_for_status = _raise
        api._request = _request_answering(response)
        with pytest.raises(OperationalError, match="no more details") as exc:
            api.update("orders", table_formats=TableFormat.ICEBERG)
        assert exc.value.http_status_code == 422

    def test_other_error_status_falls_back_when_body_unparseable(self) -> None:
        api = _api()
        api._request = _failing_request(500)
        with pytest.raises(OperationalError, match="no more details") as exc:
            api.update("orders", table_formats=TableFormat.ICEBERG)
        assert exc.value.http_status_code == 500

    def test_blocks_for_running_by_default(self, mocker) -> None:
        # No wait_for_running argument -> default (True) must poll to RUNNING, not return PENDING.
        api = _api()
        api._request = Mock(return_value=_ok_response(_topic_body(), status_code=202))
        mocker.patch("confluent_sql.polling.sleep_with_backoff", return_value=iter([None]))
        api._get_topic = Mock(  # type: ignore[method-assign]
            return_value=TableflowTopic.from_response(_topic_body(phase="RUNNING"))
        )
        topic = api.update("orders", table_formats=TableFormat.ICEBERG)
        assert topic.phase is TableflowPhase.RUNNING
        api._get_topic.assert_called()

    def test_wait_for_running_raises_on_failed(self, mocker) -> None:
        api = _api()
        api._request = Mock(return_value=_ok_response(_topic_body(), status_code=202))
        failed = _topic_body(phase="FAILED")
        failed["status"]["error_message"] = "schema boom"
        failed["status"]["failing_table_formats"] = [
            {"format": "ICEBERG", "error_message": "bad schema"}
        ]
        mocker.patch("confluent_sql.polling.sleep_with_backoff", return_value=iter([None]))
        api._get_topic = Mock(return_value=TableflowTopic.from_response(failed))  # type: ignore[method-assign]
        with pytest.raises(OperationalError, match="schema boom"):
            api.update("orders", table_formats=TableFormat.ICEBERG, wait_for_running=True)

    def test_wait_for_running_times_out(self, mocker) -> None:
        api = _api()
        api._request = Mock(return_value=_ok_response(_topic_body(), status_code=202))
        mocker.patch("confluent_sql.polling.sleep_with_backoff", return_value=iter([]))
        with pytest.raises(OperationalError, match="did not reach RUNNING within"):
            api.update(
                "orders", table_formats=TableFormat.ICEBERG, wait_for_running=True, timeout=1
            )

    def test_wait_for_running_returns_immediately_if_already_running(self) -> None:
        api = _api()
        api._request = Mock(
            return_value=_ok_response(_topic_body(phase="RUNNING"), status_code=202)
        )
        api._get_topic = Mock(side_effect=AssertionError("should not poll"))  # type: ignore[method-assign]
        topic = api.update("orders", table_formats=TableFormat.ICEBERG, wait_for_running=True)
        assert topic.phase is TableflowPhase.RUNNING


class TestWaitsCapRequestTimeouts:
    """Inside a Tableflow wait, each poll's httpx timeouts are capped at the time the wait has left;
    the request that starts the wait is outside it and is not."""

    @staticmethod
    def _response(status: int, body: dict | None = None) -> httpx.Response:
        return httpx.Response(
            status,
            json=body if body is not None else {},
            request=httpx.Request("GET", "https://api.confluent.cloud"),
        )

    def _conn(self, mocker, *responses: httpx.Response):
        conn = _connect(
            database_kafka_cluster_id="lkc-1", tableflow_api_key="tk", tableflow_api_secret="ts"
        )
        request_mock = mocker.patch.object(conn._controlplane.httpx_client, "request")
        request_mock.side_effect = list(responses)
        mocker.patch("time.sleep")
        return conn, request_mock

    def test_enable_wait_for_running(self, mocker) -> None:
        conn, request_mock = self._conn(
            mocker,
            self._response(202, _topic_body()),
            self._response(200, _topic_body(phase="RUNNING")),
        )

        conn.get_tableflow_api().enable(
            "orders", table_formats=TableFormat.ICEBERG, storage=ManagedStorage(), timeout=5
        )

        start, *polls = request_mock.call_args_list
        assert "timeout" not in start.kwargs
        assert len(polls) == 1
        assert 0 < polls[0].kwargs["timeout"].read <= 5

    def test_disable_wait_for_removal(self, mocker) -> None:
        conn, request_mock = self._conn(mocker, self._response(204), self._response(404))

        conn.get_tableflow_api().disable("orders", timeout=5)

        start, *polls = request_mock.call_args_list
        assert "timeout" not in start.kwargs
        assert len(polls) == 1
        assert 0 < polls[0].kwargs["timeout"].read <= 5
