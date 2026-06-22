"""Unit tests for ConnectorApi, driven against a fake ControlPlaneContext (no httpx, no network)."""

from __future__ import annotations

from typing import Any
from unittest.mock import Mock

import httpx
import pytest

from confluent_sql.connectors import ConnectorApi, ConnectorState
from confluent_sql.exceptions import (
    ConnectorAlreadyExistsError,
    ConnectorNotFoundError,
    OperationalError,
)

pytestmark = pytest.mark.unit


def _ok_response(body: dict | None = None, status_code: int = 200) -> Mock:
    """A success response whose raise_for_status() is a no-op and .json() returns body."""
    response = Mock()
    response.status_code = status_code
    response.raise_for_status = Mock()
    response.json = Mock(return_value=body or {})
    return response


def _error_response(status_code: int) -> Mock:
    """A response whose raise_for_status() raises an HTTPStatusError of the given code."""
    response = Mock()
    response.status_code = status_code
    inner = Mock()
    inner.status_code = status_code
    response.raise_for_status = Mock(
        side_effect=httpx.HTTPStatusError("boom", request=Mock(), response=inner)
    )
    return response


def _create_config() -> dict:
    """A config carrying the keys build_create_payload requires; details are irrelevant here."""
    return {
        "connector.class": "DatagenSource",
        "kafka.api.key": "K",
        "kafka.api.secret": "S",
    }


def _read_body() -> dict:
    return {
        "name": "MyDatagen",
        "config": {"connector.class": "DatagenSource", "name": "MyDatagen"},
        "tasks": [],
        "type": "source",
    }


def _status_body(state: str, trace: str = "") -> dict:
    return {
        "name": "MyDatagen",
        "type": "source",
        "connector": {"state": state, "worker_id": "w-1", "trace": trace},
        "tasks": [],
    }


class FakeControlPlane:
    """A ~20-line stand-in satisfying ControlPlaneContext, routing by HTTP method / `/status` path.

    Responses are queued per logical operation (create/read/status/delete) and popped in order, so
    a test can script a state transition by queueing several `status` responses.
    """

    environment_id = "env-123"

    def __init__(self) -> None:
        self._queues: dict[str, list[Mock]] = {
            "create": [],
            "read": [],
            "status": [],
            "delete": [],
        }
        self.requests: list[tuple[str, str]] = []

    def resolve_kafka_cluster_id(self) -> str:
        return "lkc-abc"

    def queue(self, key: str, *responses: Mock) -> None:
        self._queues[key].extend(responses)

    def controlplane_request(
        self, url: str, method: str = "GET", raise_for_status: bool = True, **kwargs: Any
    ) -> Mock:
        self.requests.append((method, url))
        if method == "POST":
            key = "create"
        elif method == "DELETE":
            key = "delete"
        elif url.endswith("/status"):
            key = "status"
        else:
            key = "read"
        queue = self._queues[key]
        if not queue:
            raise AssertionError(f"Wacky -- no queued {key} response for {method} {url}")
        return queue.pop(0)

    def count(self, key: str) -> int:
        method = {"create": "POST", "delete": "DELETE"}.get(key)
        if key == "status":
            return sum(1 for m, u in self.requests if m == "GET" and u.endswith("/status"))
        if key == "read":
            return sum(1 for m, u in self.requests if m == "GET" and not u.endswith("/status"))
        return sum(1 for m, _ in self.requests if m == method)


@pytest.fixture()
def no_sleep(mocker):
    """Neutralize backoff pacing so wait loops iterate instantly; yields up to 20 times."""
    return mocker.patch(
        "confluent_sql.connectors.sleep_with_backoff", return_value=iter([None] * 20)
    )


class TestConnectorApiCreate:
    """create() POSTs, then reads /status -- always once, looping only when waiting."""

    def test_blocks_until_running(self, no_sleep) -> None:
        ctx = FakeControlPlane()
        ctx.queue("create", _ok_response(_read_body(), status_code=201))
        ctx.queue(
            "status",
            _ok_response(_status_body("PROVISIONING")),
            _ok_response(_status_body("RUNNING")),
        )
        api = ConnectorApi(ctx)

        connector = api.create("MyDatagen", config=_create_config())

        assert connector.state is ConnectorState.RUNNING
        assert ctx.count("status") == 2

    def test_wait_false_does_exactly_one_status_read(self, no_sleep) -> None:
        ctx = FakeControlPlane()
        ctx.queue("create", _ok_response(_read_body(), status_code=201))
        ctx.queue("status", _ok_response(_status_body("PROVISIONING")))
        api = ConnectorApi(ctx)

        connector = api.create(
            "MyDatagen", config=_create_config(), wait_for_running=False
        )

        assert connector.state is ConnectorState.PROVISIONING
        assert ctx.count("status") == 1
        no_sleep.assert_not_called()

    def test_already_running_returns_without_polling(self, no_sleep) -> None:
        ctx = FakeControlPlane()
        ctx.queue("create", _ok_response(_read_body(), status_code=201))
        ctx.queue("status", _ok_response(_status_body("RUNNING")))
        api = ConnectorApi(ctx)

        connector = api.create("MyDatagen", config=_create_config())

        assert connector.state is ConnectorState.RUNNING
        assert ctx.count("status") == 1
        no_sleep.assert_not_called()

    def test_generic_error_raises_operational_not_already_exists(self) -> None:
        ctx = FakeControlPlane()
        ctx.queue("create", _error_response(500))
        api = ConnectorApi(ctx)

        with pytest.raises(OperationalError) as exc:
            api.create("MyDatagen", config=_create_config())
        assert not isinstance(exc.value, ConnectorAlreadyExistsError)
        assert exc.value.http_status_code == 500

    def test_409_raises_already_exists(self) -> None:
        ctx = FakeControlPlane()
        ctx.queue("create", _error_response(409))
        api = ConnectorApi(ctx)

        with pytest.raises(ConnectorAlreadyExistsError) as exc:
            api.create("MyDatagen", config=_create_config())
        assert exc.value.connector_name == "MyDatagen"

    def test_failed_state_raises_with_trace(self, no_sleep) -> None:
        ctx = FakeControlPlane()
        ctx.queue("create", _ok_response(_read_body(), status_code=201))
        ctx.queue(
            "status",
            _ok_response(_status_body("FAILED", trace="java.lang.RuntimeException: nope")),
        )
        api = ConnectorApi(ctx)

        with pytest.raises(OperationalError, match="java.lang.RuntimeException: nope"):
            api.create("MyDatagen", config=_create_config())

    def test_timeout_raises(self, mocker) -> None:
        mocker.patch("confluent_sql.connectors.sleep_with_backoff", return_value=iter([]))
        ctx = FakeControlPlane()
        ctx.queue("create", _ok_response(_read_body(), status_code=201))
        ctx.queue("status", _ok_response(_status_body("PROVISIONING")))
        api = ConnectorApi(ctx)

        with pytest.raises(OperationalError, match="did not reach RUNNING"):
            api.create("MyDatagen", config=_create_config(), timeout=5)


class TestConnectorApiGet:
    """get() merges the config read and the /status read."""

    def test_merges_read_and_status(self) -> None:
        ctx = FakeControlPlane()
        ctx.queue("read", _ok_response(_read_body()))
        ctx.queue("status", _ok_response(_status_body("RUNNING")))
        api = ConnectorApi(ctx)

        connector = api.get("MyDatagen")

        assert connector.spec.connector_class == "DatagenSource"
        assert connector.state is ConnectorState.RUNNING

    def test_path_carries_env_cluster_and_name(self) -> None:
        ctx = FakeControlPlane()
        ctx.queue("read", _ok_response(_read_body()))
        ctx.queue("status", _ok_response(_status_body("RUNNING")))
        api = ConnectorApi(ctx)

        api.get("MyDatagen")

        read_url = next(u for m, u in ctx.requests if m == "GET" and not u.endswith("/status"))
        assert read_url == (
            "/connect/v1/environments/env-123/clusters/lkc-abc/connectors/MyDatagen"
        )

    def test_404_raises_not_found_without_status_call(self) -> None:
        ctx = FakeControlPlane()
        ctx.queue("read", _error_response(404))
        api = ConnectorApi(ctx)

        with pytest.raises(ConnectorNotFoundError) as exc:
            api.get("MyDatagen")
        assert exc.value.connector_name == "MyDatagen"
        assert ctx.count("status") == 0

    def test_generic_read_error_raises_operational(self) -> None:
        ctx = FakeControlPlane()
        ctx.queue("read", _error_response(500))
        api = ConnectorApi(ctx)

        with pytest.raises(OperationalError) as exc:
            api.get("MyDatagen")
        assert exc.value.http_status_code == 500

    def test_status_404_after_successful_read_raises_not_found(self) -> None:
        # A connector deleted between the base read and the /status read: status 404s.
        ctx = FakeControlPlane()
        ctx.queue("read", _ok_response(_read_body()))
        ctx.queue("status", _error_response(404))
        api = ConnectorApi(ctx)

        with pytest.raises(ConnectorNotFoundError) as exc:
            api.get("MyDatagen")
        assert exc.value.connector_name == "MyDatagen"

    def test_generic_status_error_raises_operational(self) -> None:
        ctx = FakeControlPlane()
        ctx.queue("read", _ok_response(_read_body()))
        ctx.queue("status", _error_response(500))
        api = ConnectorApi(ctx)

        with pytest.raises(OperationalError) as exc:
            api.get("MyDatagen")
        assert exc.value.http_status_code == 500


class TestConnectorApiDelete:
    """delete() DELETEs, then polls the base read until it 404s."""

    def test_blocks_until_gone(self, no_sleep) -> None:
        ctx = FakeControlPlane()
        ctx.queue("delete", _ok_response())
        ctx.queue("read", _error_response(404))
        api = ConnectorApi(ctx)

        assert api.delete("MyDatagen") is None
        assert ctx.count("read") == 1

    def test_404_raises_not_found(self) -> None:
        ctx = FakeControlPlane()
        ctx.queue("delete", _error_response(404))
        api = ConnectorApi(ctx)

        with pytest.raises(ConnectorNotFoundError) as exc:
            api.delete("MyDatagen")
        assert exc.value.connector_name == "MyDatagen"

    def test_generic_error_raises_operational(self) -> None:
        ctx = FakeControlPlane()
        ctx.queue("delete", _error_response(500))
        api = ConnectorApi(ctx)

        with pytest.raises(OperationalError) as exc:
            api.delete("MyDatagen")
        assert exc.value.http_status_code == 500

    def test_polls_through_a_still_present_read_until_gone(self, no_sleep) -> None:
        # The connector is still readable on the first poll, then 404s on the second.
        ctx = FakeControlPlane()
        ctx.queue("delete", _ok_response())
        ctx.queue("read", _ok_response(_read_body()), _error_response(404))
        api = ConnectorApi(ctx)

        api.delete("MyDatagen")

        assert ctx.count("read") == 2

    def test_wait_false_skips_polling(self, no_sleep) -> None:
        ctx = FakeControlPlane()
        ctx.queue("delete", _ok_response())
        api = ConnectorApi(ctx)

        api.delete("MyDatagen", wait_for_removal=False)

        assert ctx.count("read") == 0
        no_sleep.assert_not_called()

    def test_removal_timeout_raises(self, mocker) -> None:
        mocker.patch("confluent_sql.connectors.sleep_with_backoff", return_value=iter([]))
        ctx = FakeControlPlane()
        ctx.queue("delete", _ok_response())
        ctx.queue("read", _ok_response(_read_body()))
        ctx.queue("status", _ok_response(_status_body("RUNNING")))
        api = ConnectorApi(ctx)

        with pytest.raises(OperationalError, match="was not removed"):
            api.delete("MyDatagen", timeout=5)
