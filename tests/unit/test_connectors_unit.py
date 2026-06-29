"""Unit tests for the network-free connector types, payload builder, and response parsers."""

from __future__ import annotations

import re

import pytest

from confluent_sql.connectors import (
    Connector,
    ConnectorSpec,
    ConnectorState,
    ConnectorStatus,
    TaskStatus,
    build_create_payload,
)
from confluent_sql.exceptions import InterfaceError, OperationalError

pytestmark = pytest.mark.unit


def _read_body() -> dict:
    """A `GET/POST .../connectors/{name}` body: config/tasks/type, never lifecycle state."""
    return {
        "name": "MyDatagen",
        "config": {
            "connector.class": "DatagenSource",
            "name": "MyDatagen",
            "kafka.api.key": "KEY",
            "kafka.api.secret": "****",
            "quickstart": "ORDERS",
        },
        "tasks": [{"connector": "MyDatagen", "task": 0}],
        "type": "source",
    }


def _status_body() -> dict:
    """A `GET .../connectors/{name}/status` body: the only place `connector.state` lives."""
    return {
        "name": "MyDatagen",
        "type": "source",
        "connector": {"state": "RUNNING", "worker_id": "w-1", "trace": ""},
        "tasks": [{"id": 0, "state": "RUNNING", "worker_id": "w-1"}],
        "plugin_lifecycle": "ACTIVE",
    }


class TestConnectorState:
    """The lifecycle enum is extensible and knows which states end a create-wait."""

    @pytest.mark.parametrize(
        ("state", "terminal"),
        [
            (ConnectorState.RUNNING, True),
            (ConnectorState.FAILED, True),
            (ConnectorState.PROVISIONING, False),
            (ConnectorState.NONE, False),
            (ConnectorState.DEGRADED, False),
            (ConnectorState.PAUSED, False),
            (ConnectorState.DELETED, False),
            (ConnectorState.UNKNOWN, False),
        ],
    )
    def test_is_terminal(self, state: ConnectorState, terminal: bool) -> None:
        assert state.is_terminal is terminal

    def test_unknown_value_maps_to_unknown(self) -> None:
        assert ConnectorState("SOMETHING_NEW") is ConnectorState.UNKNOWN

    def test_missing_value_maps_to_unknown(self) -> None:
        assert ConnectorState(None) is ConnectorState.UNKNOWN


def _valid_config() -> dict:
    """The minimal config the create payload requires: class plus the Kafka keypair."""
    return {
        "connector.class": "DatagenSource",
        "kafka.api.key": "K",
        "kafka.api.secret": "S",
    }


class TestBuildCreatePayload:
    """`build_create_payload` requires the spec-required config keys and reconciles the name."""

    def test_minimal_injects_name_into_config(self) -> None:
        payload = build_create_payload(name="MyDatagen", config=_valid_config())
        assert payload == {
            "name": "MyDatagen",
            "config": {
                "connector.class": "DatagenSource",
                "kafka.api.key": "K",
                "kafka.api.secret": "S",
                "name": "MyDatagen",
            },
        }

    def test_does_not_mutate_caller_config(self) -> None:
        config = _valid_config()
        build_create_payload(name="MyDatagen", config=config)
        assert "name" not in config

    @pytest.mark.parametrize("missing", ["connector.class", "kafka.api.key", "kafka.api.secret"])
    def test_missing_required_key_raises(self, missing: str) -> None:
        config = _valid_config()
        del config[missing]
        with pytest.raises(InterfaceError, match=re.escape(missing)):
            build_create_payload(name="MyDatagen", config=config)

    def test_name_mismatch_raises(self) -> None:
        with pytest.raises(InterfaceError, match="does not match connector name 'MyDatagen'"):
            build_create_payload(name="MyDatagen", config={**_valid_config(), "name": "Other"})

    def test_matching_name_kept(self) -> None:
        payload = build_create_payload(
            name="MyDatagen", config={**_valid_config(), "name": "MyDatagen"}
        )
        assert payload["config"]["name"] == "MyDatagen"


class TestConnectorSpecFromResponse:
    """`ConnectorSpec.from_response` parses the config/tasks/type read; no state present."""

    def test_parses_fields(self) -> None:
        spec = ConnectorSpec.from_response(_read_body())
        assert spec.name == "MyDatagen"
        assert spec.connector_class == "DatagenSource"
        assert spec.type == "source"
        assert spec.config["quickstart"] == "ORDERS"
        assert spec.tasks == [{"connector": "MyDatagen", "task": 0}]

    def test_connector_class_none_when_absent(self) -> None:
        body = _read_body()
        del body["config"]["connector.class"]
        assert ConnectorSpec.from_response(body).connector_class is None

    def test_missing_required_key_raises_operational(self) -> None:
        body = _read_body()
        del body["config"]
        with pytest.raises(OperationalError, match="config"):
            ConnectorSpec.from_response(body)


class TestTaskStatusFromResponse:
    """`TaskStatus.from_response` parses a single per-task status entry."""

    def test_parses_fields(self) -> None:
        task = TaskStatus.from_response(
            {"id": 3, "state": "FAILED", "worker_id": "w-9", "msg": "boom"}
        )
        assert (task.id, task.state, task.worker_id, task.msg) == (3, "FAILED", "w-9", "boom")

    def test_optional_msg_defaults_none(self) -> None:
        task = TaskStatus.from_response({"id": 0, "state": "RUNNING", "worker_id": "w-1"})
        assert task.msg is None


class TestConnectorStatusFromResponse:
    """`ConnectorStatus.from_response` parses the `/status` body, where state lives."""

    def test_parses_fields(self) -> None:
        status = ConnectorStatus.from_response(_status_body())
        assert status.state is ConnectorState.RUNNING
        assert status.worker_id == "w-1"
        assert status.trace is None
        assert status.plugin_lifecycle == "ACTIVE"
        assert len(status.tasks) == 1
        assert status.tasks[0].state == "RUNNING"

    def test_extensible_state_is_lenient(self) -> None:
        body = _status_body()
        body["connector"]["state"] = "SOMETHING_NEW"
        assert ConnectorStatus.from_response(body).state is ConnectorState.UNKNOWN

    def test_trace_retained_when_present(self) -> None:
        body = _status_body()
        body["connector"]["state"] = "FAILED"
        body["connector"]["trace"] = "java.lang.RuntimeException: nope"
        status = ConnectorStatus.from_response(body)
        assert status.state is ConnectorState.FAILED
        assert status.trace == "java.lang.RuntimeException: nope"

    def test_missing_connector_key_raises_operational(self) -> None:
        body = _status_body()
        del body["connector"]
        with pytest.raises(OperationalError, match="connector"):
            ConnectorStatus.from_response(body)


class TestConnectorFromResponse:
    """`Connector.from_response` merges the config read and the status read."""

    def test_merges_read_and_status(self) -> None:
        connector = Connector.from_response(_read_body(), _status_body())
        assert connector.spec.connector_class == "DatagenSource"
        assert connector.status.state is ConnectorState.RUNNING

    def test_state_convenience_reads_status(self) -> None:
        connector = Connector.from_response(_read_body(), _status_body())
        assert connector.state is ConnectorState.RUNNING

    def test_missing_key_raises_operational(self) -> None:
        bad_read = _read_body()
        del bad_read["name"]
        with pytest.raises(OperationalError, match="name"):
            Connector.from_response(bad_read, _status_body())
