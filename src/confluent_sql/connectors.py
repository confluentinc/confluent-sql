"""Connector types, request builders, response parsers, and lifecycle orchestration.

These model the Connect v1 managed-connector API
(`/connect/v1/environments/{env}/clusters/{lkc}/connectors`), which runs a Confluent-managed
source or sink connector against the Kafka cluster backing a Flink database.

Input vs. output, by design:

- The only *request input* is the open-ended `config` map (`StrAnyDict`); there is no typed
  request model, since the config is connector-class-specific. `build_create_payload` is the sole
  request-side helper, validating and shaping that map.
- Everything else here is an *output type* -- `Connector`, `ConnectorSpec`, `ConnectorStatus`,
  `ConnectorState`, `TaskStatus`. Callers receive these from the API; they are not meant to be
  constructed by hand (hence the `from_response` classmethods, not public constructors).
- The one value that travels *both* directions is the `config` map itself: a caller passes it in
  with plaintext secrets (e.g. `kafka.api.secret`), and it comes back on `ConnectorSpec.config`
  with those same secrets redacted to `****` by the server. Same shape, different trust on each leg.

The module is split into two layers plus a seam:

- A pure, I/O-free layer -- the output models above, the `ConnectorState` enum, and
  `build_create_payload`. Mirrors the `tableflow.py` precedent and is unit-testable with zero mocks.
- An orchestration layer -- `ConnectorApi` -- holding the HTTP+poll choreography.
- `ControlPlaneContext`, a narrow protocol `ConnectorApi` depends on instead of `Connection`, so
  `connection` imports `connectors` and never the reverse.

The Connect v1 wire shape splits what Tableflow kept together: a connector's lifecycle *state*
(`connector.state`) is returned only by the `/status` sub-resource, never by the create response or
the base read. So building a complete `Connector` always merges a config read with a status read.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Protocol

import httpx

from .exceptions import (
    ConnectorAlreadyExistsError,
    ConnectorNotFoundError,
    InterfaceError,
    OperationalError,
)
from .polling import sleep_with_backoff
from .types import StrAnyDict

_REQUIRED_CONFIG_KEYS = ("connector.class", "kafka.api.key", "kafka.api.secret")
"""Config keys the Connect v1 create body marks required (besides `name`, which we inject)."""


def build_create_payload(*, name: str, config: StrAnyDict) -> StrAnyDict:
    """Assemble the `POST .../connectors` request body from the open-ended config map.

    `config` is the connector-class-specific map. Beyond its class- and source/sink-specific keys
    it must carry the keys the create body marks required: `connector.class` (the managed connector
    plugin) plus the Kafka keypair `kafka.api.key` / `kafka.api.secret` the connector uses to reach
    the cluster. These are validated here so an omission fails fast with a clear local error rather
    than an opaque server 400. The wire also requires `config.name` to equal the top-level connector
    name, so a missing `config.name` is filled in and a conflicting one is rejected rather than
    silently overridden. The caller's `config` is not mutated.

    Raises:
        InterfaceError: If any required key (`connector.class`, `kafka.api.key`, `kafka.api.secret`)
            is absent, or `config.name` is present and disagrees with `name`.
    """
    missing = [key for key in _REQUIRED_CONFIG_KEYS if key not in config]
    if missing:
        listed = ", ".join(f"'{key}'" for key in missing)
        raise InterfaceError(f"connector config is missing required key(s): {listed}")
    config_name = config.get("name")
    if config_name is not None and config_name != name:
        raise InterfaceError(
            f"connector config 'name' ('{config_name}') does not match connector name '{name}'"
        )
    return {"name": name, "config": {**config, "name": name}}


class ConnectorState(str, Enum):
    """Lifecycle state of a connector (`/status` `connector.state`).

    Output-only: appears on responses (`ConnectorStatus.state`, `Connector.state`) and drives the
    create-wait; it is never an input. Extensible -- an unrecognized server-side value parses to
    `UNKNOWN` rather than raising, so a future state doesn't break response parsing.
    """

    NONE = "NONE"
    PROVISIONING = "PROVISIONING"
    RUNNING = "RUNNING"
    DEGRADED = "DEGRADED"
    FAILED = "FAILED"
    PAUSED = "PAUSED"
    DELETED = "DELETED"
    UNKNOWN = "UNKNOWN"

    @classmethod
    def _missing_(cls, value: object) -> ConnectorState:
        return cls.UNKNOWN

    @property
    def is_terminal(self) -> bool:
        """True once a create-wait can stop: the connector is healthy (RUNNING) or broken (FAILED).

        PROVISIONING/NONE/DEGRADED are transient during provisioning; PAUSED/DELETED don't arise
        from a create. UNKNOWN is deliberately non-terminal -- a state we don't understand
        shouldn't end a wait; the caller's timeout governs that instead.
        """
        return self in (ConnectorState.RUNNING, ConnectorState.FAILED)


@dataclass
class TaskStatus:
    """Per-task status from the `/status` response `tasks` array."""

    id: int
    state: str
    worker_id: str | None
    msg: str | None

    @classmethod
    def from_response(cls, data: StrAnyDict) -> TaskStatus:
        return cls(
            id=data["id"],
            state=data["state"],
            worker_id=data.get("worker_id"),
            msg=data.get("msg"),
        )


@dataclass
class ConnectorSpec:
    """Parsed connector config read (`connect.v1.Connector`): config/tasks/type, never state.

    The full `config` map is retained raw (it carries connector-class-specific keys and redacted
    secrets); `connector_class` is the convenience read of `config['connector.class']`.
    """

    name: str
    connector_class: str | None
    type: str | None
    config: StrAnyDict
    tasks: list[StrAnyDict]
    raw: StrAnyDict = field(repr=False)

    @classmethod
    def from_response(cls, data: StrAnyDict) -> ConnectorSpec:
        try:
            config = data["config"]
            return cls(
                name=data["name"],
                connector_class=config.get("connector.class"),
                type=data.get("type"),
                config=config,
                tasks=data.get("tasks") or [],
                raw=data,
            )
        except KeyError as e:
            raise OperationalError(f"Error parsing connector response, missing {e}.") from e


@dataclass
class ConnectorStatus:
    """Parsed `/status` response -- the only source of the connector's lifecycle state."""

    state: ConnectorState
    worker_id: str | None
    trace: str | None
    tasks: list[TaskStatus]
    plugin_lifecycle: str | None
    raw: StrAnyDict = field(repr=False)

    @classmethod
    def from_response(cls, data: StrAnyDict) -> ConnectorStatus:
        try:
            connector = data["connector"]
            return cls(
                state=ConnectorState(connector.get("state")),
                worker_id=connector.get("worker_id"),
                trace=connector.get("trace") or None,
                tasks=[TaskStatus.from_response(t) for t in (data.get("tasks") or [])],
                plugin_lifecycle=data.get("plugin_lifecycle"),
                raw=data,
            )
        except KeyError as e:
            raise OperationalError(
                f"Error parsing connector status response, missing {e}."
            ) from e


@dataclass
class Connector:
    """A connector assembled from its config read and its `/status` read.

    Mirrors `TableflowTopic`: parsed `spec`/`status` and a `.state` convenience. The two halves
    come from different endpoints because the Connect v1 wire never returns state on the connector
    object itself. Holds no connection back-reference -- refresh via `Connection.get_connector`.
    """

    spec: ConnectorSpec
    status: ConnectorStatus

    @property
    def state(self) -> ConnectorState:
        """The connector's lifecycle state (convenience for `status.state`)."""
        return self.status.state

    @classmethod
    def from_response(cls, read_data: StrAnyDict, status_data: StrAnyDict) -> Connector:
        """Build a Connector from a config read body and a `/status` body."""
        return cls(
            spec=ConnectorSpec.from_response(read_data),
            status=ConnectorStatus.from_response(status_data),
        )


class ControlPlaneContext(Protocol):
    """The narrow surface `ConnectorApi` needs from its host connection.

    Depending on this protocol rather than on `Connection` is what keeps the import graph acyclic
    (`connection` imports `connectors`, never the reverse) and lets `ConnectorApi` be tested
    against a tiny fake. The host supplies the environment, resolves the Kafka cluster id, and
    issues authenticated control-plane requests; `ConnectorApi` owns everything connector-specific
    on top.
    """

    environment_id: str

    def resolve_kafka_cluster_id(self) -> str:
        """Return the `lkc-…` id of the Kafka cluster the connectors run against."""
        ...

    def controlplane_request(
        self, url: str, method: str = "GET", raise_for_status: bool = True, **kwargs: object
    ) -> httpx.Response:
        """Issue a request against the connector control-plane routes, authed for Connect."""
        ...


class ConnectorApi:
    """Create / read / delete choreography for managed connectors against the Connect v1 API.

    Holds the stateful HTTP + polling work behind `Connection`'s flat passthroughs. Blocks by
    default per the repo's `wait_for_<settled-state>` convention, polling via `sleep_with_backoff`.
    Because the wire returns lifecycle state only from the `/status` sub-resource, a complete
    `Connector` always pairs a config read with a status read.
    """

    def __init__(self, context: ControlPlaneContext) -> None:
        self._context = context

    def _connectors_path(self) -> str:
        """The base `/connect/v1/.../connectors` collection path for this connection's cluster."""
        env = self._context.environment_id
        cluster = self._context.resolve_kafka_cluster_id()
        return f"/connect/v1/environments/{env}/clusters/{cluster}/connectors"

    def create(
        self,
        name: str,
        *,
        config: StrAnyDict,
        wait_for_running: bool = True,
        timeout: float = 300,
    ) -> Connector:
        """Create a managed connector and, by default, block until it reaches RUNNING.

        `POST .../connectors`. The create response carries the config but no lifecycle state, so a
        single `/status` read always follows to populate it; with `wait_for_running` that read is
        then repeated until RUNNING (raising on FAILED).

        Raises:
            InterfaceError: If `config` is missing a required key (`connector.class`,
                `kafka.api.key`, `kafka.api.secret`) or its `name` disagrees with `name`.
            ProgrammingError: If the context can't authenticate the Connect routes (no Connect
                credential) or resolve the Kafka cluster id.
            ConnectorAlreadyExistsError: If a connector with this name exists (HTTP 409).
            OperationalError: On other API errors, on FAILED during a wait, or on wait timeout.
        """
        payload = build_create_payload(name=name, config=config)
        response = self._context.controlplane_request(
            self._connectors_path(), method="POST", json=payload, raise_for_status=False
        )
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 409:
                raise ConnectorAlreadyExistsError(
                    f"Connector '{name}' already exists", connector_name=name
                ) from e
            raise OperationalError(
                "Error creating connector", http_status_code=e.response.status_code
            ) from e

        connector = Connector(
            spec=ConnectorSpec.from_response(response.json()),
            status=self._fetch_status(name),
        )
        if wait_for_running:
            return self._wait_for_running(connector, timeout)
        return connector

    def get(self, name: str) -> Connector:
        """Read a connector's config and lifecycle state, merging the base read with `/status`.

        Raises:
            ProgrammingError: If the context can't authenticate the Connect routes (no Connect
                credential) or resolve the Kafka cluster id.
            ConnectorNotFoundError: If no connector with this name exists (HTTP 404).
            OperationalError: On other API errors.
        """
        spec = self._read_spec(name)
        return Connector(spec=spec, status=self._fetch_status(name))

    def delete(self, name: str, *, wait_for_removal: bool = True, timeout: float = 300) -> None:
        """Delete a connector and, by default, block until the base read 404s.

        Raises:
            ProgrammingError: If the context can't authenticate the Connect routes (no Connect
                credential) or resolve the Kafka cluster id.
            ConnectorNotFoundError: If no connector with this name exists (HTTP 404).
            OperationalError: On other API errors, or on wait timeout.
        """
        response = self._context.controlplane_request(
            f"{self._connectors_path()}/{name}", method="DELETE", raise_for_status=False
        )
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise ConnectorNotFoundError(
                    f"Connector '{name}' does not exist", connector_name=name
                ) from e
            raise OperationalError(
                "Error deleting connector", http_status_code=e.response.status_code
            ) from e

        if wait_for_removal:
            self._wait_for_removal(name, timeout)

    def _read_spec(self, name: str) -> ConnectorSpec:
        """`GET .../connectors/{name}` -> parsed spec; 404 -> ConnectorNotFoundError, other HTTP
        errors -> OperationalError."""
        response = self._context.controlplane_request(
            f"{self._connectors_path()}/{name}", raise_for_status=False
        )
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise ConnectorNotFoundError(
                    f"Connector '{name}' does not exist", connector_name=name
                ) from e
            raise OperationalError(
                "Error reading connector", http_status_code=e.response.status_code
            ) from e
        return ConnectorSpec.from_response(response.json())

    def _fetch_status(self, name: str) -> ConnectorStatus:
        """`GET .../connectors/{name}/status` -> parsed status; 404 -> ConnectorNotFoundError, other
        HTTP errors -> OperationalError."""
        response = self._context.controlplane_request(
            f"{self._connectors_path()}/{name}/status", raise_for_status=False
        )
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise ConnectorNotFoundError(
                    f"Connector '{name}' does not exist", connector_name=name
                ) from e
            raise OperationalError(
                "Error reading connector status", http_status_code=e.response.status_code
            ) from e
        return ConnectorStatus.from_response(response.json())

    def _wait_for_running(self, connector: Connector, timeout: float) -> Connector:
        """Poll `/status` until the connector reaches RUNNING, refreshing only its status.

        The spec is fixed once created, so each poll rebuilds the `Connector` from the original spec
        and the freshly-read status rather than re-reading the config.

        Raises:
            OperationalError: If the connector transitions to FAILED (surfacing `connector.trace`),
                or if RUNNING is not reached within timeout.
        """
        def settled(candidate: Connector) -> Connector | None:
            """Resolve a create-wait terminal state: return the connector at RUNNING, raise at
            FAILED, or None while still transient so the caller keeps polling."""
            if not candidate.state.is_terminal:
                return None
            if candidate.state is ConnectorState.FAILED:
                detail = candidate.status.trace or ""
                raise OperationalError(
                    f"Connector '{candidate.spec.name}' transitioned to FAILED: {detail}".strip()
                )
            return candidate

        if (resolved := settled(connector)) is not None:
            return resolved

        for _ in sleep_with_backoff(timeout):
            connector = Connector(
                spec=connector.spec, status=self._fetch_status(connector.spec.name)
            )
            if (resolved := settled(connector)) is not None:
                return resolved

        raise OperationalError(
            f"Connector '{connector.spec.name}' did not reach RUNNING within {timeout} seconds"
        )

    def _wait_for_removal(self, name: str, timeout: float) -> None:
        """Poll the base read until it 404s, confirming the connector is gone.

        Raises:
            OperationalError: If the connector is not removed within timeout.
        """
        try:
            self._read_spec(name)
        except ConnectorNotFoundError:
            return

        for _ in sleep_with_backoff(timeout):
            try:
                self._read_spec(name)
            except ConnectorNotFoundError:
                return

        raise OperationalError(f"Connector '{name}' was not removed within {timeout} seconds")
