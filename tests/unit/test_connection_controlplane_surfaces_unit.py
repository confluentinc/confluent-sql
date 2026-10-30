"""Unit tests for `Connection`'s control-plane surfaces: one shared `ServiceClient`, with each
request carrying its own surface's credential (`ControlPlaneSurface`, `Connection._auth_for`).

These drive a real `httpx.Client` over a recording transport rather than mocking the request
methods, since the wiring from surface to credential, retry and logging is what's under test.
"""

from __future__ import annotations

import base64
import logging
from typing import Any

import httpx
import pytest

from confluent_sql import InterfaceError, ProgrammingError, connect
from confluent_sql.connection import Connection, ControlPlaneSurface

pytestmark = pytest.mark.unit

KEYS = {
    ControlPlaneSurface.TABLEFLOW: ("tk", "ts"),
    ControlPlaneSurface.CONNECT: ("ck", "cs"),
    ControlPlaneSurface.ARTIFACTS: ("ak", "as"),
}
"""Distinct dedicated key pairs, so a request's Authorization header names its surface."""

SECRET_BODY = "X-Amz-Signature=deadbeef"


def _basic(key: str, secret: str) -> str:
    return "Basic " + base64.b64encode(f"{key}:{secret}".encode()).decode()


def _connect(**overrides: Any) -> Connection:
    kwargs: dict[str, Any] = {
        "flink_api_key": "fk",
        "flink_api_secret": "fs",
        "tableflow_api_key": KEYS[ControlPlaneSurface.TABLEFLOW][0],
        "tableflow_api_secret": KEYS[ControlPlaneSurface.TABLEFLOW][1],
        "connect_api_key": KEYS[ControlPlaneSurface.CONNECT][0],
        "connect_api_secret": KEYS[ControlPlaneSurface.CONNECT][1],
        "cloud_api_key": KEYS[ControlPlaneSurface.ARTIFACTS][0],
        "cloud_api_secret": KEYS[ControlPlaneSurface.ARTIFACTS][1],
        "environment_id": "env-1",
        "organization_id": "org-1",
        "cloud_provider": "aws",
        "cloud_region": "us-east-2",
        "database": "",
    }
    kwargs.update(overrides)
    return connect(**kwargs)


def _record_requests(conn: Connection, *statuses: int) -> list[httpx.Request]:
    """Route the control-plane client through a transport that records requests and answers with
    each given status in turn (200s afterwards)."""
    requests: list[httpx.Request] = []
    pending = list(statuses)

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(pending.pop(0) if pending else 200, request=request, text=SECRET_BODY)

    conn._controlplane._client = httpx.Client(
        base_url=conn._controlplane_endpoint, transport=httpx.MockTransport(handler)
    )
    return requests


SURFACE_CALLS = [
    (
        ControlPlaneSurface.TABLEFLOW,
        lambda conn, **kw: conn._controlplane_request(ControlPlaneSurface.TABLEFLOW, "/x", **kw),
    ),
    (
        ControlPlaneSurface.CONNECT,
        lambda conn, **kw: conn.connect_controlplane_request("/x", **kw),
    ),
    (
        ControlPlaneSurface.ARTIFACTS,
        lambda conn, **kw: conn.artifact_controlplane_request("/x", **kw),
    ),
]
SURFACE_IDS = ["tableflow", "connectors-api", "artifacts-api"]


class TestSurfaceCredentials:
    @pytest.mark.parametrize(("surface", "call"), SURFACE_CALLS, ids=SURFACE_IDS)
    @pytest.mark.parametrize("method", ["GET", "POST"])
    def test_request_carries_its_own_surfaces_credential(self, surface, call, method) -> None:
        conn = _connect()
        requests = _record_requests(conn)

        call(conn, method=method)

        assert requests[0].headers["Authorization"] == _basic(*KEYS[surface])

    def test_a_global_key_authenticates_every_surface(self) -> None:
        conn = _connect(global_api_key="gk", global_api_secret="gs")
        requests = _record_requests(conn)

        for _, call in SURFACE_CALLS:
            call(conn)

        assert {r.headers["Authorization"] for r in requests} == {_basic("gk", "gs")}
        assert len(requests) == len(SURFACE_CALLS)

    def test_all_surfaces_share_one_client(self) -> None:
        conn = _connect()
        requests = _record_requests(conn)

        for _, call in SURFACE_CALLS:
            call(conn)

        assert len(requests) == len(SURFACE_CALLS)
        assert len({str(r.url.host) for r in requests}) == 1


class TestSurfaceRetries:
    @pytest.mark.parametrize(("surface", "call"), SURFACE_CALLS, ids=SURFACE_IDS)
    def test_get_is_retried_and_keeps_its_credential(self, mocker, surface, call) -> None:
        mocker.patch("confluent_sql.retry.time.sleep")
        conn = _connect()
        requests = _record_requests(conn, 503)

        assert call(conn).status_code == 200

        assert len(requests) == 2
        assert {r.headers["Authorization"] for r in requests} == {_basic(*KEYS[surface])}

    @pytest.mark.parametrize(("surface", "call"), SURFACE_CALLS, ids=SURFACE_IDS)
    def test_mutation_is_not_retried(self, mocker, surface, call) -> None:
        mocker.patch("confluent_sql.retry.time.sleep")
        conn = _connect()
        requests = _record_requests(conn, 503)

        response = call(conn, method="POST", raise_for_status=False)

        assert response.status_code == 503
        assert len(requests) == 1


class TestSurfaceResponseLogging:
    @pytest.mark.parametrize(("surface", "call"), SURFACE_CALLS, ids=SURFACE_IDS)
    @pytest.mark.parametrize("method", ["GET", "POST"])
    def test_only_artifacts_withholds_the_response_body(
        self, caplog, surface, call, method
    ) -> None:
        conn = _connect()
        _record_requests(conn)

        with caplog.at_level(logging.DEBUG, logger="confluent_sql"):
            call(conn, method=method)

        assert (SECRET_BODY in caplog.text) is (surface is not ControlPlaneSurface.ARTIFACTS)


GLOBAL_ONLY_SURFACES = [ControlPlaneSurface.CMK, ControlPlaneSurface.ORGANIZATION]


class TestGlobalOnlySurfaces:
    """The CMK and organization lookups take the global key (or an oauth login) only; a dedicated
    key, however valid for its own surface, never authenticates them."""

    @pytest.mark.parametrize("surface", GLOBAL_ONLY_SURFACES)
    def test_global_key_authenticates(self, surface) -> None:
        conn = _connect(global_api_key="gk", global_api_secret="gs")
        requests = _record_requests(conn)

        conn._controlplane_request(surface, "/x")

        assert requests[0].headers["Authorization"] == _basic("gk", "gs")

    @pytest.mark.parametrize("surface", GLOBAL_ONLY_SURFACES)
    def test_dedicated_keys_alone_do_not_authenticate(self, surface) -> None:
        conn = _connect()  # tableflow/connect/cloud pairs, but no global key

        assert conn._auth_for(ControlPlaneSurface.TABLEFLOW) is not None
        with pytest.raises(ProgrammingError, match="global API key"):
            conn._auth_for(surface)


class TestMissingCredentials:
    @pytest.mark.parametrize("surface", list(ControlPlaneSurface))
    def test_names_the_surface_and_its_key_pair(self, surface) -> None:
        conn = _connect(
            tableflow_api_key="",
            tableflow_api_secret="",
            connect_api_key="",
            connect_api_secret="",
            cloud_api_key="",
            cloud_api_secret="",
        )

        with pytest.raises(ProgrammingError) as exc_info:
            conn._auth_for(surface)

        message = str(exc_info.value)
        assert surface.value.subject in message
        assert surface.value.key_pair in message
        assert "neither was supplied" in message

    def test_a_missing_credential_does_not_affect_other_surfaces(self) -> None:
        conn = _connect(connect_api_key="", connect_api_secret="")

        with pytest.raises(ProgrammingError, match="Connectors require"):
            conn._auth_for(ControlPlaneSurface.CONNECT)
        assert conn._auth_for(ControlPlaneSurface.ARTIFACTS) is not None

    @pytest.mark.parametrize("surface", list(ControlPlaneSurface))
    def test_closed_connection_wins_over_a_missing_credential(self, surface) -> None:
        conn = _connect(
            tableflow_api_key="",
            tableflow_api_secret="",
            connect_api_key="",
            connect_api_secret="",
            cloud_api_key="",
            cloud_api_secret="",
        )
        conn.close()

        with pytest.raises(InterfaceError, match="Connection is closed"):
            conn._auth_for(surface)
