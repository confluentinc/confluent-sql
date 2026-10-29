"""Unit tests for `ServiceClient`: the one place requests are sent, retried, and error-translated.

A GET is retried on transient transport errors (#137) and transient HTTP statuses (#140); every
other method is sent exactly once. All planes (Flink gateway, Tableflow/CMK/org, Connect,
Artifacts) share this class, so these tests are the single home for that policy.
"""

from __future__ import annotations

import logging
from typing import Any

import httpx
import pytest

from confluent_sql import InterfaceError, OperationalError
from confluent_sql.context import BACKGROUND
from confluent_sql.exceptions import (
    ReauthenticationReason,
    ReauthenticationRequired,
    RequestTimeoutError,
)
from confluent_sql.service_client import ServiceClient

pytestmark = pytest.mark.unit

URL = "/some/route"

TOTAL_ATTEMPTS = 4
"""One initial attempt plus `call_with_retries`' default of three retries."""

MUTATING_METHODS = ["POST", "PUT", "PATCH", "DELETE"]


class Server:
    """A scripted MockTransport: each request consumes the next outcome (a status code, or an
    exception to raise) and is recorded."""

    def __init__(self, *outcomes: int | Exception) -> None:
        self.outcomes = list(outcomes)
        self.requests: list[httpx.Request] = []

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return httpx.Response(outcome, request=request, text="body-text")

    @property
    def call_count(self) -> int:
        return len(self.requests)


def make_client(*outcomes: int | Exception, **kwargs: Any) -> tuple[ServiceClient, Server]:
    server = Server(*outcomes)
    client = ServiceClient(
        lambda: httpx.Client(
            base_url="https://x.invalid", transport=httpx.MockTransport(server.handle)
        ),
        **kwargs,
    )
    return client, server


@pytest.fixture(autouse=True)
def no_retry_sleep(mocker) -> None:
    mocker.patch("confluent_sql.retry.time.sleep")


class TestGetRetries:
    def test_retries_transport_error_then_succeeds(self) -> None:
        client, server = make_client(httpx.ReadError("reset"), 200)

        assert client.request(URL).status_code == 200
        assert server.call_count == 2

    @pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
    def test_retries_transient_status_then_succeeds(self, status: int) -> None:
        client, server = make_client(status, 200)

        assert client.request(URL).status_code == 200
        assert server.call_count == 2

    def test_exhausted_transport_retries_surface_as_operational_error(self) -> None:
        original = httpx.ReadError("reset")
        client, server = make_client(*[original] * TOTAL_ATTEMPTS)

        with pytest.raises(OperationalError) as exc_info:
            client.request(URL)

        assert exc_info.value.__cause__ is original
        assert "ReadError" in str(exc_info.value)
        assert exc_info.value.http_status_code is None
        assert server.call_count == TOTAL_ATTEMPTS

    def test_exhausted_status_retries_raise_with_status_code(self) -> None:
        client, server = make_client(*[503] * TOTAL_ATTEMPTS)

        with pytest.raises(OperationalError) as exc_info:
            client.request(URL)

        assert exc_info.value.http_status_code == 503
        assert server.call_count == TOTAL_ATTEMPTS

    def test_exhausted_status_retries_return_final_response_when_not_raising(self) -> None:
        """Callers that map specific codes themselves (raise_for_status=False) still get the final
        response back after the retry budget is spent."""
        client, server = make_client(*[503] * TOTAL_ATTEMPTS)

        assert client.request(URL, raise_for_status=False).status_code == 503
        assert server.call_count == TOTAL_ATTEMPTS

    def test_transport_and_status_failures_share_one_retry_budget(self) -> None:
        client, server = make_client(httpx.ReadError("reset"), 503, httpx.ReadError("reset"), 200)

        assert client.request(URL).status_code == 200
        assert server.call_count == TOTAL_ATTEMPTS

    @pytest.mark.parametrize("status", [400, 401, 404, 409])
    def test_non_retryable_status_is_not_retried(self, status: int) -> None:
        client, server = make_client(status)

        assert client.request(URL, raise_for_status=False).status_code == status
        assert server.call_count == 1

    def test_non_retryable_status_raises_with_status_code_by_default(self) -> None:
        client, server = make_client(404)

        with pytest.raises(OperationalError) as exc_info:
            client.request(URL)

        assert exc_info.value.http_status_code == 404
        assert server.call_count == 1

    def test_timeout_is_not_retried(self) -> None:
        """Timeouts stay out of the retry layer on purpose; wait loops tolerate them (#236)."""
        client, server = make_client(httpx.ReadTimeout("slow"))

        with pytest.raises(RequestTimeoutError):
            client.request(URL)

        assert server.call_count == 1

    def test_method_match_is_case_insensitive(self) -> None:
        client, server = make_client(503, 200)

        assert client.request(URL, method="get").status_code == 200
        assert server.call_count == 2

    def test_kwargs_reach_the_httpx_request(self) -> None:
        client, server = make_client(200)

        client.request(URL, params={"a": "1"}, headers={"X-Test": "yes"})

        sent = server.requests[0]
        assert sent.url.query == b"a=1"
        assert sent.headers["X-Test"] == "yes"


class TestErrorTranslation:
    @pytest.mark.parametrize("method", ["GET", "POST"])
    @pytest.mark.parametrize(
        ("error_cls", "expect_timeout_error"),
        [
            (httpx.ReadTimeout, True),
            (httpx.ConnectTimeout, True),
            (httpx.PoolTimeout, True),
            (httpx.ConnectError, False),
            (httpx.ReadError, False),
            (httpx.RemoteProtocolError, False),
        ],
    )
    def test_only_timeouts_become_request_timeout_error(
        self, method: str, error_cls: type[httpx.RequestError], expect_timeout_error: bool
    ) -> None:
        """httpx timeouts surface as the OperationalError subclass RequestTimeoutError (#236) so
        polling loops can tell 'one slow response' from a hard failure; every other transport
        error is a plain OperationalError, on retried and non-retried methods alike."""
        client, _ = make_client(*[error_cls("boom")] * TOTAL_ATTEMPTS)

        with pytest.raises(OperationalError) as exc_info:
            client.request(URL, method=method)

        assert isinstance(exc_info.value, RequestTimeoutError) is expect_timeout_error
        assert error_cls.__name__ in str(exc_info.value)


class TestMutationsAreNotRetried:
    @pytest.mark.parametrize("method", MUTATING_METHODS)
    def test_transport_error_is_sent_once(self, method: str) -> None:
        original = httpx.ReadError("reset")
        client, server = make_client(original)

        with pytest.raises(OperationalError) as exc_info:
            client.request(URL, method=method)

        assert exc_info.value.__cause__ is original
        assert server.call_count == 1

    @pytest.mark.parametrize("method", MUTATING_METHODS)
    def test_retryable_status_is_sent_once(self, method: str) -> None:
        client, server = make_client(503)

        with pytest.raises(OperationalError) as exc_info:
            client.request(URL, method=method)

        assert exc_info.value.http_status_code == 503
        assert server.call_count == 1

    def test_timeout_surfaces_as_request_timeout_error(self) -> None:
        client, server = make_client(httpx.ConnectTimeout("slow"))

        with pytest.raises(RequestTimeoutError):
            client.request(URL, method="POST")

        assert server.call_count == 1


class TestSendPolicy:
    """The policy wraps every individual HTTP call, retried attempts included."""

    def test_policy_wraps_each_attempt(self) -> None:
        calls = []

        def policy(do_request):
            calls.append(1)
            return do_request()

        client, server = make_client(httpx.ReadError("reset"), 200, send_policy=policy)

        client.request(URL)

        assert len(calls) == server.call_count == 2

    def test_reauthentication_required_is_not_swallowed_or_retried(self) -> None:
        def policy(do_request):
            raise ReauthenticationRequired(
                "session expired", ReauthenticationReason.REFRESH_REJECTED
            )

        client, server = make_client(200, send_policy=policy)

        with pytest.raises(ReauthenticationRequired):
            client.request(URL)

        assert server.call_count == 0


class TestLifecycle:
    def test_httpx_client_is_built_lazily_and_once(self) -> None:
        built = []

        def build() -> httpx.Client:
            built.append(1)
            return httpx.Client()

        client = ServiceClient(build)
        assert built == []

        assert client.httpx_client is client.httpx_client
        assert built == [1]
        client.close()

    def test_build_errors_propagate_and_are_retried_on_next_use(self) -> None:
        attempts = []

        def build() -> httpx.Client:
            attempts.append(1)
            raise RuntimeError("no credential")

        client = ServiceClient(build)

        for _ in range(2):
            with pytest.raises(RuntimeError, match="no credential"):
                client.request(URL)
        assert len(attempts) == 2

    def test_close_closes_a_built_client(self) -> None:
        client, _ = make_client(200)
        httpx_client = client.httpx_client

        client.close()

        assert httpx_client.is_closed

    def test_close_without_ever_building_is_fine(self) -> None:
        built = []
        client = ServiceClient(lambda: built.append(1) or httpx.Client())

        client.close()

        assert built == []

    def test_use_after_close_raises_interface_error_without_building(self) -> None:
        built = []
        client = ServiceClient(lambda: built.append(1) or httpx.Client())
        client.close()

        with pytest.raises(InterfaceError, match="closed"):
            client.request(URL)
        with pytest.raises(InterfaceError, match="closed"):
            _ = client.httpx_client
        assert built == []

    def test_set_user_agent_updates_a_built_client_only(self) -> None:
        client, _ = make_client(200)

        client.set_user_agent("early/1")  # not built yet: a no-op, the factory supplies it
        assert client._client is None

        client.httpx_client  # noqa: B018 -- force the build
        client.set_user_agent("late/2")
        assert client.httpx_client.headers["User-Agent"] == "late/2"


class TestResponseLogging:
    @pytest.mark.parametrize(
        ("log_body", "expect_body"), [(True, True), (False, False)], ids=["logged", "not-logged"]
    )
    @pytest.mark.parametrize("method", ["GET", "POST"])
    def test_log_body_controls_debug_logging_per_request(
        self, caplog, method: str, log_body: bool, expect_body: bool
    ) -> None:
        client, _ = make_client(200)

        with caplog.at_level(logging.DEBUG, logger="confluent_sql.service_client"):
            client.request(URL, method=method, log_body=log_body)

        assert ("body-text" in caplog.text) is expect_body

    def test_body_is_logged_by_default(self, caplog) -> None:
        client, _ = make_client(200)

        with caplog.at_level(logging.DEBUG, logger="confluent_sql.service_client"):
            client.request(URL)

        assert "body-text" in caplog.text

    def test_log_body_is_not_forwarded_to_httpx(self) -> None:
        client, server = make_client(200)

        client.request(URL, log_body=False)

        assert server.call_count == 1  # httpx would have raised a TypeError on an unknown kwarg


class TestPerRequestAuth:
    @pytest.mark.parametrize("method", ["GET", "POST"])
    def test_auth_is_applied_to_the_request(self, method: str) -> None:
        """One auth-less client serves several credentials: each request names its own."""
        client, server = make_client(200)

        client.request(URL, method=method, auth=httpx.BasicAuth("key", "secret"))

        assert server.requests[0].headers["Authorization"].startswith("Basic ")

    def test_auth_is_reapplied_on_each_retried_attempt(self) -> None:
        client, server = make_client(503, 200)

        client.request(URL, auth=httpx.BasicAuth("key", "secret"))

        assert all("Authorization" in r.headers for r in server.requests)
        assert server.call_count == 2


class TestGetRetriesStopAtTheCtxDeadline:
    """A retried GET under a `ctx` keeps to its deadline end to end: attempts are clamped (see
    below) and the retry loop itself stops when the deadline cannot outlast the next backoff."""

    def test_a_transport_error_is_not_retried_past_an_expired_ctx(self) -> None:
        client, server = make_client(httpx.ReadError("reset"), httpx.ReadError("reset"), 200)

        with pytest.raises(OperationalError):
            client.request(URL, ctx=BACKGROUND.with_timeout(-1.0))

        assert server.call_count == 1

    def test_a_retryable_status_is_not_retried_past_an_expired_ctx(self) -> None:
        client, server = make_client(503, 200)

        with pytest.raises(OperationalError) as exc_info:
            client.request(URL, ctx=BACKGROUND.with_timeout(-1.0))

        assert exc_info.value.http_status_code == 503
        assert server.call_count == 1

    def test_a_generous_ctx_still_retries_to_success(self) -> None:
        client, server = make_client(httpx.ReadError("reset"), 503, 200)

        assert client.request(URL, ctx=BACKGROUND.with_timeout(600.0)).status_code == 200
        assert server.call_count == 3


class TestPerAttemptTimeoutClamp:
    """Given a `ctx` with a deadline, no single attempt may run past it: each request's httpx
    timeouts are clamped to the time left (they are never raised)."""

    CLIENT_TIMEOUT = 10.0

    def _client(self) -> tuple[ServiceClient, Server]:
        server = Server(*[200] * 4)
        client = ServiceClient(
            lambda: httpx.Client(
                base_url="https://x.invalid",
                timeout=self.CLIENT_TIMEOUT,
                transport=httpx.MockTransport(server.handle),
            )
        )
        return client, server

    @staticmethod
    def _timeouts(server: Server) -> dict[str, float | None]:
        return server.requests[-1].extensions["timeout"]

    def _send_with_budget(self, budget: float, **request_kwargs: Any) -> Server:
        client, server = self._client()
        client.request(URL, ctx=BACKGROUND.with_timeout(budget), **request_kwargs)
        return server

    def test_unclamped_when_no_ctx_is_given(self) -> None:
        client, server = self._client()
        client.request(URL)

        assert set(self._timeouts(server).values()) == {self.CLIENT_TIMEOUT}

    def test_unclamped_under_the_background_ctx(self) -> None:
        client, server = self._client()
        client.request(URL, ctx=BACKGROUND)

        assert set(self._timeouts(server).values()) == {self.CLIENT_TIMEOUT}

    @pytest.mark.parametrize("method", ["GET", "POST"])
    def test_clamped_to_the_remaining_budget(self, method: str) -> None:
        server = self._send_with_budget(2.0, method=method)

        for value in self._timeouts(server).values():
            assert value is not None
            assert 0 < value <= 2.0

    def test_a_generous_budget_does_not_raise_the_timeout(self) -> None:
        server = self._send_with_budget(600.0)

        assert set(self._timeouts(server).values()) == {self.CLIENT_TIMEOUT}

    def test_an_expired_budget_still_sends_with_a_tiny_positive_timeout(self) -> None:
        client, server = self._client()
        client.request(URL, ctx=BACKGROUND.with_timeout(-5.0))

        for value in self._timeouts(server).values():
            assert value is not None
            assert 0 < value < 0.01

    @pytest.mark.parametrize(
        ("budget", "expected_max"),
        [(600.0, 3.0), (1.0, 1.0)],
        ids=["caller-tighter", "ctx-tighter"],
    )
    def test_an_explicit_per_request_timeout_is_clamped_too(
        self, budget: float, expected_max: float
    ) -> None:
        server = self._send_with_budget(budget, timeout=3.0)

        for value in self._timeouts(server).values():
            assert value is not None
            assert 0 < value <= expected_max
