"""Unit tests for the interactive-OAuth loopback callback server.

Driven end-to-end by real `httpx` GETs against a real listener on loopback, rather than by
synthesizing `BaseHTTPRequestHandler` internals: the whole point of this class is that a browser
redirected by the auth service can reach it, so the socket is the interesting part and mocking it
away would test nothing.

Every server here binds port **0** (an ephemeral port the OS picks), never the real 26640 from
`CCloudOAuthConfig`. A developer running a live mcp-confluent login legitimately holds 26640, and
so does a parallel run of this same suite; binding it would make this file flaky for reasons
having nothing to do with the code under test. `TestLifecycle.test_port_already_bound_...` is the
one case that needs a known-taken port, and it makes one for itself.
"""

from __future__ import annotations

import contextlib
import logging
import socket
import threading
from collections.abc import Iterator
from contextlib import contextmanager

import httpx
import pytest

from confluent_sql.exceptions import OAuthLoginError, OAuthLoginFailure, ProgrammingError
from confluent_sql.oauth.callback_server import CallbackServer
from confluent_sql.oauth.config import CCloudOAuthConfig

pytestmark = pytest.mark.unit

EXPECTED_STATE = "the-state-this-login-generated"
AUTH_CODE = "the-authorization-code"
CALLBACK_PATH = "/gateway/v1/callback-local-mcp-docs"
LOGGER_NAME = "confluent_sql.oauth.callback_server"

BRIEF_TIMEOUT = 5.0
"""For waits that are *expected* to succeed. Generous enough that a loaded CI box still wins the
race, but far below the production 120s default, so a genuinely wedged wait fails fast."""

PENDING_TIMEOUT = 0.1
"""For waits that are expected to time out, where the assertion is that the login is still
pending. Nothing is being raced -- we only need long enough to prove the event was never set."""


def _config(port: int = 0, host: str = "127.0.0.1") -> CCloudOAuthConfig:
    return CCloudOAuthConfig(
        auth_service_domain="login.confluent.io",
        api_host="https://confluent.cloud",
        client_id="test-client-id",
        callback_host=host,
        callback_port=port,
        callback_path=CALLBACK_PATH,
    )


@contextmanager
def _running_server(port: int = 0, state: str = EXPECTED_STATE) -> Iterator[CallbackServer]:
    with CallbackServer(_config(port), state) as server:
        yield server


def _get(server: CallbackServer, path: str = CALLBACK_PATH, **params: str) -> httpx.Response:
    """Hit the live listener the way the browser would."""
    return httpx.get(f"http://127.0.0.1:{server.port}{path}", params=params, timeout=BRIEF_TIMEOUT)


def _assert_login_still_pending(server: CallbackServer) -> None:
    """A rejected request must leave the login waiting, not resolve it: on loopback anyone can
    send a stray (or hostile) GET while the genuine auth-service redirect is still in flight, and
    that must not be able to cancel the user's login."""
    with pytest.raises(OAuthLoginError) as exc_info:
        server.wait_for_code(timeout=PENDING_TIMEOUT)
    assert exc_info.value.reason is OAuthLoginFailure.TIMED_OUT


class TestSuccessfulRedirect:
    def test_captures_code_and_serves_a_success_page(self):
        with _running_server() as server:
            response = _get(server, state=EXPECTED_STATE, code=AUTH_CODE)

            assert response.status_code == 200
            assert response.headers["content-type"] == "text/html; charset=utf-8"
            assert "successful" in response.text.lower()
            assert server.wait_for_code(timeout=BRIEF_TIMEOUT) == AUTH_CODE

    def test_success_page_references_no_external_assets(self):
        """The page has to render on a browser that may have no reach past loopback -- and must
        not phone anywhere from a tab whose URL bar holds an authorization code -- so everything
        it needs is inline."""
        with _running_server() as server:
            response = _get(server, state=EXPECTED_STATE, code=AUTH_CODE)

        assert "src=" not in response.text
        assert "href=" not in response.text
        assert "@import" not in response.text

    def test_the_code_can_be_read_repeatedly(self):
        """wait_for_code is not a one-shot consume: #153's login() reads the code once, but a
        second *sequential* read (a retry path, a test) must see the same value rather than hang.
        Concurrent readers are covered separately, by TestConcurrentWaiters."""
        with _running_server() as server:
            _get(server, state=EXPECTED_STATE, code=AUTH_CODE)

            assert server.wait_for_code(timeout=BRIEF_TIMEOUT) == AUTH_CODE
            assert server.wait_for_code(timeout=BRIEF_TIMEOUT) == AUTH_CODE

    def test_first_code_wins_when_the_success_page_is_reloaded(self):
        """Reloading the success page re-delivers the redirect. The captured code must not be
        replaced -- the first one is the one the PKCE verifier was minted alongside."""
        with _running_server() as server:
            _get(server, state=EXPECTED_STATE, code=AUTH_CODE)
            reload_response = _get(server, state=EXPECTED_STATE, code="a-later-different-code")

            assert reload_response.status_code == 200
            assert server.wait_for_code(timeout=BRIEF_TIMEOUT) == AUTH_CODE


class TestRejectedRequests:
    def test_state_mismatch_is_400_and_does_not_resolve_the_login(self):
        with _running_server() as server:
            response = _get(server, state="not-the-expected-state", code=AUTH_CODE)

            assert response.status_code == 400
            assert "state" in response.text.lower()
            _assert_login_still_pending(server)

    def test_missing_state_is_400_and_does_not_resolve_the_login(self):
        with _running_server() as server:
            response = _get(server, code=AUTH_CODE)

            assert response.status_code == 400
            _assert_login_still_pending(server)

    def test_missing_code_is_400_and_does_not_resolve_the_login(self):
        with _running_server() as server:
            response = _get(server, state=EXPECTED_STATE)

            assert response.status_code == 400
            assert "code" in response.text.lower()
            _assert_login_still_pending(server)

    def test_unknown_path_is_404_and_does_not_resolve_the_login(self):
        with _running_server() as server:
            response = _get(
                server, path="/not-the-callback-path", state=EXPECTED_STATE, code=AUTH_CODE
            )

            assert response.status_code == 404
            _assert_login_still_pending(server)


class TestAuthServiceReportedError:
    def test_error_param_fails_the_login(self):
        with _running_server() as server:
            response = _get(server, state=EXPECTED_STATE, error="access_denied")

            assert response.status_code == 400
            assert "access_denied" in response.text
            with pytest.raises(OAuthLoginError) as exc_info:
                server.wait_for_code(timeout=BRIEF_TIMEOUT)

        assert exc_info.value.reason is OAuthLoginFailure.AUTHORIZATION_DENIED
        assert "access_denied" in str(exc_info.value)

    def test_error_description_is_surfaced_when_present(self):
        description = "User did not authorize the request"
        with _running_server() as server:
            _get(
                server,
                state=EXPECTED_STATE,
                error="access_denied",
                error_description=description,
            )

            with pytest.raises(OAuthLoginError) as exc_info:
                server.wait_for_code(timeout=BRIEF_TIMEOUT)

        assert description in str(exc_info.value)

    def test_error_with_wrong_state_does_not_fail_the_login(self):
        """State is validated before the error param, so an unsolicited error redirect can't
        abort a login it doesn't belong to."""
        with _running_server() as server:
            response = _get(server, state="not-the-expected-state", error="access_denied")

            assert response.status_code == 400
            _assert_login_still_pending(server)

    def test_error_text_is_html_escaped_in_the_page(self):
        """The error value lands in an HTML page, so it gets escaped -- loopback or not, we don't
        reflect attacker-chosen markup into a browser."""
        with _running_server() as server:
            response = _get(server, state=EXPECTED_STATE, error="<script>alert('x')</script>")

            assert "<script>" not in response.text
            assert "&lt;script&gt;" in response.text


class TestTimeout:
    def test_no_redirect_at_all_times_out(self):
        with _running_server() as server, pytest.raises(OAuthLoginError) as exc_info:
            server.wait_for_code(timeout=PENDING_TIMEOUT)

        assert exc_info.value.reason is OAuthLoginFailure.TIMED_OUT
        assert str(PENDING_TIMEOUT) in str(exc_info.value)


class TestLifecycle:
    def test_binds_loopback_only(self):
        with _running_server() as server:
            assert server.host == "127.0.0.1"
            assert server.port != 0

    def test_port_already_bound_raises_port_in_use(self):
        """The callback port is baked into the auth service client's whitelisted redirect_uri, so
        there is no fallback port to try -- a collision has to surface as its own diagnosable
        reason."""
        with _running_server() as first:
            second = CallbackServer(_config(first.port), EXPECTED_STATE)
            with pytest.raises(OAuthLoginError) as exc_info:
                second.start()

            assert exc_info.value.reason is OAuthLoginFailure.PORT_IN_USE
            assert str(first.port) in str(exc_info.value)

    def test_port_is_released_once_stopped(self):
        with _running_server() as server:
            port = server.port

        # Re-binding the very same port proves the listener is gone rather than lingering to
        # collide with the next login attempt.
        with _running_server(port=port) as reborn:
            assert reborn.port == port

    def test_listener_is_unreachable_once_stopped(self):
        with _running_server() as server:
            port = server.port

        with pytest.raises(httpx.ConnectError):
            httpx.get(f"http://127.0.0.1:{port}{CALLBACK_PATH}", timeout=BRIEF_TIMEOUT)

    def test_stop_is_idempotent(self):
        server = CallbackServer(_config(), EXPECTED_STATE)
        server.start()
        server.stop()
        server.stop()

    def test_stop_without_start_is_harmless(self):
        CallbackServer(_config(), EXPECTED_STATE).stop()

    def test_starting_twice_is_a_programming_error(self):
        with _running_server() as server, pytest.raises(ProgrammingError):
            server.start()

    def test_port_before_start_is_a_programming_error(self):
        server = CallbackServer(_config(), EXPECTED_STATE)
        with pytest.raises(ProgrammingError):
            _ = server.port

    def test_wait_for_code_before_start_is_a_programming_error(self):
        server = CallbackServer(_config(), EXPECTED_STATE)
        with pytest.raises(ProgrammingError):
            server.wait_for_code(timeout=PENDING_TIMEOUT)


class TestWaitingAfterStop:
    """Waiting once the listener is gone. The mirror image of waiting before `start()`: in both
    states no redirect can arrive, so blocking out the full timeout only delays a verdict already
    known -- unless an outcome was captured before the teardown, which stays readable."""

    def test_waiting_after_stop_without_a_captured_code_fails_fast(self):
        server = CallbackServer(_config(), EXPECTED_STATE)
        server.start()
        server.stop()

        with pytest.raises(ProgrammingError):
            server.wait_for_code(timeout=BRIEF_TIMEOUT)

    def test_waiting_after_stop_still_returns_an_already_captured_code(self):
        """The fail-fast must not shadow a completed login: `stop()` normally runs right after the
        code arrives, and the code stays readable afterward."""
        with _running_server() as server:
            _get(server, state=EXPECTED_STATE, code=AUTH_CODE)
            assert server.wait_for_code(timeout=BRIEF_TIMEOUT) == AUTH_CODE

        assert server.wait_for_code(timeout=BRIEF_TIMEOUT) == AUTH_CODE

    def test_waiting_after_stop_still_re_raises_a_captured_failure(self):
        """Same ordering for the failure half: a denied authorization captured before teardown
        outranks the generic post-stop guard, so the caller still learns *why* it failed."""
        with _running_server() as server:
            _get(server, state=EXPECTED_STATE, error="access_denied")
            with pytest.raises(OAuthLoginError):
                server.wait_for_code(timeout=BRIEF_TIMEOUT)

        with pytest.raises(OAuthLoginError) as exc_info:
            server.wait_for_code(timeout=BRIEF_TIMEOUT)
        assert exc_info.value.reason is OAuthLoginFailure.AUTHORIZATION_DENIED


class TestOnlyLoopbackIsBound:
    """`callback_host` is public config, so "loopback only" has to be enforced, not just
    documented: this listener takes a live authorization code over plain HTTP."""

    @pytest.mark.parametrize(
        "host",
        [
            "0.0.0.0",  # every interface -- the case worth failing loudly
            "192.168.1.10",  # a specific routable address
            "::",  # the IPv6 wildcard
            "::1",  # loopback, but unbindable: ThreadingHTTPServer is AF_INET
            "localhost",  # a name, and what it resolves to is not ours to decide
            "",  # empty means "all interfaces" to bind(2)
        ],
    )
    def test_non_loopback_host_is_refused_before_binding(self, host):
        server = CallbackServer(_config(host=host), EXPECTED_STATE)
        try:
            with pytest.raises(ProgrammingError):
                server.start()
        finally:
            # Cleans up in case the guard is missing and the bind actually succeeded.
            server.stop()

    def test_the_loopback_literal_is_accepted(self):
        with CallbackServer(_config(host="127.0.0.1"), EXPECTED_STATE) as server:
            assert server.host == "127.0.0.1"


_Outcome = tuple[str | None, BaseException | None]
"""One waiter's result: either the code it received, or the exception it caught."""


@contextmanager
def _blocked_waiters(server: CallbackServer, count: int) -> Iterator[list[_Outcome]]:
    """Park `count` threads inside `wait_for_code`, then yield the list they report into.

    The barrier releases every thread at the call site before the body runs, and the body is what
    triggers the outcome. Nothing can settle the login until then, so the `outcomes == []` check
    below is a real assertion rather than a formality: a waiter that has already returned would be
    a lost wakeup, not a slow test.

    Threads are joined on exit, so the caller's assertions run against a complete list.
    """
    outcomes: list[_Outcome] = []
    reporting = threading.Lock()
    at_the_call_site = threading.Barrier(count + 1)

    def wait() -> None:
        at_the_call_site.wait(timeout=BRIEF_TIMEOUT)
        try:
            code = server.wait_for_code(timeout=BRIEF_TIMEOUT)
        except BaseException as e:  # noqa: BLE001 -- reported to the test, not swallowed
            with reporting:
                outcomes.append((None, e))
        else:
            with reporting:
                outcomes.append((code, None))

    threads = [threading.Thread(target=wait, name=f"waiter-{n}", daemon=True) for n in range(count)]
    for thread in threads:
        thread.start()
    at_the_call_site.wait(timeout=BRIEF_TIMEOUT)
    assert outcomes == []
    try:
        yield outcomes
    finally:
        for thread in threads:
            thread.join(timeout=BRIEF_TIMEOUT)


class TestConcurrentWaiters:
    """Several threads blocked in `wait_for_code` at the moment the redirect lands.

    Not how #153 drives this class -- one login thread does the waiting, and the N-waiter fan-out
    belongs to #154's holder -- but `wait_for_code` blocks on an `Event`, which permits any number
    of waiters, so the behavior is pinned rather than left to chance.
    """

    WAITER_COUNT = 4

    def test_every_blocked_waiter_receives_the_same_code(self):
        with _running_server() as server, _blocked_waiters(server, self.WAITER_COUNT) as outcomes:
            _get(server, state=EXPECTED_STATE, code=AUTH_CODE)

        assert [code for code, _ in outcomes] == [AUTH_CODE] * self.WAITER_COUNT
        assert [error for _, error in outcomes] == [None] * self.WAITER_COUNT

    def test_every_blocked_waiter_gets_its_own_copy_of_the_failure(self):
        with _running_server() as server, _blocked_waiters(server, self.WAITER_COUNT) as outcomes:
            _get(server, state=EXPECTED_STATE, error="access_denied")

        errors = [error for _, error in outcomes]
        assert len(errors) == self.WAITER_COUNT
        assert {type(error) for error in errors} == {OAuthLoginError}
        assert {error.reason for error in errors} == {  # type: ignore[union-attr]
            OAuthLoginFailure.AUTHORIZATION_DENIED
        }
        # Distinct instances: sharing one would mean each raise mutating the traceback the other
        # waiters are holding.
        assert len({id(error) for error in errors}) == self.WAITER_COUNT


def _read_failure(server: CallbackServer) -> OAuthLoginError:
    with pytest.raises(OAuthLoginError) as exc_info:
        server.wait_for_code(timeout=BRIEF_TIMEOUT)
    return exc_info.value


def _traceback_depth(error: BaseException) -> int:
    depth, frame = 0, error.__traceback__
    while frame is not None:
        depth += 1
        frame = frame.tb_next
    return depth


def _read_failure_traceback_depth(server: CallbackServer) -> int:
    """Read a recorded failure and measure its traceback depth *at once*.

    Measuring later would be useless: if the same object is raised repeatedly, every reference to
    it -- including one captured earlier -- reports whatever depth it has grown to by the time the
    measurement happens, so the comparison would trivially hold.
    """
    return _traceback_depth(_read_failure(server))


class TestRepeatedFailureReads:
    """Every read of a recorded failure raises its own exception object.

    A single shared instance would be mutated by each `raise`: Python appends frames to
    `__traceback__` as an exception propagates, so a caller holding on to one could watch its
    traceback change underneath it when another waiter raised, and repeated reads would pile up
    retained frames (and the locals they keep alive) without bound.
    """

    def test_each_read_raises_a_distinct_object_carrying_the_same_detail(self):
        with _running_server() as server:
            _get(server, state=EXPECTED_STATE, error="access_denied")
            first = _read_failure(server)
            second = _read_failure(server)

        assert first is not second
        assert first.reason is second.reason is OAuthLoginFailure.AUTHORIZATION_DENIED
        assert str(first) == str(second)

    def test_repeated_reads_do_not_accumulate_traceback_frames(self):
        with _running_server() as server:
            _get(server, state=EXPECTED_STATE, error="access_denied")
            depths = [_read_failure_traceback_depth(server) for _ in range(3)]

        assert depths[1] == depths[0]
        assert depths[2] == depths[0]


def _reserve_then_release_a_port() -> int:
    """A port that was free a moment ago, for the cases that need to name one up front rather than
    read it back off a bound listener."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def _refuse_to_start_a_thread(self, *args, **kwargs):
    raise RuntimeError("can't start new thread")


class TestServingThreadFailsToStart:
    """The socket is bound before the serving thread exists. If the thread cannot be created, the
    listener must not be left holding the fixed callback port -- a failed `__enter__` means
    `__exit__` never runs, so nothing else would ever release it and every later login in the
    process would hit PORT_IN_USE."""

    def test_the_failure_is_translated_and_the_port_released(self, monkeypatch):
        port = _reserve_then_release_a_port()
        server = CallbackServer(_config(port=port), EXPECTED_STATE)

        with monkeypatch.context() as patched:
            patched.setattr(threading.Thread, "start", _refuse_to_start_a_thread)
            with pytest.raises(OAuthLoginError) as exc_info:
                server.start()

        assert exc_info.value.reason is OAuthLoginFailure.SERVER_ERROR
        # Re-binding the same port is the proof that the half-started listener let go of it.
        with CallbackServer(_config(port=port), EXPECTED_STATE) as reborn:
            assert reborn.port == port

    def test_the_instance_is_still_startable(self, monkeypatch):
        """A start that failed is not a use, so the single-use rule must not have been consumed --
        the same instance can try again, exactly as it can after a failed bind."""
        server = CallbackServer(_config(), EXPECTED_STATE)

        with monkeypatch.context() as patched:
            patched.setattr(threading.Thread, "start", _refuse_to_start_a_thread)
            with pytest.raises(OAuthLoginError):
                server.start()

        try:
            server.start()
            assert server.port != 0
        finally:
            server.stop()


def _refuse_to_write(self, *args, **kwargs):
    raise BrokenPipeError("the browser closed the tab mid-response")


class TestBrowserDisconnectingMidResponse:
    """The outcome is recorded before the page is written. A browser that vanishes mid-response
    must not cost the user a captured authorization code, nor downgrade a real authorization
    failure into a timeout. Closing the listening socket does not close an already-accepted
    handler connection, so nothing about the page write depends on recording later."""

    def test_a_captured_code_survives_a_failed_page_write(self, monkeypatch):
        monkeypatch.setattr(
            "confluent_sql.oauth.callback_server._CallbackRequestHandler.send_response",
            _refuse_to_write,
        )
        with _running_server() as server:
            with contextlib.suppress(httpx.HTTPError):
                _get(server, state=EXPECTED_STATE, code=AUTH_CODE)

            assert server.wait_for_code(timeout=BRIEF_TIMEOUT) == AUTH_CODE

    def test_a_denial_survives_a_failed_page_write(self, monkeypatch):
        monkeypatch.setattr(
            "confluent_sql.oauth.callback_server._CallbackRequestHandler.send_response",
            _refuse_to_write,
        )
        with _running_server() as server:
            with contextlib.suppress(httpx.HTTPError):
                _get(server, state=EXPECTED_STATE, error="access_denied")

            with pytest.raises(OAuthLoginError) as exc_info:
                server.wait_for_code(timeout=BRIEF_TIMEOUT)

        assert exc_info.value.reason is OAuthLoginFailure.AUTHORIZATION_DENIED

    def test_a_failed_page_write_does_not_spew_a_traceback(self, monkeypatch, capfd):
        """socketserver's default `handle_error` prints a traceback per failed handler. A closed
        browser tab is an ordinary end to a *successful* login, not something to dump a stack for
        -- and the noise would land in the caller's terminal mid-login."""
        monkeypatch.setattr(
            "confluent_sql.oauth.callback_server._CallbackRequestHandler.send_response",
            _refuse_to_write,
        )
        with _running_server() as server:
            with contextlib.suppress(httpx.HTTPError):
                _get(server, state=EXPECTED_STATE, code=AUTH_CODE)
            assert server.wait_for_code(timeout=BRIEF_TIMEOUT) == AUTH_CODE

        captured = capfd.readouterr()
        assert "Traceback" not in captured.err
        assert "BrokenPipeError" not in captured.err


def _raise_listener_failure(self, *args, **kwargs):
    """Injected into `service_actions`, which the real `serve_forever` calls once per poll
    iteration -- so the genuine loop runs, and its `finally` still sets the event `shutdown()`
    waits on.

    Replacing `serve_forever` itself would be the obvious way to fake a dead listener and is a
    trap: `BaseServer.shutdown()` waits on that event *without a timeout*, so a fake that never
    sets it makes `stop()` block forever whenever the serving thread happens to still be alive.
    That hung a CI run to its 1-hour limit while passing locally every time.
    """
    raise OSError("the listener socket exploded")


class TestListenerFailure:
    def test_listener_dying_after_bind_fails_the_wait_immediately(self, monkeypatch):
        """A listener that dies after a successful bind must fail the wait, not leave the user
        staring at a browser tab until the 120s timeout expires."""
        monkeypatch.setattr(
            "confluent_sql.oauth.callback_server._CallbackHTTPServer.service_actions",
            _raise_listener_failure,
        )
        with _running_server() as server, pytest.raises(OAuthLoginError) as exc_info:
            server.wait_for_code(timeout=BRIEF_TIMEOUT)

        assert exc_info.value.reason is OAuthLoginFailure.SERVER_ERROR
        assert "exploded" in str(exc_info.value)

    def test_a_stop_before_start_does_not_suppress_a_later_listener_failure(self, monkeypatch):
        """`stop()` is documented safe before `start()`, so a caller's defensive teardown can run
        on an instance whose `start()` never bound (or was never called). That must not leave the
        instance believing a shutdown was requested -- `_serve` suppresses failure reporting when
        it was, which would turn a dead listener back into a full-timeout hang."""
        monkeypatch.setattr(
            "confluent_sql.oauth.callback_server._CallbackHTTPServer.service_actions",
            _raise_listener_failure,
        )
        server = CallbackServer(_config(), EXPECTED_STATE)
        server.stop()
        with server, pytest.raises(OAuthLoginError) as exc_info:
            server.wait_for_code(timeout=BRIEF_TIMEOUT)

        assert exc_info.value.reason is OAuthLoginFailure.SERVER_ERROR


class TestRequestLoggingIsSilenced:
    def test_the_authorization_code_never_reaches_stderr(self, capfd):
        """`BaseHTTPRequestHandler` logs every request line to stderr by default, and that line
        carries the full query string -- i.e. the authorization code. The override that silences
        it is security-relevant, so it gets a regression test."""
        with _running_server() as server:
            _get(server, state=EXPECTED_STATE, code=AUTH_CODE)
            assert server.wait_for_code(timeout=BRIEF_TIMEOUT) == AUTH_CODE

        captured = capfd.readouterr()
        assert AUTH_CODE not in captured.err
        assert AUTH_CODE not in captured.out
        assert CALLBACK_PATH not in captured.err


class TestOutcomeLogging:
    """Silencing the stderr access log must not leave the listener silent: a login that hangs
    because redirects are arriving and being rejected has to be diagnosable from the logs, and
    without the query string that got the default access log switched off."""

    def test_captured_code_is_logged_without_the_code_itself(self, caplog):
        with caplog.at_level(logging.INFO, logger=LOGGER_NAME):
            with _running_server() as server:
                _get(server, state=EXPECTED_STATE, code=AUTH_CODE)
                assert server.wait_for_code(timeout=BRIEF_TIMEOUT) == AUTH_CODE

            assert "captured the authorization code" in caplog.text
            assert AUTH_CODE not in caplog.text

    def test_state_mismatch_is_warned_about(self, caplog):
        with caplog.at_level(logging.WARNING, logger=LOGGER_NAME):
            with _running_server() as server:
                _get(server, state="not-the-expected-state", code=AUTH_CODE)
                _assert_login_still_pending(server)

            assert "state" in caplog.text
            assert "still waiting" in caplog.text

    def test_no_state_or_code_value_is_ever_logged(self, caplog):
        """The message says *that* the state did not match, never what either value was: the code
        is a live credential and the state is the CSRF token guarding it."""
        with caplog.at_level(logging.DEBUG, logger=LOGGER_NAME):
            with _running_server() as server:
                _get(server, state="not-the-expected-state", code=AUTH_CODE)
                _assert_login_still_pending(server)

            assert AUTH_CODE not in caplog.text
            assert EXPECTED_STATE not in caplog.text
            assert "not-the-expected-state" not in caplog.text

    def test_missing_code_is_warned_about(self, caplog):
        with caplog.at_level(logging.WARNING, logger=LOGGER_NAME):
            with _running_server() as server:
                _get(server, state=EXPECTED_STATE)
                _assert_login_still_pending(server)

            assert "no authorization code" in caplog.text

    def test_denied_authorization_is_warned_about(self, caplog):
        with caplog.at_level(logging.WARNING, logger=LOGGER_NAME):
            with _running_server() as server:
                _get(server, state=EXPECTED_STATE, error="access_denied")
                with pytest.raises(OAuthLoginError):
                    server.wait_for_code(timeout=BRIEF_TIMEOUT)

            assert "access_denied" in caplog.text

    def test_unknown_path_is_logged_below_warning(self, caplog):
        """Every login ends with the browser probing /favicon.ico on the success page. Warning
        about that would cry wolf on a completely healthy login, so it goes to debug."""
        with caplog.at_level(logging.DEBUG, logger=LOGGER_NAME):
            with _running_server() as server:
                _get(server, path="/favicon.ico")

            assert "/favicon.ico" in caplog.text
            assert [r for r in caplog.records if r.levelno >= logging.WARNING] == []
