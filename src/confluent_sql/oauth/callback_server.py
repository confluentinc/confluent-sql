"""The throwaway loopback HTTP server that catches the auth service's redirect during a login.

Hop 1 of the token chain sends the user's browser to Confluent's auth service, which redirects back
to a `redirect_uri` carrying `?code=…&state=…`. Something has to be listening at that URI: this
module binds a short-lived `ThreadingHTTPServer` on `127.0.0.1:<config.callback_port>`, captures
the one authorization code, shows the human a "you can close this tab" page, and hands the code to
the thread that started the login.

Two behaviors here are less obvious than they look, and both are deliberate:

- **A rejected request does not end the login.** Anything on the box can GET a loopback port, so a
  stray or hostile request arriving while the genuine redirect is still in flight must not be able
  to cancel the user's login. Only two outcomes resolve the wait: a state-matching redirect
  carrying a `code` (success), and a state-matching redirect carrying an `error` (failure).
  Everything else gets a 4xx page and leaves the login pending until it times out.
- **Request logging is silenced.** `BaseHTTPRequestHandler.log_message` writes the full request
  line to stderr, and on this server that line contains the authorization code. See
  `_CallbackRequestHandler.log_message`.

Out of scope, by design: generating the PKCE verifier/challenge/state (`pkce.py`), building the
authorize URL, opening the browser, and the token exchanges (`token_chain.py`) -- the provider
landing in #153 owns the login sequence and merely drives this class through
`start()`/`wait_for_code()`/`stop()`, or the context manager that pairs them.
"""

from __future__ import annotations

import errno
import html
import ipaddress
import logging
import threading
import urllib.parse
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import TracebackType
from typing import Any, cast

from ..exceptions import OAuthLoginError, OAuthLoginFailure, ProgrammingError
from .config import CCloudOAuthConfig

logger = logging.getLogger(__name__)

DEFAULT_LOGIN_TIMEOUT_SECS = 120.0
"""How long `wait_for_code` waits by default -- matching mcp-confluent's PKCE_LOGIN_TIMEOUT_MS.
Long enough for a human to find the browser window, pick an identity provider, and get through an
MFA prompt; short enough that an abandoned login eventually releases the port."""

_SHUTDOWN_POLL_INTERVAL_SECS = 0.05
"""How often the serving loop checks whether `stop()` has been called. `serve_forever`'s own
default is 0.5s, which makes every teardown cost up to half a second -- worth avoiding twice over:
it is dead latency on the login path, and it widens the window in which a retry would hit
`PORT_IN_USE` against this listener's own not-yet-released port. A 50ms wakeup costs nothing
measurable over a login's lifetime."""

_STOP_JOIN_TIMEOUT_SECS = 5.0
"""Bound on how long `stop()` waits for the serving thread to wind down. The thread is a daemon,
so even the pathological case cannot keep the interpreter alive -- this only keeps `stop()` itself
from becoming an unbounded block."""


class CallbackServer:
    """A one-shot loopback listener for a single interactive login's auth-service redirect.

    Bind and start serving with `start()` (or the context manager), block for the authorization
    code with `wait_for_code()`, and release the port with `stop()`. Prefer the context manager:
    the callback port is fixed by the auth service's client registration, so a leaked listener
    blocks every subsequent login in the process.

    Not reusable -- one instance serves one login attempt, matching the single-use authorization
    code it exists to capture.
    """

    def __init__(self, config: CCloudOAuthConfig, expected_state: str) -> None:
        self._config = config
        self._expected_state = expected_state
        self._httpd: _CallbackHTTPServer | None = None
        self._thread: threading.Thread | None = None
        self._stopped = False

        # The outcome slot, written by a request-handling thread and read by the thread waiting in
        # wait_for_code. `_lock` makes the write first-wins (a reloaded success page must not
        # replace the code its PKCE verifier was minted alongside); `_settled` is what the waiter
        # actually blocks on, set only once an outcome is recorded.
        self._lock = threading.Lock()
        self._settled = threading.Event()
        self._code: str | None = None
        self._failure: OAuthLoginError | None = None

    def __enter__(self) -> CallbackServer:
        self.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.stop()

    def start(self) -> None:
        """Bind the loopback port and begin serving in a background daemon thread.

        Raises `OAuthLoginError(PORT_IN_USE)` if the port is already taken -- most often this
        process's own leftover listener from an abandoned login, or another Confluent tool
        registered on the same port -- and `OAuthLoginError(SERVER_ERROR)` for any other bind
        failure.

        Raises `ProgrammingError` if called more than once on the same instance (a CallbackServer
          is explicitly single-use), or if `config.callback_host` is not an IPv4 loopback literal
          (see `_require_ipv4_loopback`) -- checked before anything is bound.
        """
        if self._httpd is not None:
            raise ProgrammingError("This CallbackServer has already been started")

        _require_ipv4_loopback(self._config.callback_host)
        address = (self._config.callback_host, self._config.callback_port)
        try:
            self._httpd = _CallbackHTTPServer(address, _CallbackRequestHandler)
        except OSError as e:
            host, port = address
            if e.errno == errno.EADDRINUSE:
                raise OAuthLoginError(
                    f"Cannot start the OAuth callback listener: {host}:{port} is already in use. "
                    "Another login may still be in progress, or another application is bound to "
                    "this port. The port is fixed by the registered OAuth client, so it cannot "
                    "be reassigned.",
                    OAuthLoginFailure.PORT_IN_USE,
                ) from e
            raise OAuthLoginError(
                f"Cannot start the OAuth callback listener on {host}:{port}: "
                f"{type(e).__name__}: {e}",
                OAuthLoginFailure.SERVER_ERROR,
            ) from e

        self._httpd.callback_server = self
        self._httpd.callback_path = self._config.callback_path
        self._httpd.expected_state = self._expected_state
        self._thread = threading.Thread(
            target=self._serve, name="confluent-sql-oauth-callback", daemon=True
        )
        self._thread.start()

    @property
    def host(self) -> str:
        """The address actually bound. Always loopback -- the redirect never crosses the network."""
        # socketserver types server_address for every address family it supports, including the
        # bytes-y ones; an AF_INET bind is always (str, int).
        return cast(str, self._require_started().server_address[0])

    @property
    def port(self) -> int:
        """The port actually bound.

        Equal to `config.callback_port` in production, where that value is pinned by the auth
        service client's whitelisted `redirect_uri`. Reading it back from the socket is what lets
        tests bind port 0 and still know where to send their request.
        """
        return self._require_started().server_address[1]

    def wait_for_code(self, *, timeout: float = DEFAULT_LOGIN_TIMEOUT_SECS) -> str:
        """Block until the auth service's redirect delivers an authorization code, and return it.

        Blocks on an `Event` rather than polling with `sleep_with_backoff`: unlike the driver's
        server-side lifecycle waits, the condition here is signalled in-process by our own handler
        thread, so there is nothing to poll.

        Raises `OAuthLoginError` -- `TIMED_OUT` if the redirect never arrives within `timeout`
        seconds, `AUTHORIZATION_DENIED` if the auth service reported an error instead,
        `SERVER_ERROR` if the listener died. Repeated calls return the same code (or re-raise the
        same failure), including after `stop()`.

        Raises `ProgrammingError` if called before `start()`, or after `stop()` on a login that
        never produced an outcome -- in both states no redirect can arrive, so waiting out the
        timeout would only defer a verdict already known.
        """
        self._require_started()
        # Checked against the already-settled case, not instead of it: `stop()` normally runs the
        # moment the code lands, and the captured outcome stays readable afterward.
        if not self._settled.is_set() and self._stopped:
            raise ProgrammingError(
                "This CallbackServer was stopped before it captured an authorization code, so no "
                "redirect can arrive to satisfy wait_for_code(). Wait for the code before "
                "stopping the server -- the context manager sequences this correctly."
            )
        if not self._settled.wait(timeout):
            raise OAuthLoginError(
                f"Timed out after {timeout} seconds waiting for the browser to complete the "
                "Confluent Cloud login.",
                OAuthLoginFailure.TIMED_OUT,
            )
        with self._lock:
            if self._failure is not None:
                raise self._failure
            # _settled is only ever set alongside one of the two slots, so this is a code.
            assert self._code is not None
            return self._code

    def stop(self) -> None:
        """Stop serving and release the port. Idempotent, and safe before `start()`."""
        httpd, thread = self._httpd, self._thread
        if httpd is None:
            # Nothing was ever bound, so there is nothing to stop -- and deliberately no state
            # change either.
            return

        self._stopped = True
        # Only ask a live loop to exit: `shutdown()` waits on an event that `serve_forever` sets
        # as it unwinds, so calling it after the thread has already gone would have nothing left
        # to wake it.
        if thread is not None and thread.is_alive():
            httpd.shutdown()
        httpd.server_close()
        if thread is not None:
            thread.join(timeout=_STOP_JOIN_TIMEOUT_SECS)

    def _serve(self) -> None:
        """The background thread's body: serve until `stop()`, recording any failure.

        A listener that dies after a successful bind (a closed socket, an exhausted fd table)
        would otherwise leave `wait_for_code` blocked for the full timeout on an event nothing can
        ever set, so the failure is recorded as the login's outcome instead.
        """
        assert self._httpd is not None
        try:
            self._httpd.serve_forever(poll_interval=_SHUTDOWN_POLL_INTERVAL_SECS)
        except Exception as e:
            if not self._stopped:
                self._record_failure(
                    OAuthLoginError(
                        f"The OAuth callback listener failed after binding: "
                        f"{type(e).__name__}: {e}",
                        OAuthLoginFailure.SERVER_ERROR,
                    )
                )

    def _record_code(self, code: str) -> None:
        with self._lock:
            if self._settled.is_set():
                return
            self._code = code
            self._settled.set()

    def _record_failure(self, failure: OAuthLoginError) -> None:
        with self._lock:
            if self._settled.is_set():
                return
            self._failure = failure
            self._settled.set()

    def _require_started(self) -> _CallbackHTTPServer:
        if self._httpd is None:
            raise ProgrammingError("This CallbackServer has not been started yet")
        return self._httpd


class _CallbackHTTPServer(ThreadingHTTPServer):
    """The listener, carrying a typed reference back to its `CallbackServer`.

    Threading through the server instance (rather than closing over the `CallbackServer` in a
    per-instance handler class) is what lets the handler stay an ordinary module-level class.
    """

    daemon_threads = True

    callback_server: CallbackServer
    callback_path: str
    expected_state: str


class _CallbackRequestHandler(BaseHTTPRequestHandler):
    """Handles the single GET the auth service's redirect makes, plus any stray request that
    beats it to the port."""

    @property
    def _listener(self) -> _CallbackHTTPServer:
        """The listener this request arrived on, narrowed from socketserver's `BaseServer`.

        A property rather than re-annotating the inherited `server` attribute, which pyright
        rejects: a mutable attribute's type is invariant, so a subclass cannot narrow it.
        """
        return cast(_CallbackHTTPServer, self.server)

    def do_GET(self) -> None:  # `do_GET` is the name BaseHTTPRequestHandler dispatches to
        listener = self._listener
        callback_server = listener.callback_server
        split = urllib.parse.urlsplit(self.path)
        if split.path != listener.callback_path:
            # Debug, not warning: the browser probing /favicon.ico on the success page lands here
            # on every single login, so this branch is routine rather than notable.
            logger.debug(f"OAuth callback listener ignoring a request to {split.path!r}")
            self._send_page(HTTPStatus.NOT_FOUND, _error_page("Not found."))
            return

        params = urllib.parse.parse_qs(split.query)
        if _first(params, "state") != listener.expected_state:
            # Checked before `error` so that an unsolicited error redirect cannot abort a login it
            # does not belong to.
            logger.warning(
                "Rejected a login redirect whose 'state' does not match the pending login; "
                "still waiting for the expected redirect. A browser tab replaying an earlier "
                "login attempt is the usual cause."
            )
            self._send_page(
                HTTPStatus.BAD_REQUEST,
                _error_page(
                    "This login response does not match the pending login (state mismatch)."
                ),
            )
            return

        error = _first(params, "error")
        if error is not None:
            description = _first(params, "error_description")
            detail = f"{error}: {description}" if description else error
            logger.warning(f"Confluent Cloud login was not granted -- {detail}")
            # Record before responding, same as the success branch below: the waiter's verdict must
            # not depend on the page write surviving.
            callback_server._record_failure(
                OAuthLoginError(
                    f"Confluent Cloud login was not granted -- {detail}",
                    OAuthLoginFailure.AUTHORIZATION_DENIED,
                )
            )
            self._send_page(
                HTTPStatus.BAD_REQUEST, _error_page(f"Confluent Cloud login failed -- {detail}")
            )
            return

        code = _first(params, "code")
        if code is None:
            logger.warning(
                "Rejected a login redirect that matched the pending login's 'state' but carried "
                "no authorization code; still waiting for the expected redirect."
            )
            self._send_page(
                HTTPStatus.BAD_REQUEST, _error_page("Login response carried no authorization code.")
            )
            return

        logger.info("Received the Confluent Cloud login redirect; captured the authorization code")
        # Record before responding. The code is the whole point of this request, and it is
        # single-use: if the browser resets the connection mid-write, recording afterwards would
        # discard a perfectly good code and cost the user an entire fresh login. Nothing about the
        # response depends on recording later, either -- the waiter's `stop()` closes the
        # *listening* socket, not this handler's already-accepted connection.
        callback_server._record_code(code)
        self._send_page(HTTPStatus.OK, _SUCCESS_PAGE)

    def log_message(self, format: str, *args: Any) -> None:
        """Silence the default stderr access log.

        `BaseHTTPRequestHandler` logs the whole request line, which on this server reads
        `GET /gateway/v1/callback-…?code=<the authorization code>&state=… HTTP/1.1`. Writing a
        live credential to stderr -- where it lands in terminal scrollback, CI logs, and captured
        subprocess output -- is not something to leave on by default for the sake of a debug line.

        Silencing it does not make the listener silent: `do_GET` logs each outcome through this
        module's `logger` with a curated message instead, so a login that hangs because redirects
        are being rejected is diagnosable without any query string reaching a log.
        """

    def _send_page(self, status: HTTPStatus, body: str) -> None:
        """Write one HTML page, treating a broken connection as unremarkable.

        By the time this runs the login's outcome is already recorded, so a browser that closed
        its tab mid-write has cost us nothing. Letting the `OSError` escape would hand it to
        socketserver's default `handle_error`, which dumps a traceback to stderr -- alarming
        output in the caller's terminal for what is, on the success path, a *completed* login.
        """
        try:
            self._write_page(status, body)
        except OSError as e:
            logger.debug(
                f"Could not write the OAuth callback page (the browser most likely closed the "
                f"connection): {type(e).__name__}: {e}"
            )

    def _write_page(self, status: HTTPStatus, body: str) -> None:
        encoded = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)


def _require_ipv4_loopback(host: str) -> None:
    """Refuse to bind anything but an IPv4 loopback literal.

    `callback_host` is public config, and this listener receives a live authorization code over
    plain HTTP with no transport security at all -- so "loopback only" has to be enforced rather
    than merely documented. A value like `0.0.0.0` or `""` would publish the callback endpoint on
    every interface on the machine.

    Three kinds of value are refused, all deliberately:

    - **Non-loopback addresses**, the case this exists for.
    - **Names, `localhost` included.** What a name resolves to is decided by the host's resolver
      configuration rather than by us; RFC 8252 §8.3 recommends the literal address for native-app
      loopback redirects for exactly that reason.
    - **IPv6, `::1` included.** `ThreadingHTTPServer` is `AF_INET`, so an IPv6 literal cannot be
      bound at all; refusing it here turns an obscure bind-time `OSError` into a clear message.
      The registered `redirect_uri` is IPv4 regardless.
    """
    try:
        address = ipaddress.ip_address(host)
        acceptable = address.version == 4 and address.is_loopback
    except ValueError:
        acceptable = False
    if not acceptable:
        raise ProgrammingError(
            f"The OAuth callback listener refuses to bind {host!r}: it must be an IPv4 loopback "
            "address literal, such as 127.0.0.1. The redirect carries a live authorization code "
            "over plain HTTP, so this listener must not be reachable from off the machine."
        )


def _first(params: dict[str, list[str]], key: str) -> str | None:
    """The first value for `key`, or None if absent. `parse_qs` yields a list per key, since a
    query string may repeat one; a duplicated OAuth parameter is malformed, and taking the first
    keeps a repeat from smuggling a second value past the checks above."""
    values = params.get(key)
    return values[0] if values else None


def _page(heading: str, message: str) -> str:
    """One self-contained HTML page. No external stylesheet, font, script, or image: the browser
    showing this may have no reach past loopback, and a tab whose URL bar holds an authorization
    code has no business making requests anywhere."""
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Confluent Cloud login</title>
<style>
  body {{
    font-family: system-ui, -apple-system, "Segoe UI", sans-serif;
    background: #f7f8fa;
    color: #1a1a2e;
    display: flex;
    align-items: center;
    justify-content: center;
    min-height: 100vh;
    margin: 0;
  }}
  main {{
    background: #ffffff;
    border-radius: 10px;
    box-shadow: 0 2px 12px rgba(0, 0, 0, 0.08);
    max-width: 30rem;
    padding: 2.5rem;
    text-align: center;
  }}
  h1 {{ font-size: 1.35rem; margin: 0 0 0.75rem; }}
  p {{ color: #4a4a68; line-height: 1.5; margin: 0; }}
</style>
</head>
<body>
<main>
<h1>{html.escape(heading)}</h1>
<p>{html.escape(message)}</p>
</main>
</body>
</html>
"""


_SUCCESS_PAGE = _page(
    "Login successful",
    "You are signed in to Confluent Cloud. You can close this tab and return to your program.",
)
"""Rendered once at import: the success page has nothing per-request to interpolate."""


def _error_page(message: str) -> str:
    return _page("Login failed", message)
