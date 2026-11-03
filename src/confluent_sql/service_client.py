"""HTTP client wrapper shared by every Confluent API surface this driver talks to.

A `ServiceClient` owns one lazily-built `httpx.Client` -- the Flink gateway (data plane), or one of
the control-plane clients (Tableflow/CMK/org lookup, Connect, Artifacts) -- and is the one place
that knows how to send a request on it: translating transport and HTTP errors into the driver's own
`OperationalError` family (#138), retrying idempotent GETs (#137, #140), applying the reauth policy,
and DEBUG-logging responses.

Its single public entry point, `request()`, makes the retry decision from the HTTP method, so a GET
cannot skip retries and a mutating request (POST/PATCH/DELETE) can never be retried -- re-issuing
one after a dropped connection could double-submit or double-mutate state. Callers pick a
`ServiceClient` for the plane they mean; they never pick a retry policy.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from typing import Any, NoReturn, Protocol

import httpx

from .context import BACKGROUND, Context
from .exceptions import InterfaceError, OperationalError, ProgrammingError, RequestTimeoutError
from .retry import DEFAULT_RETRYABLE_EXCEPTIONS, DEFAULT_RETRYABLE_STATUS_CODES, call_with_retries
from .utils import extract_error_detail

logger = logging.getLogger(__name__)

_BODY_NOT_LOGGED = "<body not logged>"

StatusExceptions = Mapping[int, Callable[[httpx.Response], Exception]]
"""HTTP status -> factory building the exception to raise for it. The factory is handed the
response and *returns* the exception (the framework raises it): a lambda cannot contain `raise`."""

SendPolicy = Callable[[Callable[[], httpx.Response]], httpx.Response]
"""Wraps the one-shot send of a single HTTP call, e.g. to apply the connection's reauth policy."""


class Requester(Protocol):
    """A bound way to send requests to one API: a `ServiceClient.request`, or that plus the
    credential and logging rule of one control-plane surface. Service APIs depend on this alone,
    so they need not import (or know about) the host `Connection`. A request made inside a timed
    wait passes that wait's `Context` as `ctx=`, which `ServiceClient.request` honors; one made
    outside any wait runs under `BACKGROUND`."""

    def __call__(
        self,
        url: str,
        method: str = "GET",
        raise_for_status: bool = True,
        *,
        status_exceptions: StatusExceptions | None = None,
        error_prefix: str | None = None,
        ctx: Context = BACKGROUND,
        **kwargs: Any,
    ) -> httpx.Response: ...


def _run_directly(do_request: Callable[[], httpx.Response]) -> httpx.Response:
    return do_request()


class _RetryableStatusError(Exception):
    """Wraps a response carrying a retryable HTTP status (#140) so call_with_retries' existing
    exception-based retry loop drives status-code retries too. Never escapes `ServiceClient`."""

    def __init__(self, response: httpx.Response):
        self.response = response
        super().__init__(f"transient HTTP status {response.status_code}")


def raise_for_status_as_operational_error(
    response: httpx.Response, *, prefix: str = "error sending request"
) -> None:
    """Translate a 4xx/5xx response into OperationalError, chaining the original
    httpx.HTTPStatusError and including any server-provided error detail.

    `prefix` is the message lead-in; pass a caller-specific one (e.g. "Error enabling
    Tableflow") in place of the default.
    """
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as e:
        details = extract_error_detail(e.response)
        raise OperationalError(
            f"{prefix} '{e.response.status_code}' - {details}",
            http_status_code=e.response.status_code,
        ) from e


def apply_status_policy(
    response: httpx.Response,
    status_exceptions: StatusExceptions | None = None,
    error_prefix: str | None = None,
) -> None:
    """Raise for an error response: the exception `status_exceptions` builds for its status, if
    mapped; otherwise the standard `OperationalError` translation, led by `error_prefix` (or the
    default lead-in when None). A 2xx response returns normally.

    This is the single translation step `ServiceClient.request` ends in; test fakes standing in for
    a `Requester` call it too, so they exercise the real mapping.
    """
    if status_exceptions is not None:
        status = response.status_code
        factory = status_exceptions.get(status)
        if factory is not None and not 200 <= status < 300:
            raise factory(response)
    # Falls through for an unmapped error; a success is left to `raise_for_status` to wave through.
    if error_prefix is None:
        raise_for_status_as_operational_error(response)
    else:
        raise_for_status_as_operational_error(response, prefix=error_prefix)


def _wrap_request_error(e: httpx.RequestError) -> NoReturn:
    """Translate a network-level transport failure (no HTTP response -- timeout, connection
    reset/refused, protocol error, etc.) into OperationalError. httpx's own str() for these
    tends to be terse ("reset", "All connection attempts failed") and doesn't name the
    exception's type, so the type name is included explicitly rather than losing it.

    httpx timeouts (connect/read/write/pool) become the OperationalError subclass
    `RequestTimeoutError`, so polling loops can tell one slow response from a hard failure
    (#236)."""
    error_cls = RequestTimeoutError if isinstance(e, httpx.TimeoutException) else OperationalError
    raise error_cls(f"error sending request: {type(e).__name__}: {e}") from e


_MIN_CLAMPED_TIMEOUT_SECS = 0.001
"""Floor for a clamped httpx timeout, so an attempt that starts at (or just past) the deadline
fails fast as a timeout instead of being handed a zero or negative one."""


class ServiceClient:
    """One API surface's lazily-built `httpx.Client`, plus the request policy that goes with it.

    `build_client` runs on first use, not at construction, so building it may do work that must
    not happen while a `Connection` is being constructed (e.g. resolving `organization_id`) and may
    raise (e.g. `ProgrammingError` for a missing credential). `send_policy` wraps every individual
    HTTP call.

    The client can be built without credentials and shared: a request names its own with `auth=`
    (forwarded to httpx), which is how the control-plane surfaces reuse one client and pool.
    """

    def __init__(
        self,
        build_client: Callable[[], httpx.Client],
        *,
        send_policy: SendPolicy = _run_directly,
    ) -> None:
        self._build_client = build_client
        self._send_policy = send_policy
        self._client: httpx.Client | None = None
        self._closed = False

    @property
    def httpx_client(self) -> httpx.Client:
        """The underlying httpx client, built on first use.

        Raises:
            InterfaceError: If the owning connection is closed.
        """
        if self._closed:
            raise InterfaceError("Connection is closed")
        if self._client is None:
            self._client = self._build_client()
        return self._client

    def request(
        self,
        url: str,
        method: str = "GET",
        raise_for_status: bool = True,
        *,
        status_exceptions: StatusExceptions | None = None,
        error_prefix: str | None = None,
        log_body: bool = True,
        ctx: Context = BACKGROUND,
        **kwargs: Any,
    ) -> httpx.Response:
        """Send a request, translating every httpx.RequestError into OperationalError (#138).

        A GET is idempotent and so retried on transient transport errors (#137) and transient HTTP
        statuses (#140) with a short backoff; any other method is sent exactly once.

        An HTTP error status is translated by `apply_status_policy`: a status in `status_exceptions`
        raises the exception its factory builds from the response; any other becomes
        OperationalError carrying `http_status_code`, its message led by `error_prefix`. This runs
        once the request is finished, so a factory mapped to a retryable status (e.g. 503) fires
        only after a GET's retries are exhausted. With `raise_for_status=False` the final response
        is returned as-is and the caller owns every status; combining that with `status_exceptions`
        or `error_prefix` contradicts it and is rejected up front. `log_body=False` keeps
        the response body out of the DEBUG log, for responses carrying secrets (the Artifacts
        presign response carries a live upload URL). `ctx` is the `Context` of the operation this
        request belongs to, e.g. the one a `poll_until` attempt is handed: each attempt's httpx
        timeouts are capped at the time its deadline has left, so the request cannot run past it.
        `kwargs` (params, json, headers, auth, timeout, ...) go to `httpx.Client.request`.

        Raises:
            ProgrammingError: If `status_exceptions` or `error_prefix` is given with
                `raise_for_status=False`.
            InterfaceError: If the owning connection is closed.
            OperationalError: On a transport failure or (with `raise_for_status`) an unmapped HTTP
                error; `RequestTimeoutError` when the failure was a single httpx timeout.
        """
        if not raise_for_status and (status_exceptions is not None or error_prefix is not None):
            raise ProgrammingError(
                "status_exceptions and error_prefix have no effect with raise_for_status=False; "
                "either drop them or let the request raise"
            )
        if method.upper() == "GET":
            response = self._get_with_retries(url, log_body, ctx, **kwargs)
        else:
            try:
                response = self._send_untranslated(method, url, log_body, ctx, **kwargs)
            except httpx.RequestError as e:
                _wrap_request_error(e)
        if raise_for_status:
            apply_status_policy(response, status_exceptions, error_prefix)
        return response

    def _get_with_retries(
        self, url: str, log_body: bool, ctx: Context, **kwargs: Any
    ) -> httpx.Response:
        """Retry loop for GETs. Once retries are exhausted, transport errors become
        OperationalError and the final response is returned for `request` to judge."""

        def _get_or_flag_retryable_status() -> httpx.Response:
            response = self._send_untranslated("GET", url, log_body, ctx, **kwargs)
            if response.status_code in DEFAULT_RETRYABLE_STATUS_CODES:
                raise _RetryableStatusError(response)
            return response

        try:
            try:
                response = call_with_retries(
                    _get_or_flag_retryable_status,
                    exceptions=(*DEFAULT_RETRYABLE_EXCEPTIONS, _RetryableStatusError),
                    ctx=ctx,
                )
            except _RetryableStatusError as e:
                response = e.response
        except httpx.RequestError as e:
            _wrap_request_error(e)

        return response

    def _send_untranslated(
        self,
        method: str,
        url: str,
        log_body: bool,
        ctx: Context,
        **kwargs: Any,
    ) -> httpx.Response:
        """Send one request without translating httpx.RequestError, so `_get_with_retries` can see
        the raw exception types its retry loop keys on. Never call this outside this class."""
        client = self.httpx_client

        def send() -> httpx.Response:
            # Clamped per actual send, so a replay by the send policy (after an OAuth
            # reauthentication, say) sees the time left by then.
            return client.request(method, url, **self._clamped_to_ctx(client, ctx, kwargs))

        response = self._send_policy(send)
        logger.debug("Response: %s", response.content if log_body else _BODY_NOT_LOGGED)
        return response

    @staticmethod
    def _clamped_to_ctx(
        client: httpx.Client, ctx: Context, kwargs: dict[str, Any]
    ) -> dict[str, Any]:
        """Given a `ctx` with a deadline, cap each of this attempt's httpx timeouts (the caller's
        `timeout=` if given, else the client's) at the time that deadline has left, so one slow
        attempt cannot run past it. An unlimited (`None`) phase is capped too. Never raises a
        timeout. Evaluated per send, so a retried GET's later attempts see a shrinking budget."""
        remaining = ctx.remaining_secs()
        if remaining is None:
            return kwargs
        cap = max(remaining, _MIN_CLAMPED_TIMEOUT_SECS)
        requested = kwargs.get("timeout", httpx.USE_CLIENT_DEFAULT)
        base = client.timeout if requested is httpx.USE_CLIENT_DEFAULT else httpx.Timeout(requested)
        clamped = httpx.Timeout(
            connect=cap if base.connect is None else min(base.connect, cap),
            read=cap if base.read is None else min(base.read, cap),
            write=cap if base.write is None else min(base.write, cap),
            pool=cap if base.pool is None else min(base.pool, cap),
        )
        return {**kwargs, "timeout": clamped}

    def set_user_agent(self, value: str) -> None:
        """Apply a new User-Agent to the already-built client; a not-yet-built one picks it up from
        its factory."""
        if self._client is not None:
            self._client.headers["User-Agent"] = value

    def close(self) -> None:
        """Close the underlying client if it was ever built; later use raises InterfaceError."""
        self._closed = True
        if self._client is not None:
            self._client.close()
