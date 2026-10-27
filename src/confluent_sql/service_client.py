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
from collections.abc import Callable
from typing import Any, NoReturn, Protocol

import httpx

from .exceptions import InterfaceError, OperationalError, RequestTimeoutError
from .retry import DEFAULT_RETRYABLE_EXCEPTIONS, DEFAULT_RETRYABLE_STATUS_CODES, call_with_retries
from .utils import extract_error_detail

logger = logging.getLogger(__name__)

_BODY_NOT_LOGGED = "<body not logged>"

SendPolicy = Callable[[Callable[[], httpx.Response]], httpx.Response]
"""Wraps the one-shot send of a single HTTP call, e.g. to apply the connection's reauth policy."""


class Requester(Protocol):
    """A bound way to send requests to one API: a `ServiceClient.request`, or that plus the
    credential and logging rule of one control-plane surface. Service APIs depend on this alone,
    so they need not import (or know about) the host `Connection`."""

    def __call__(
        self, url: str, method: str = "GET", raise_for_status: bool = True, **kwargs: Any
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
        log_body: bool = True,
        **kwargs: Any,
    ) -> httpx.Response:
        """Send a request, translating every httpx.RequestError into OperationalError (#138).

        A GET is idempotent and so retried on transient transport errors (#137) and transient HTTP
        statuses (#140) with a short backoff; any other method is sent exactly once. An HTTP error
        status becomes OperationalError carrying `http_status_code`, unless `raise_for_status` is
        False (callers that map specific codes -- 404/409 -- to their own exceptions); then the
        final response is returned as-is, even after retries are exhausted. `log_body=False` keeps
        the response body out of the DEBUG log, for responses carrying secrets (the Artifacts
        presign response carries a live upload URL). `kwargs` (params, json, headers, auth,
        timeout, ...) go to `httpx.Client.request`.

        Raises:
            InterfaceError: If the owning connection is closed.
            OperationalError: On a transport failure or (with `raise_for_status`) an HTTP error;
                `RequestTimeoutError` when the failure was a single httpx timeout.
        """
        if method.upper() == "GET":
            return self._get_with_retries(url, raise_for_status, log_body, **kwargs)
        try:
            return self._send_untranslated(method, url, raise_for_status, log_body, **kwargs)
        except httpx.RequestError as e:
            _wrap_request_error(e)

    def _get_with_retries(
        self, url: str, raise_for_status: bool, log_body: bool, **kwargs: Any
    ) -> httpx.Response:
        """Retry loop for GETs. Once retries are exhausted, transport errors become
        OperationalError and the final response is returned for `raise_for_status` to judge."""

        def _get_or_flag_retryable_status() -> httpx.Response:
            response = self._send_untranslated("GET", url, False, log_body, **kwargs)
            if response.status_code in DEFAULT_RETRYABLE_STATUS_CODES:
                raise _RetryableStatusError(response)
            return response

        try:
            try:
                response = call_with_retries(
                    _get_or_flag_retryable_status,
                    exceptions=(*DEFAULT_RETRYABLE_EXCEPTIONS, _RetryableStatusError),
                )
            except _RetryableStatusError as e:
                response = e.response
        except httpx.RequestError as e:
            _wrap_request_error(e)

        if raise_for_status:
            raise_for_status_as_operational_error(response)
        return response

    def _send_untranslated(
        self, method: str, url: str, raise_for_status: bool, log_body: bool, **kwargs: Any
    ) -> httpx.Response:
        """Send one request without translating httpx.RequestError, so `_get_with_retries` can see
        the raw exception types its retry loop keys on. Never call this outside this class."""
        client = self.httpx_client
        response = self._send_policy(lambda: client.request(method, url, **kwargs))
        logger.debug("Response: %s", response.content if log_body else _BODY_NOT_LOGGED)
        if raise_for_status:
            raise_for_status_as_operational_error(response)
        return response

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
