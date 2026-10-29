"""Retry helper for idempotent, side-effect-free calls.

Centralizes retrying of calls that intermittently fail with transient transport errors, so this
policy lives in one place rather than being reimplemented at each call site. Only appropriate for
calls where re-issuing after a partial failure is safe -- retrying a call with side effects (e.g. a
POST that already reached the server before the response was lost) could double-submit or
double-mutate state.
"""

import logging
import random
import time
from collections.abc import Callable
from typing import Any, TypeVar

import httpx

from .context import BACKGROUND, Context

logger = logging.getLogger(__name__)

T = TypeVar("T")

DEFAULT_RETRYABLE_EXCEPTIONS: tuple[type[BaseException], ...] = (
    httpx.NetworkError,
    httpx.RemoteProtocolError,
)
"""Transient transport errors worth retrying: connection resets/drops (httpx.NetworkError covers
ConnectError/ReadError/WriteError/CloseError) and servers that close pooled connections without
responding (httpx.RemoteProtocolError). httpx.TimeoutException is deliberately excluded --
retrying a timeout compounds latency (up to (max_retries + 1)x the configured timeout) rather than
recovering from a blip; pass a custom `exceptions` tuple to opt in."""

DEFAULT_RETRYABLE_STATUS_CODES: frozenset[int] = frozenset({429, 500, 502, 503, 504})
"""HTTP statuses worth retrying on an idempotent GET: 429 (rate limited) and 500/502/503/504
(gateway/server transients). Consumed by `ServiceClient`, not by `call_with_retries`
itself, which has no HTTP-specific knowledge. 408 (Request Timeout) is deliberately excluded --
same "don't compound latency on a timeout" reasoning as excluding httpx.TimeoutException from
DEFAULT_RETRYABLE_EXCEPTIONS above. Honoring a Retry-After header on 429/503 is out of scope."""

_RETRY_BASE_DELAY_SECS = 0.1
"""Delay before the first retry sleep -- tuned for sub-second transient network blips rather than
the multi-second server-state polling that `polling.poll_until` paces."""

_RETRY_DELAY_GROWTH = 2.0
"""Multiplier applied to the delay after each retry."""

_RETRY_JITTER_BAND = (0.75, 1.25)
"""Multiplicative jitter band (+-25%) applied to each sleep to avoid thundering-herd alignment."""


def call_with_retries(
    func: Callable[..., T],
    *args: Any,
    max_retries: int = 3,
    exceptions: tuple[type[BaseException], ...] = DEFAULT_RETRYABLE_EXCEPTIONS,
    ctx: Context = BACKGROUND,
    **kwargs: Any,
) -> T:
    """Call func(*args, **kwargs), retrying up to max_retries times on the given exceptions.

    Sleeps a short exponential backoff with jitter between attempts and logs one INFO line per
    retry naming the attempt number. After max_retries retries (max_retries + 1 total attempts),
    the original exception from the final attempt is re-raised.

    `ctx` bounds the whole call in wall-clock time (it is consumed here, not forwarded to func): a
    retry happens only if the deadline outlasts its backoff sleep, so no sleep runs past the
    deadline and no attempt is begun that has no time left. When the deadline is what stops the
    retries, the last attempt's own exception is raised, exactly as when max_retries runs out. The
    first attempt is always made, even under an already-expired ctx. Bounding each attempt itself
    is func's job (see `ServiceClient`, which caps its requests' timeouts at the ctx's remainder).
    """
    delay = _RETRY_BASE_DELAY_SECS
    attempt = 0
    while True:
        try:
            return func(*args, **kwargs)
        except exceptions as e:
            attempt += 1
            if attempt > max_retries:
                raise
            sleep_secs = delay * random.uniform(*_RETRY_JITTER_BAND)
            remaining = ctx.remaining_secs()
            if remaining is not None and remaining <= sleep_secs:
                logger.info(
                    "Not retrying %s: deadline (%.3fs left) is nearer than the %.3fs backoff",
                    getattr(func, "__name__", repr(func)),
                    remaining,
                    sleep_secs,
                )
                raise
            logger.info(
                'Retrying %s after transient error (attempt %d/%d): "%s"; sleeping %.3fs',
                getattr(func, "__name__", repr(func)),
                attempt,
                max_retries,
                e,
                sleep_secs,
            )
            # No re-check of the deadline after the sleep: if the OS wakes us late, the next attempt
            # is still clamped to a tiny remainder by the caller. This is not a realtime engine.
            time.sleep(sleep_secs)
            delay *= _RETRY_DELAY_GROWTH
