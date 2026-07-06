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
from typing import TypeVar

import httpx

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

_RETRY_BASE_DELAY_SECS = 0.1
"""Delay before the first retry sleep -- tuned for sub-second transient network blips rather than
the multi-second server-state polling that polling.sleep_with_backoff paces."""

_RETRY_DELAY_GROWTH = 2.0
"""Multiplier applied to the delay after each retry."""

_RETRY_JITTER_BAND = (0.75, 1.25)
"""Multiplicative jitter band (+-25%) applied to each sleep to avoid thundering-herd alignment."""


def call_with_retries(
    func: Callable[..., T],
    *args,
    max_retries: int = 3,
    exceptions: tuple[type[BaseException], ...] = DEFAULT_RETRYABLE_EXCEPTIONS,
    **kwargs,
) -> T:
    """Call func(*args, **kwargs), retrying up to max_retries times on the given exceptions.

    Sleeps a short exponential backoff with jitter between attempts and logs one INFO line per
    retry naming the attempt number. After max_retries retries (max_retries + 1 total attempts),
    the original exception from the final attempt is re-raised.
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
            logger.info(
                'Retrying %s after transient error (attempt %d/%d): "%s"; sleeping %.3fs',
                getattr(func, "__name__", repr(func)),
                attempt,
                max_retries,
                e,
                sleep_secs,
            )
            time.sleep(sleep_secs)
            delay *= _RETRY_DELAY_GROWTH
