"""Shared pacing for server-polling loops.

Centralizes the exponential-backoff-with-jitter policy used when waiting on asynchronous
statement state transitions, so the timing mechanics (and their tuning) live in one place rather
than being copied into each poller.
"""

import random
import time
from collections.abc import Iterator

_POLL_BASE_DELAY_SECS = 1.0
"""Delay before the first retry sleep."""

_POLL_MAX_DELAY_SECS = 30.0
"""Ceiling on the per-retry sleep, regardless of how long we have been waiting."""

_POLL_DELAY_GROWTH = 1.5
"""Multiplier applied to the delay after each retry."""

_POLL_JITTER_BAND = (0.75, 1.25)
"""Multiplicative jitter band (±25%) applied to each sleep to avoid thundering-herd alignment."""


def sleep_with_backoff(timeout_secs: float) -> Iterator[None]:
    """Pace the retries of a polling loop, yielding once per retry until timeout_secs elapses.

    The caller makes its own first (immediate, unpaced) attempt; each yield then sleeps with
    exponential backoff + jitter and signals "you have waited, try again". The generator stops
    yielding once timeout_secs has elapsed, at which point the caller is responsible for raising
    its own timeout error.
    """
    start = time.monotonic()
    delay = _POLL_BASE_DELAY_SECS
    while time.monotonic() - start < timeout_secs:
        time.sleep(delay * random.uniform(*_POLL_JITTER_BAND))
        delay = min(delay * _POLL_DELAY_GROWTH, _POLL_MAX_DELAY_SECS)
        yield
