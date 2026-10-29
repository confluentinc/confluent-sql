"""Value objects that group `connect()` / `Connection` arguments which belong together.

`connect()` had grown one keyword per knob. Defaultable settings that travel together live in a
small frozen dataclass here instead, each with a module-level default instance, so a caller
overrides only the part it cares about
(`dataclasses.replace(DEFAULT_TIMEOUTS, httpx_request_timeout_secs=5)`) and validation lives next
to the fields. New groups of like-minded options belong in this module.
"""

import warnings
from dataclasses import dataclass

from .exceptions import InterfaceError
from .utils import DEFAULT_HTTP_TIMEOUT_SECS

DEFAULT_STATEMENT_RESULTS_RESPONSE_WALLCLOCK_SECS: float = 30.0
"""Three times the default HTTP timeout, which is what the old three-consecutive-timeouts limit on
results fetches amounted to."""


def _require_positive_number(owner: str, name: str, value: object) -> None:
    # Reject bool explicitly: bool is a subclass of int in Python, but a True/False value here is
    # almost certainly a programming error rather than a valid timeout.
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise InterfaceError(f"{owner}.{name} must be a number, got {type(value).__name__}")
    if value <= 0:
        raise InterfaceError(f"{owner}.{name} must be positive, got {value}")


@dataclass(frozen=True)
class Timeouts:
    """How long the driver waits on the server, in seconds. Every field must be positive."""

    httpx_request_timeout_secs: float = DEFAULT_HTTP_TIMEOUT_SECS
    """Timeout for each HTTP request made by the underlying httpx clients, applied to connect,
    read, write and pool operations. With `auth="oauth"` it also bounds every HTTP hop of the token
    chain (code exchange, token exchanges, refresh), but only if this connection is the one that
    establishes the process's shared login; later connections reuse that login's provider as-is."""

    statement_results_response_wallclock_secs: float = (
        DEFAULT_STATEMENT_RESULTS_RESPONSE_WALLCLOCK_SECS
    )
    """The most wall-clock time the driver spends getting one response to a fetch of statement
    results from the server, however many timed-out attempts that takes. It applies to blocking
    reads (iterating a cursor, `fetchall()`, snapshot `fetchone()`/`fetchmany()`), which retry a
    fetch that times out; counted from the first timeout of a run, after which the last
    `RequestTimeoutError` is raised. A response, even an empty one, ends the run, and a multi-fetch
    call starts it afresh for each fetch.

    It is not a cap on how long a read may take overall: a streaming query can wait indefinitely
    for rows, and a fetch that is slow but answers within `httpx_request_timeout_secs` never
    counts against it.

    Each timed-out attempt takes up to `httpx_request_timeout_secs`, so a window of that or less is
    used up by the first timeout and that timeout is raised without any retry. That is a legitimate
    way to turn the tolerance off, but is more often `httpx_request_timeout_secs` raised without
    this, so it draws a `UserWarning`. Leave headroom: the default is three times the default
    `httpx_request_timeout_secs`."""

    def __post_init__(self) -> None:
        _require_positive_number(
            "Timeouts", "httpx_request_timeout_secs", self.httpx_request_timeout_secs
        )
        _require_positive_number(
            "Timeouts",
            "statement_results_response_wallclock_secs",
            self.statement_results_response_wallclock_secs,
        )
        if self.statement_results_response_wallclock_secs <= self.httpx_request_timeout_secs:
            # stacklevel 3: past this method and the generated __init__, to whoever built Timeouts.
            warnings.warn(
                "Timeouts(statement_results_response_wallclock_secs="
                f"{self.statement_results_response_wallclock_secs}, "
                f"httpx_request_timeout_secs={self.httpx_request_timeout_secs}): the "
                "statement-results window is no longer than one HTTP timeout, so a timed-out "
                "results fetch is raised at once instead of retried. Raise "
                "statement_results_response_wallclock_secs to keep blocking result reads "
                "tolerant of slow responses.",
                UserWarning,
                stacklevel=3,
            )


DEFAULT_TIMEOUTS = Timeouts()
"""What `connect()` uses when no `timeouts` is passed."""
