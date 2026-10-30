"""A Go-style `Context`: the wall-clock deadline of the operation a request is part of.

A timed wait (`poll_until`) creates one with its deadline and hands it to each attempt. The attempt
passes it down, explicitly, as `ctx=` to the request it makes, and `ServiceClient.request` caps that
request's httpx timeouts at the time left, so a single slow request cannot run past the wait's
deadline. Like Go's `context.Context` it is derived (`with_timeout`) and only ever tightens: a child
can end sooner than its parent, never later.

Unlike Go's it carries only a deadline: there is no cancellation, and no values.
"""

from __future__ import annotations

import time
from dataclasses import dataclass


@dataclass(frozen=True, slots=True, repr=False)
class Context:
    """An immutable carrier of an optional wall-clock deadline (a `time.monotonic()` instant).

    Frozen, so the shared `BACKGROUND` singleton cannot be mutated by a caller."""

    deadline: float | None = None

    @classmethod
    def background(cls) -> Context:
        """The root context: no deadline."""
        return BACKGROUND

    def with_timeout(self, secs: float) -> Context:
        """A child that ends `secs` from now, or when this one does if that is sooner."""
        deadline = time.monotonic() + secs
        if self.deadline is not None:
            deadline = min(deadline, self.deadline)
        return Context(deadline)

    def remaining_secs(self) -> float | None:
        """Seconds until the deadline (zero once it has passed, never negative), or None if there
        is no deadline."""
        return None if self.deadline is None else max(0.0, self.deadline - time.monotonic())

    def __repr__(self) -> str:
        remaining = self.remaining_secs()
        return (
            "Context(background)" if remaining is None else f"Context(remaining={remaining:.3f}s)"
        )


BACKGROUND = Context()
"""The root `Context`, with no deadline; what an operation outside any timed wait runs under."""
