"""Session-lifecycle policy for an `auth="oauth"` connection (#198).

Groups the `auth="oauth"`-only knobs governing how a `Connection` behaves over its OAuth
session's lifetime into one frozen struct, passed as `connect(oauth_policy=...)` -- rather than
one `oauth_*` scalar per knob, which would keep growing one look-alike parameter per child
ticket. #157 adds this struct's second field (`token_refresh_mode`, governing whether the
short-lived control-plane/data-plane tokens are refreshed on demand or by a background daemon);
this ticket carries only the first (what to do once the session is unrecoverable without a fresh
login).

Because the struct is frozen, two `OAuthPolicy` instances compare equal (and hash equal)
whenever their fields match. That is what lets a later child give the process-wide OAuth holder
a one-line whole-struct equality guard across concurrently-opened `Connection`s, mirroring its
existing checks on environment and organization.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from ..exceptions import InterfaceError


@dataclass(frozen=True)
class OAuthPolicy:
    """Session-lifecycle policy for an `auth="oauth"` `Connection`.

    Every field defaults to what a first-time, interactively-attended caller wants; a non-default
    field opts into behavior suited to some flavor of unattended or automated use. Construct
    directly, or start from a preset below (`OAUTH_INTERACTIVE`, `OAUTH_UNATTENDED`) and override
    only what differs.
    """

    on_reauthentication_required: Literal["auto", "raise"] = "auto"
    """How to respond when a request raises `ReauthenticationRequired` -- the session's refresh
    token can no longer be exchanged (the ~8h absolute wall, idle expiry, or revocation), so only
    a fresh interactive login (another browser round-trip) can recover it. This is distinct from
    the routine control-plane/data-plane token refresh that keeps requests authenticated minute to
    minute (#157 adds a field governing *that*) -- this field only ever matters once that refresh
    has failed *terminally* in the way just described, not on an ordinary transient failure (a
    429/5xx blip from the token endpoint), which propagates as an unrelated error and leaves the
    session retryable on the next request.

    `"auto"` (default): catch the exception transparently, block on
    `confluent_sql.oauth.reauthenticate()` (another browser round-trip), and retry the request
    once. `auth="oauth"` is inherently a human-present mode -- login itself requires a browser --
    so popping one again hours later to keep going is unsurprising for the common case.

    `"raise"`: let `ReauthenticationRequired` propagate instead of attempting the browser round
    trip at all. For a session that *started* attended but may be unattended by the time it
    crosses the wall (a long dbt run kicked off and left running, a notebook kernel idle
    overnight), `"auto"` recovers no more often than `"raise"` -- both eventually fail with no one
    at the browser -- it just spends the login timeout finding that out first. `"raise"` fails the
    one request immediately instead.
    """

    def __post_init__(self) -> None:
        if self.on_reauthentication_required not in ("auto", "raise"):
            raise InterfaceError(
                "on_reauthentication_required must be 'auto' or 'raise', got "
                f"{self.on_reauthentication_required!r}"
            )


OAUTH_INTERACTIVE = OAuthPolicy(on_reauthentication_required="auto")
"""The default: a human is present at the browser, so a lapsed session re-prompts and continues."""

OAUTH_UNATTENDED = OAuthPolicy(on_reauthentication_required="raise")
"""A session that *started* attended but may be alone at the wall by the time it dies (a long dbt
run, an idle notebook kernel) -- surface `ReauthenticationRequired` instead of blocking on a
browser no one will answer."""
