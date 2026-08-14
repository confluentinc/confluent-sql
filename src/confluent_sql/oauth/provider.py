"""`CCloudOAuth`: one interactive login, one shared `TokenSet`, two `httpx.Auth` views over it.

This is the orchestrator that turns #151's pure primitives and #152's callback server into a
working credential. It owns everything stateful: the current token snapshot, the locks guarding
it, the private `httpx.Client` the token chain runs over, and the organization the login settled
on. It is deliberately **not** itself an `httpx.Auth` -- that split is what lets one login feed
two clients carrying two *different* tokens (`control_plane_auth` for Tableflow / Connect / CMK,
`data_plane_auth` for Flink) instead of forcing a provider per surface.

**Refresh here is synchronous and on-request, and that is the whole story.** There is no
background thread in this child (#157 adds one purely to keep the ~4-round-trip refresh latency
off the hot path). Correctness deliberately does not depend on that thread ever existing: every
request checks its token's validity, and a stale one is refreshed through the single-flight gate
before the header is stamped. A connection that sat idle for an hour re-mints from the long-lived
refresh token on its next request, however long the short-lived (~5-min) tokens have been lapsed.

Two locks, with sharply different hold times, and they are always acquired in this order --
`_refresh_lock` first, `_token_lock` second, never the reverse:

- **`_token_lock`, held for microseconds.** Guards the reference slot and the failure flag.
  Nothing but read-the-reference and rebind-the-reference happens under it, and it is never held
  across network I/O. Because a `TokenSet` is immutable, a reader that has copied the reference
  can then read its fields with no lock at all and no risk of a torn read: refresh builds a
  brand-new snapshot rather than mutating one.
- **`_refresh_lock`, held across the ~4-call chain.** The single-flight gate, and `login()`'s gate
  too (#156's re-auth will reuse it, so that N threads at the 8h wall collapse to one browser
  bounce). It is separate from `_token_lock` precisely because a refresh is four sequential HTTP
  round-trips; holding the reference lock across those would block every reader.

**Refresh tokens are single-use and rotating**, and two threads spending the same one leaves the
second refused and the session dead. Two distinct mechanisms keep that from happening, and it is
worth being precise about which does what, because the epic's design notes conflate them:

- **What makes a double-spend impossible** is that `_refresh` reads the token to spend out of the
  slot *inside* the gate, rather than out of the snapshot its caller arrived with. Whatever a
  waiter was holding when it queued, by the time it runs it spends the current token or none.
- **What the double-check buys** is avoiding *redundant* chain runs. Without it, N waiters each
  run their own four-call chain -- each spending a legitimate, freshly-rotated token, so no
  lockout, but burning N refreshes against the service's ~50-refresh cap and paying four round
  trips apiece for a token they already have.

Both matter; only the first is about the token being single-use.

Prior art: mcp-confluent's `oauth/auth-context.ts` (`refresh()` single-flight, and `doRefresh()`
persisting the rotated refresh token *before* the CP/DP legs -- the ordering `_refresh` copies
below); ide-sidecar's `CCloudOAuthContext` (the same chain under a `ReentrantReadWriteLock`, with
`getDataPlaneAuthenticationHeaders()` / `getControlPlaneAuthenticationHeaders()` as the two-view
analogue).
"""

from __future__ import annotations

import dataclasses
import logging
import threading
import urllib.parse
import webbrowser
from collections.abc import Callable, Generator
from datetime import datetime, timezone
from enum import Enum

import httpx

from ..exceptions import (
    OAuthTokenEndpointError,
    OperationalError,
    ProgrammingError,
    ReauthenticationReason,
    ReauthenticationRequired,
)
from .callback_server import DEFAULT_LOGIN_TIMEOUT_SECS, CallbackServer
from .config import CCloudOAuthConfig
from .pkce import challenge_for, generate_state, generate_verifier
from .token_chain import (
    exchange_code_for_tokens,
    exchange_cp_for_dp_token,
    exchange_id_token_for_cp_token,
    exchange_refresh_token,
)
from .token_set import ABSOLUTE_LIFETIME, TokenSet

logger = logging.getLogger(__name__)

_INVALID_GRANT = "invalid_grant"
"""The token endpoint's machine-readable verdict that a refresh token is expired, revoked, or
already spent (RFC 6749 section 5.2). The one refresh failure no retry can fix, and the reason
`OAuthTokenEndpointError` carries the code as data rather than folding it into the message."""


class _Plane(Enum):
    """Which of the snapshot's two Confluent tokens a given auth view reads.

    One parameterized adapter rather than two near-identical classes: the planes differ only in
    which field they read and which validity helper they call, and every line of the refresh /
    401-retry logic around that is identical.
    """

    CONTROL = "control-plane"
    DATA = "data-plane"

    def token(self, snapshot: TokenSet) -> str:
        return snapshot.cp_token if self is _Plane.CONTROL else snapshot.dp_token

    def token_valid(self, snapshot: TokenSet, now: datetime) -> bool:
        if self is _Plane.CONTROL:
            return snapshot.cp_token_valid(now)
        return snapshot.dp_token_valid(now)


class CCloudOAuth:
    """Owns one Confluent Cloud interactive login and the tokens it mints.

    Drive it with `login()`, then hand `data_plane_auth` / `control_plane_auth` to the clients
    that need them. `close()` releases the private HTTP client.

    One provider is one identity: one `(user, organization)` pair, established by `login()` and
    fixed for the provider's life. Sharing a single provider process-wide -- so that N
    `Connection`s trigger one browser bounce rather than N -- is #154's holder; this class knows
    nothing about that and is usable standalone.
    """

    def __init__(
        self,
        config: CCloudOAuthConfig,
        *,
        http_client: httpx.Client | None = None,
        open_browser: Callable[[str], bool] = webbrowser.open,
    ) -> None:
        """
        Args:
            config: the environment to authenticate against (`PROD`, typically).
            http_client: the client the token chain runs over. Defaults to a fresh one. It is
                deliberately **private to this provider** -- never any of a `Connection`'s three
                request clients -- so a refresh triggered from one caller's thread cannot
                contend with that caller's own in-flight requests. Owned either way: `close()`
                closes it.
            open_browser: how to send the user to the authorization URL. Returning False (a
                headless box, no `BROWSER`, no display) is not a failure -- the URL is logged so
                the user can paste it, and the login waits as normal.
        """
        self._config = config
        self._client = http_client if http_client is not None else httpx.Client()
        self._open_browser = open_browser

        # Lock ordering, everywhere in this class: _refresh_lock before _token_lock, never the
        # reverse. Nothing acquires the refresh gate while holding the reference lock.
        self._refresh_lock = threading.Lock()
        self._token_lock = threading.Lock()

        self._token_set: TokenSet | None = None
        self._failure: tuple[str, ReauthenticationReason] | None = None
        self._organization_id: str | None = None

        self._control_plane_auth = _PlaneAuth(self, _Plane.CONTROL)
        self._data_plane_auth = _PlaneAuth(self, _Plane.DATA)

    @property
    def token_set(self) -> TokenSet | None:
        """The current immutable snapshot, or None before `login()`.

        Safe to read from any thread: the reference is copied under `_token_lock`, and the object
        it names is never mutated.
        """
        with self._token_lock:
            return self._token_set

    @property
    def organization_id(self) -> str | None:
        """The `organization.resource_id` this login settled on -- whichever was supplied to
        `login()`, or the default Confluent Cloud resolved for the user. None before `login()`.

        #155 reads this back to fill the Flink path for a `connect()` that omitted the org."""
        with self._token_lock:
            return self._organization_id

    @property
    def control_plane_auth(self) -> httpx.Auth:
        """The view stamping the control-plane token -- Tableflow, Connect, CMK, org lookups."""
        return self._control_plane_auth

    @property
    def data_plane_auth(self) -> httpx.Auth:
        """The view stamping the data-plane token -- the Flink SQL gateway."""
        return self._data_plane_auth

    def login(
        self,
        org_resource_id: str | None = None,
        *,
        timeout: float = DEFAULT_LOGIN_TIMEOUT_SECS,
    ) -> None:
        """Run the interactive browser login end to end and populate the token set.

        Binds the loopback callback listener, sends the user's browser to the authorization URL,
        waits for the redirect, then runs the three hops: authorization code -> id_token,
        id_token -> control-plane token, control-plane token -> data-plane token.

        Args:
            org_resource_id: scopes the session to one organization, for a user who belongs to
                several. Omitted, Confluent Cloud resolves the user's default and reports it
                back; either way the resolved value lands on `organization_id`.
            timeout: seconds to wait for the browser round-trip -- human time, so the default is
                generous.

        Raises `OAuthLoginError` if the browser leg fails (timeout, denied consent, an
        unavailable callback port), `OperationalError` if a token exchange fails, and
        `ProgrammingError` if this provider has already logged in.
        """
        # The same gate a refresh runs under: a login and a refresh must never overlap, and
        # #156's re-auth reuses this method behind this very lock.
        with self._refresh_lock:
            if self._token_set is not None:
                raise ProgrammingError(
                    "This CCloudOAuth provider has already logged in. One provider is one "
                    "identity for its whole life; use a new provider for a different one."
                )

            verifier = generate_verifier()
            state = generate_state()
            with CallbackServer(self._config, state) as server:
                authorize_url = self._authorize_url(challenge_for(verifier), state)
                if not self._open_browser(authorize_url):
                    logger.info(
                        "Could not open a browser automatically. To finish signing in to "
                        f"Confluent Cloud, open this URL yourself: {authorize_url}"
                    )
                code = server.wait_for_code(timeout=timeout)

            # Anchored before the exchanges rather than after: the absolute wall is a policy on
            # the *session*, and dating it from after a slow chain would overstate its remaining
            # life by however long the chain took.
            minted_at = datetime.now(timezone.utc)
            exchanged = exchange_code_for_tokens(
                self._client, self._config, code=code, verifier=verifier
            )
            control_plane = exchange_id_token_for_cp_token(
                self._client,
                self._config,
                id_token=exchanged.id_token,
                org_resource_id=org_resource_id,
            )
            if control_plane.organization_resource_id is None:
                raise OperationalError(
                    "Confluent Cloud did not report an organization for this login, so there is "
                    "no organization to scope this connection to."
                )
            data_plane = exchange_cp_for_dp_token(
                self._client, self._config, cp_token=control_plane.token
            )

            with self._token_lock:
                self._token_set = TokenSet(
                    refresh_token=exchanged.refresh_token,
                    refresh_token_expires_at=minted_at + ABSOLUTE_LIFETIME,
                    cp_token=control_plane.token,
                    cp_token_expires_at=control_plane.expires_at,
                    dp_token=data_plane.token,
                    dp_token_expires_at=data_plane.expires_at,
                )
                self._organization_id = control_plane.organization_resource_id
                self._failure = None

        logger.info(
            "Signed in to Confluent Cloud for organization "
            f"{control_plane.organization_resource_id}"
        )

    def close(self) -> None:
        """Release the private HTTP client. Idempotent.

        Does not invalidate the tokens -- there is nothing process-local to tear down beyond the
        client, and #157's daemon is what will give this method more to do.
        """
        if not self._client.is_closed:
            self._client.close()

    def _authorize_url(self, challenge: str, state: str) -> str:
        """The URL the browser is sent to.

        The seven parameters are pinned against both prior-art implementations (mcp-confluent's
        `buildAuthorizationUrl`, ide-sidecar's `getSignInUri`). Notably absent: `audience` --
        neither sends one, and the `/api/sessions` hop, not the auth service, is what scopes this
        credential to Confluent Cloud.

        `redirect_uri` comes from config rather than from the listener's actually-bound port: the
        auth service matches it against the client registration's whitelist, so it has to be the
        registered value exactly.
        """
        params = {
            "client_id": self._config.client_id,
            "response_type": "code",
            "redirect_uri": self._config.redirect_uri,
            "scope": " ".join(self._config.scopes),
            "code_challenge": challenge,
            "code_challenge_method": "S256",
            "state": state,
        }
        return f"{self._config.authorize_url}?{urllib.parse.urlencode(params)}"

    def _current_snapshot(self) -> TokenSet:
        """Read the current snapshot and the failure flag in one critical section.

        Both are read together on purpose: a caller that checked them separately could observe a
        snapshot from before a failure was latched and go on to use tokens already known dead.
        """
        with self._token_lock:
            snapshot, failure = self._token_set, self._failure
        if failure is not None:
            message, reason = failure
            # Built fresh per read rather than stored: re-raising one shared instance would grow
            # its traceback on every raise and leak one caller's frames into another's.
            raise ReauthenticationRequired(message, reason)
        if snapshot is None:
            raise ProgrammingError(
                "This CCloudOAuth provider has no tokens yet -- it must log in before it can "
                "authenticate a request."
            )
        return snapshot

    def _refresh(self, stale: TokenSet) -> TokenSet:
        """Mint a fresh token set, through the single-flight gate. Returns the current snapshot.

        `stale` is the snapshot the caller found wanting. Inside the gate it is double-checked
        against the slot: if it no longer matches, another thread refreshed while this one
        waited, and that result is returned rather than a second chain being run.

        Note what does *not* depend on that check: the refresh token actually spent is read from
        the slot below, not from `stale`, so a waiter can never re-spend a token the winner
        already rotated away no matter what it arrived holding. The double-check is what keeps N
        waiters from each running a redundant four-call chain -- real cost against the service's
        ~50-refresh cap, but not the lockout the single-use rule threatens.
        """
        with self._refresh_lock:
            current = self._token_set
            if current is None:
                raise ProgrammingError(
                    "This CCloudOAuth provider has no tokens to refresh -- it must log in first."
                )
            if current is not stale:
                # Somebody else already ran the chain while this thread waited its turn.
                return current
            if self._failure is not None:
                message, reason = self._failure
                raise ReauthenticationRequired(message, reason)

            if not current.refresh_token_valid(datetime.now(timezone.utc)):
                # Known dead locally. Spending a round trip to be told so is pure latency on an
                # error path, and the answer cannot come back any other way.
                raise self._latch_failure(
                    "This Confluent Cloud login has passed its maximum session lifetime and can "
                    "no longer be refreshed. Sign in again to continue.",
                    ReauthenticationReason.ABSOLUTE_EXPIRY,
                )

            try:
                exchanged = exchange_refresh_token(
                    self._client, self._config, refresh_token=current.refresh_token
                )
            except OAuthTokenEndpointError as e:
                if e.error_code != _INVALID_GRANT:
                    # Anything else -- a 429, a 5xx, an unclassified body -- is treated as a
                    # blip. It propagates to this request's caller but leaves the session
                    # intact, so the next request tries again rather than demanding a browser.
                    raise
                raise self._latch_failure(
                    "Confluent Cloud rejected this session's refresh token, so it can no longer "
                    "be refreshed -- it has expired through inactivity, been revoked, or already "
                    f"been used. Sign in again to continue. ({e})",
                    ReauthenticationReason.REFRESH_REJECTED,
                ) from e

            # Persist the rotated refresh token *before* the CP/DP legs. The one just spent is
            # already dead server-side; if a leg below fails and we still held the old value,
            # the session would be unrecoverable rather than merely one request short. The
            # interim snapshot keeps the old (stale) CP/DP tokens, which is honest -- they are
            # exactly what is still in hand.
            rotated = dataclasses.replace(current, refresh_token=exchanged.refresh_token)
            with self._token_lock:
                self._token_set = rotated

            control_plane = exchange_id_token_for_cp_token(
                self._client,
                self._config,
                id_token=exchanged.id_token,
                # The org this login settled on, never a re-resolved default: re-resolving would
                # silently move a multi-org user to a different organization mid-session.
                # Written once under `_token_lock` during `login()`, which happens-before any
                # refresh by way of this same gate.
                org_resource_id=self._organization_id,
            )
            data_plane = exchange_cp_for_dp_token(
                self._client, self._config, cp_token=control_plane.token
            )

            refreshed = TokenSet(
                refresh_token=exchanged.refresh_token,
                # The absolute wall does not move. Rotation resets the *idle* timer, but the ~8h
                # cap is a server-side policy dated from the interactive login; letting it ride
                # forward on each refresh would mean it never arrives until a request fails.
                refresh_token_expires_at=current.refresh_token_expires_at,
                cp_token=control_plane.token,
                cp_token_expires_at=control_plane.expires_at,
                dp_token=data_plane.token,
                dp_token_expires_at=data_plane.expires_at,
            )
            with self._token_lock:
                self._token_set = refreshed
            return refreshed

    def _latch_failure(
        self, message: str, reason: ReauthenticationReason
    ) -> ReauthenticationRequired:
        """Record that this session is unrecoverable, and return the exception to raise.

        Stores the *facts* rather than a built exception, and returns rather than raises, so the
        caller keeps `raise ... from e` at the throw site. Latching is what stops every
        subsequent request from queueing another doomed exchange behind the gate: once set, the
        flag is read in the same microsecond critical section as the snapshot and short-circuits
        the request path.
        """
        with self._token_lock:
            self._failure = (message, reason)
        return ReauthenticationRequired(message, reason)


class _PlaneAuth(httpx.Auth):
    """One of the provider's two `httpx.Auth` views, stamping its plane's token.

    Plain `auth_flow`, not `sync_auth_flow`. httpx's advice to override the latter is aimed at
    auth schemes that need to make requests *through the caller's own client*; the refresh this
    one may trigger runs over the provider's private client instead. What httpx does guarantee is
    that it applies no serialization of its own -- `auth_flow` runs concurrently on every caller
    thread -- which is exactly the condition the provider's two locks are written for.
    """

    def __init__(self, provider: CCloudOAuth, plane: _Plane) -> None:
        self._provider = provider
        self._plane = plane

    def auth_flow(self, request: httpx.Request) -> Generator[httpx.Request, httpx.Response, None]:
        snapshot = self._provider._current_snapshot()
        if not self._plane.token_valid(snapshot, datetime.now(timezone.utc)):
            snapshot = self._provider._refresh(snapshot)

        self._stamp(request, snapshot)
        response = yield request
        if response.status_code != httpx.codes.UNAUTHORIZED:
            return

        # A token that read as live but was refused anyway -- a revoked session, clock skew, a
        # server-side invalidation. Exactly one forced refresh and one retry: past that the
        # rejection is about something a new token will not fix, and looping would just burn
        # refresh tokens against it.
        self._stamp(request, self._provider._refresh(snapshot))
        yield request

    def _stamp(self, request: httpx.Request, snapshot: TokenSet) -> None:
        request.headers["Authorization"] = f"Bearer {self._plane.token(snapshot)}"
