"""`CCloudOAuth`: one interactive login, one shared `TokenSet`, two `httpx.Auth` views over it.

This is the orchestrator that turns #151's pure primitives and #152's callback server into a
working credential. It owns everything stateful: the current token snapshot, the locks guarding
it, the private `httpx.Client` the token chain runs over, and the organization the login settled
on. It is deliberately **not** itself an `httpx.Auth` -- that split is what lets one login feed
two clients carrying two *different* tokens (`control_plane_auth` for Tableflow / Connect / CMK,
`data_plane_auth` for Flink) instead of forcing a provider per surface.

**Refresh here is synchronous and on-request.** There is no
background thread in this child (#157 adds one purely to keep the ~4-round-trip refresh latency
off the hot path). Correctness deliberately does not depend on that thread ever existing: every
request checks its token's validity, and a stale one is refreshed through the single-flight gate
before the header is stamped. A connection that sat idle for an hour re-mints from the long-lived
refresh token on its next request, however long the short-lived (~5-min) tokens have been lapsed.

**The refresh single-flight is a shared `Future`, not a lock held across the chain.** One thread
wins the right to run the refresh and publishes its outcome into the `Future`; everyone who
arrives while it is in flight joins that same `Future` and receives the winner's result -- or has
the winner's exception re-raised on their own thread. Nothing is held across network I/O.

That shape is the point, and it is the one #154 also mandates for the login single-flight. A lock
only says *"you may proceed"*: a thread waking from one holds no handle on what the winner did and
has to reconstruct it by re-reading shared mutable state. Reconstruction is where this went wrong
twice -- a mid-chain checkpoint could be misread as somebody's finished work (see
`_interim_snapshot`), and a *failed* attempt taught the waiters nothing, so eight waiters against
one outage ran eight chains and spent eight rotations against the service's ~50-refresh cap. A
`Future` carries success, failure, and in-flight-ness in one object, so waiters learn all three.

Two locks remain, both short, always acquired in this order and never the reverse:

- **`_refresh_lock`, held for microseconds.** Guards the in-flight `Future` slot -- who wins, who
  joins -- and nothing else. Explicitly *not* held across the chain.
- **`_token_lock`, held for microseconds.** Guards the snapshot reference, the interim marker, the
  failure flag, and the resolved organization id. Nothing but read-the-reference and
  rebind-the-reference happens under it.
  Because a `TokenSet` is immutable, a reader that has copied the reference can then read its
  fields with no lock at all and no risk of a torn read.

`login()` takes its own `_login_lock`, since it *does* hold across a multi-minute browser
round-trip and must never sit on a lock the request path needs.

**Refresh tokens are single-use and rotating**, and two threads spending the same one leaves the
second refused and the session dead. What makes that impossible is that the chain reads the token
to spend out of the slot at the moment it runs, never out of the snapshot its caller arrived
with -- so whatever a waiter was holding when it queued, the chain spends the current token or
none. The `Future` is what keeps waiters from running redundant chains at all.

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
from concurrent.futures import Future
from concurrent.futures import TimeoutError as FuturesTimeoutError
from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING, Protocol

import httpx

from ..exceptions import (
    OAuthLoginError,
    OAuthLoginFailure,
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


class OAuthProvider(Protocol):
    """The surface a logged-in provider presents to its consumers -- what a `Connection` needs.

    `CCloudOAuth` is the one implementation outside of the test suite.
    """

    @property
    def organization_id(self) -> str | None: ...

    @property
    def control_plane_auth(self) -> httpx.Auth: ...

    @property
    def data_plane_auth(self) -> httpx.Auth: ...

    def login(self, org_resource_id: str | None = ..., *, timeout: float = ...) -> None: ...

    def reauthenticate(self, *, timeout: float = ...) -> None: ...

    def close(self) -> None: ...


class CCloudOAuth:
    """Owns one Confluent Cloud interactive login and the tokens it mints.

    Drive it with `login()`, then hand `data_plane_auth` / `control_plane_auth` to the clients
    that need them. `close()` releases the private HTTP client.

    One provider is one identity: one `(user, organization)` pair, established by `login()` and
    fixed for the provider's life. Sharing a single provider process-wide -- so that N
    `Connection`s trigger one browser bounce rather than N -- is #154's holder; this class knows
    nothing about that and is usable standalone.

    Satisfies `OAuthProvider` structurally (see `_CCLOUDOAUTH_CONFORMS`).
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
        """What Confluent Cloud environment to authenticate against, and the callback host/port/path
           to listen on."""
        self._client = http_client if http_client is not None else httpx.Client()
        self._open_browser = open_browser
        """How to send the user to the authorization URL."""

        self._login_lock = threading.Lock()
        """Guards the potentially multi-minute browser round-trip in `login()` and its I/O"""
        self._refresh_lock = threading.Lock()
        """Guards _inflight_refresh: the slot holding the `Future` for a refresh chain -- the
           four-hop token exchange -- or a reauthenticate() login, currently running, or None
           when none is. Held for microseconds; never held across the chain/login itself."""
        self._token_lock = threading.Lock()
        """Guards _token_set, _interim_snapshot, _failure, and _organization_id.
           Held for microseconds."""

        # _login_lock and _refresh_lock are never held at once -- not an ordering, a straight
        # mutual exclusion enforced by the data invariant above (login only runs with no token
        # set; refresh and reauthenticate() only with one, and share _inflight_refresh so they
        # can never race each other to install _token_set -- see reauthenticate()'s docstring).
        # _token_lock is the one that nests inside either: under _login_lock in login(), and
        # jointly with _refresh_lock in _enter_flight()/_enter_reauth_flight(). Only _login_lock
        # is ever held across I/O.

        self._inflight_refresh: Future[TokenSet] | None = None
        """The single-flight `Future` slot, shared by `_refresh()` and `reauthenticate()`. One
        thread wins the right to run the refresh chain (or the reauthenticate() login) and
        publishes its outcome into this `Future`; any other that arrives while it is in flight
        joins the same `Future` and receives the winner's result."""

        self._token_set: TokenSet | None = None
        """The current immutable Token snapshot, or None before `login()`. Only read
        under _token_lock."""

        self._interim_snapshot: TokenSet | None = None
        """A mid-chain checkpoint, published before the CP/DP legs. Only read under _token_lock.
        Distinguishes a refresh in progress from a finished one, so a waiter can tell the
        difference between a snapshot that is already superseded by a completed refresh and one
        that is still in flight."""

        self._failure: tuple[str, ReauthenticationReason] | None = None
        """Set when refresh has permanently failed and re-authentication is required -- latched
        by `_latch_failure`, never by `login()` itself, which only clears it on success."""

        self._organization_id: str | None = None
        """The `organization.resource_id` this login settled on -- whichever was supplied to
        `login()`, or the default Confluent Cloud resolved for the user. None before `login()`.
        Only read under _token_lock."""

        self._control_plane_auth = _PlaneAuth(self, _Plane.CONTROL)
        """The view stamping the control-plane token -- Tableflow, Connect, CMK, org lookups."""
        self._data_plane_auth = _PlaneAuth(self, _Plane.DATA)
        """The view stamping the data-plane token -- the Flink SQL gateway."""

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
        # A dedicated lock, deliberately not the refresh gate: this one is held across a
        # multi-minute human round-trip, and the request path must never queue behind that. The
        # two cannot overlap anyway -- login runs only when there is no token set, refresh only
        # when there is. #156's reauthenticate() is what runs a second login-shaped flow *with* a
        # token set already present -- it routes through the refresh gate instead (see
        # reauthenticate()), never through this lock, so the invariant above still holds for this
        # method specifically.
        with self._login_lock:
            if self._token_set is not None:
                raise ProgrammingError(
                    "This CCloudOAuth provider has already logged in. One provider is one "
                    "identity for its whole life; use a new provider for a different one."
                )

            token_set, organization_id = self._run_login_flow(org_resource_id, timeout)

            with self._token_lock:
                self._token_set = token_set
                self._organization_id = organization_id
                self._interim_snapshot = None
                self._failure = None

        logger.info(f"Signed in to Confluent Cloud for organization {organization_id}")

    def reauthenticate(self, *, timeout: float = DEFAULT_LOGIN_TIMEOUT_SECS) -> None:
        """Recover a session that can no longer be refreshed by running a fresh interactive login.

        For use once a request has raised `ReauthenticationRequired` (the refresh token is
        idle-expired, revoked, already spent, or past its ~8h absolute lifetime): this re-runs the
        full browser login -- a new authorization code, a new three-hop token exchange -- against
        the organization this provider originally settled on, and installs the result as the
        current snapshot, clearing the latched failure and resetting the 8h wall.

        Routed through the *same* single-flight gate `_refresh()` uses (`_inflight_refresh` /
        `_refresh_lock`), not an independent one: see the module docstring and
        `_enter_reauth_flight` for why sharing it is what keeps a concurrent plain refresh from
        racing a reauthentication to install `_token_set`. N callers hitting the wall together
        collapse into one browser bounce -- the first becomes the flight winner and runs the
        login; the rest join its `Future` and share the outcome, or have its exception (a denied
        login, a timed-out browser round-trip, a failed token exchange) re-raised on their own
        thread.

        Args:
            timeout: seconds to wait for the browser round-trip (the winner), or for the winner's
                login to finish (a joiner) -- its own timeout either way, so a brief-timeout
                caller is never bound to another's longer deadline.

        Raises `ProgrammingError` if this provider has never logged in, `OAuthLoginError` if the
        browser leg fails or a joiner's own timeout elapses first, and `OperationalError` if a
        token exchange fails.
        """
        flight, is_winner = self._enter_reauth_flight()
        if not is_winner:
            try:
                flight.result(timeout=timeout)
            except FuturesTimeoutError as e:
                raise OAuthLoginError(
                    "Timed out waiting for this process's in-progress Confluent Cloud "
                    "re-authentication to complete. Retry, or allow a longer timeout.",
                    OAuthLoginFailure.TIMED_OUT,
                ) from e
            return

        try:
            token_set, organization_id = self._run_login_flow(self._organization_id, timeout)
        except BaseException as e:
            flight.set_exception(e)
            raise
        with self._token_lock:
            self._token_set = token_set
            self._organization_id = organization_id
            self._interim_snapshot = None
            self._failure = None
        flight.set_result(token_set)
        with self._refresh_lock:
            self._inflight_refresh = None

        logger.info(f"Re-authenticated to Confluent Cloud for organization {organization_id}")

    def _enter_reauth_flight(self) -> tuple[Future[TokenSet], bool]:
        """Decide whether this caller runs the fresh login or joins one already running.

        Shares `_refresh()`'s `_inflight_refresh` slot under `_refresh_lock`, but -- unlike
        `_enter_flight` -- neither raises on a latched `self._failure` (clearing it is the whole
        point of `reauthenticate()`) nor double-checks for an already-superseded snapshot (there
        is no `stale` reference to compare against; a caller that explicitly asked to
        reauthenticate gets a reauthentication, or joins one already in flight).
        """
        with self._refresh_lock:
            if self._token_set is None:
                raise ProgrammingError(
                    "This CCloudOAuth provider has no tokens yet -- it must log in before it can "
                    "be re-authenticated."
                )
            if self._inflight_refresh is not None:
                return self._inflight_refresh, False
            self._inflight_refresh = Future()
            return self._inflight_refresh, True

    def _run_login_flow(self, org_resource_id: str | None, timeout: float) -> tuple[TokenSet, str]:
        """The browser round-trip and the three token hops, shared by `login()` and
        `reauthenticate()`. Touches no shared state -- the caller installs the result under
        `_token_lock` itself, since the two callers install it differently (`login()` also clears
        `_interim_snapshot`/`_failure` under `_login_lock`; `reauthenticate()` under the refresh
        gate).
        """
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

        token_set = TokenSet(
            refresh_token=exchanged.refresh_token,
            refresh_token_expires_at=minted_at + ABSOLUTE_LIFETIME,
            cp_token=control_plane.token,
            cp_token_expires_at=control_plane.expires_at,
            dp_token=data_plane.token,
            dp_token_expires_at=data_plane.expires_at,
        )
        return token_set, control_plane.organization_resource_id

    def close(self) -> None:
        """Release the private HTTP client. Idempotent.

        Does not invalidate the tokens -- there is nothing process-local to tear down beyond the
        client, and #157's daemon is what will give this method more to do.

        **Not safe to call while another thread may still be using this provider.** `login()` and
        `_run_refresh_chain()` run their HTTP calls over `_client` with no lock held across that
        I/O, by design; closing `_client` out from under one of them surfaces as a confusing
        transport error on that thread rather than a clean one. Nothing here coordinates with
        in-flight use -- #157's refcount + park-don't-evict lifecycle is what will make `close()`
        safe to call from a live holder. Until then, callers must ensure the provider has no other
        active users first.
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
        """Mint a fresh token set, through the single-flight `Future`. Returns a live snapshot.

        `stale` is the snapshot the caller found wanting. Exactly one caller runs the chain; any
        other that arrives while it is in flight joins the same `Future` and receives the
        winner's snapshot, or has the winner's exception re-raised on its own thread. So N
        threads meeting a lapsed token produce one chain run, one refresh-token rotation, and --
        when it fails -- one failure rather than N independent attempts.

        The refresh token actually spent is read from the slot at the moment the chain runs,
        never from `stale`, so a waiter can never re-spend a token the winner already rotated
        away, whatever it arrived holding.
        """
        flight, is_winner = self._enter_flight(stale)
        if not is_winner:
            # The winner's outcome verbatim, including its exception. Deliberately untimed: the
            # chain is already bounded by the HTTP client's own timeouts, and a second, arbitrary
            # deadline here would only invent a failure mode the winner never had.
            return flight.result()
        try:
            refreshed = self._run_refresh_chain()
        except BaseException as e:
            flight.set_exception(e)
            raise
        else:
            flight.set_result(refreshed)
            return refreshed
        finally:
            # Cleared unconditionally, so a *rejected* flight is never left in the slot for later
            # callers to inherit -- they must be free to try again on their own.
            with self._refresh_lock:
                self._inflight_refresh = None

    def _enter_flight(self, stale: TokenSet) -> tuple[Future[TokenSet], bool]:
        """Decide whether this caller runs the chain or joins one already running.

        Returns the `Future` to publish into (winner) or wait on (joiner). A caller whose `stale`
        snapshot has already been superseded by a *completed* refresh gets that result handed
        back in an already-resolved `Future` without any chain running at all.
        """
        with self._refresh_lock, self._token_lock:
            if self._failure is not None:
                message, reason = self._failure
                raise ReauthenticationRequired(message, reason)
            current = self._token_set
            if current is None:
                raise ProgrammingError(
                    "This CCloudOAuth provider has no tokens to refresh -- it must log in first."
                )
            if self._inflight_refresh is not None:
                return self._inflight_refresh, False
            if current is not stale and current is not self._interim_snapshot:
                # Already superseded by a finished refresh -- no chain needed. "Finished" is the
                # load-bearing word: persist-before-exchange publishes a mid-chain checkpoint into
                # this same slot, and handing that back as somebody's completed work would return
                # the very tokens this caller came to replace.
                settled: Future[TokenSet] = Future()
                settled.set_result(current)
                return settled, False
            self._inflight_refresh = Future()
            return self._inflight_refresh, True

    def _run_refresh_chain(self) -> TokenSet:
        """The four exchanges, run by the single-flight winner outside every lock."""
        with self._token_lock:
            current = self._token_set
        # Only the flight winner reaches here, and `login()` cannot be running concurrently (it
        # requires an empty slot), so the snapshot just read is stable for the chain's duration.
        assert current is not None

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
                # Anything else -- a 429, a 5xx, an unclassified body -- is treated as a blip. It
                # propagates to this flight's callers but leaves the session intact, so the next
                # request tries again rather than demanding a browser.
                raise
            raise self._latch_failure(
                "Confluent Cloud rejected this session's refresh token, so it can no longer "
                "be refreshed -- it has expired through inactivity, been revoked, or already "
                f"been used. Sign in again to continue. ({e})",
                ReauthenticationReason.REFRESH_REJECTED,
            ) from e

        # Persist the rotated refresh token *before* the CP/DP legs. The one just spent is
        # already dead server-side; if a leg below fails and we still held the old value, the
        # session would be unrecoverable rather than merely one request short. The interim
        # snapshot keeps the old (stale) CP/DP tokens, which is honest -- they are exactly what
        # is still in hand.
        rotated = dataclasses.replace(current, refresh_token=exchanged.refresh_token)
        with self._token_lock:
            self._token_set = rotated
            # Remembered so `_enter_flight` can tell this checkpoint apart from a finished
            # refresh. It carries a *new* refresh token but the *old* CP/DP tokens, so a caller
            # handed it would send a token it already knew was dead -- and on the 401 path would
            # re-stamp the very bearer just rejected, spend its one retry, and surface a second
            # 401 with a usable refresh token sitting right here.
            self._interim_snapshot = rotated
            organization_id = self._organization_id

        control_plane = exchange_id_token_for_cp_token(
            self._client,
            self._config,
            id_token=exchanged.id_token,
            # The org this login settled on, never a re-resolved default: re-resolving would
            # silently move a multi-org user to a different organization mid-session.
            org_resource_id=organization_id,
        )
        data_plane = exchange_cp_for_dp_token(
            self._client, self._config, cp_token=control_plane.token
        )

        refreshed = TokenSet(
            refresh_token=exchanged.refresh_token,
            # The absolute wall does not move. Rotation resets the *idle* timer, but the ~8h cap
            # is a server-side policy dated from the interactive login; letting it ride forward
            # on each refresh would mean it never arrives until a request fails.
            refresh_token_expires_at=current.refresh_token_expires_at,
            cp_token=control_plane.token,
            cp_token_expires_at=control_plane.expires_at,
            dp_token=data_plane.token,
            dp_token_expires_at=data_plane.expires_at,
        )
        with self._token_lock:
            self._token_set = refreshed
            self._interim_snapshot = None
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


if TYPE_CHECKING:
    _CCLOUDOAUTH_CONFORMS: type[OAuthProvider] = CCloudOAuth
    """Static assertion that `CCloudOAuth` satisfies `OAuthProvider`."""


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
