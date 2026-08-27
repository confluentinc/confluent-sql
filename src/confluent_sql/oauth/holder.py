"""`ProcessOAuthHolder`: at most one interactive OAuth login per process/interpreter, shared by
every `Connection`.

#153's `CCloudOAuth` is one login and one shared `TokenSet`; this is the layer above it that makes
the login itself singular across a whole process. The motivating case is **dbt in multi-threaded
mode**, where each worker thread opens its own DB-API `Connection`: without this holder that is N
browser bounces and N independently-refreshing providers; with it, the user sees the browser
**exactly once** and every `Connection` shares the one provider it produces.

There is a single module-level holder, deliberately not a registry keyed by org or client. One
browser session is one `(user, organization)` identity, so "one login per process" and "one
identity per process" are the same statement. The holder does three things:

- **Login single-flight.** The first `acquire()` to find the slot empty becomes the *winner*: it
  marks a shared `Future` in-flight, **releases the lock**, and runs `login()` -- a multi-minute
  human round-trip. Every other `acquire()` arriving meanwhile joins that same `Future` and
  receives the winner's provider, has the winner's login failure re-raised on its own thread, or
  gives up if its *own* `timeout` elapses first. This is the same `Future`-as-single-flight idiom
  #153 uses for token refresh, lifted one level: one browser, never N. On failure the slot is
  cleared so a later `acquire()` retries with a fresh browser.

- **One-identity guard.** Every caller, winner or joiner, checks the `(environment, organization)`
  it asked for against what the login settled on. A caller naming a different environment
  (`config`) or a different `organization_id` is refused with `InterfaceError` rather than handed
  tokens minted for the wrong Confluent Cloud or the wrong org; one that omits the org inherits
  whatever the first login established (supplied by that first caller, or resolved by Confluent
  Cloud as the user's default). The refusal fails only that caller -- the shared login is
  untouched.

- **Teardown.** `shutdown_all()` retires the established provider and resets the holder. It leaves
  a login that is *in flight* alone -- with a fixed callback port only one login may run at a time,
  so it is left to settle rather than aborted mid-browser while a competitor starts. An escape
  hatch for test isolation and tidy long-lived hosts, not a mechanism the request path relies on.

**The module lock is never held across `login()`.** It guards only the slot and the in-flight
`Future` -- microseconds each -- so a waiter, a teardown, or a second `acquire()` never queues
behind a human at a browser.

**Deferred to #157:** the refresh daemon and the refcount + park-don't-evict + linger lifecycle.
Without a daemon there is nothing to *park* at refcount 0, so this baseline holder simply keeps the
shared provider alive until `shutdown_all()` or interpreter exit. `Connection.close()`'s
`holder.release()` refcount hook arrives with that child; here a `Connection` closing is a no-op on
the holder.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from concurrent.futures import Future
from concurrent.futures import TimeoutError as FuturesTimeoutError
from typing import TypeAlias

from ..exceptions import InterfaceError, OAuthLoginError, OAuthLoginFailure
from .callback_server import DEFAULT_LOGIN_TIMEOUT_SECS
from .config import CCloudOAuthConfig
from .provider import CCloudOAuth, OAuthProvider

logger = logging.getLogger(__name__)

ProviderFactory: TypeAlias = Callable[[CCloudOAuthConfig], OAuthProvider]
"""How the holder builds the one provider it will share. Defaults to `CCloudOAuth` itself;
exists so a test can inject a provider whose `login()` it controls without a real browser."""


def acquire(
    config: CCloudOAuthConfig,
    organization_id: str | None = None,
    *,
    timeout: float = DEFAULT_LOGIN_TIMEOUT_SECS,
    provider_factory: ProviderFactory = CCloudOAuth,
) -> OAuthProvider:
    """Return the process's shared OAuth provider, running the browser login if needed.

    Module-level convenience over `ProcessOAuthHolder.instance().acquire(...)`, and the peer of
    `shutdown_all()` -- so the everyday API is two free functions, not a singleton dance. See
    `ProcessOAuthHolder.acquire` for the full contract (single-flight login, the one-identity
    guard, and what it raises).
    """
    return ProcessOAuthHolder.instance().acquire(
        config, organization_id, timeout=timeout, provider_factory=provider_factory
    )


def shutdown_all() -> None:
    """Tear down the process's shared OAuth login, if any.

    The module-level escape hatch #154's design calls out: for test isolation between cases, and
    for a long-lived host that wants to drop the credential explicitly. Idempotent. The teardown
    peer of `acquire()`.
    """
    ProcessOAuthHolder.instance().shutdown()


def release() -> None:
    """The `Connection.close()` counterpart to `acquire()` (#155). No-op today.

    Deferred to #157: the refcount-driven park lifecycle. Until that lands, the holder keeps the
    shared provider alive regardless of how many Connections have released it -- see the module
    docstring. Exists now so #155's `Connection.close()` has a stable call target that won't need
    to change shape when #157 adds real teeth to it.
    """
    ProcessOAuthHolder.instance().release()


class ProcessOAuthHolder:
    """The process-wide owner of one `CCloudOAuth` login.

    Reach it through `ProcessOAuthHolder.instance()`; there is one per process. Call `acquire()` to
    obtain the shared, logged-in provider (running the browser login the first time), and
    `shutdown()` to tear it down. Most callers use the module-level `acquire()` / `shutdown_all()`
    free functions instead, which delegate here.
    """

    _instance: ProcessOAuthHolder | None = None
    _instance_lock = threading.Lock()

    @classmethod
    def instance(cls) -> ProcessOAuthHolder:
        """The one holder for this process, created on first use.

        The singleton object itself is never torn down -- `shutdown()` resets its *state*, not its
        identity -- so a reference taken once stays valid for the process's life.
        """
        existing = cls._instance
        if existing is not None:
            return existing
        with cls._instance_lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

    def __init__(self) -> None:
        # Guards the slot below and nothing else. Held for microseconds, never across login() --
        # the whole point of the single-flight release.
        self._lock = threading.Lock()
        self._provider: OAuthProvider | None = None
        # The config the established login was run against, kept so a later acquire() naming a
        # *different* environment is refused rather than silently handed wrong-environment tokens.
        self._config: CCloudOAuthConfig | None = None
        self._inflight: Future[OAuthProvider] | None = None

    def acquire(
        self,
        config: CCloudOAuthConfig,
        organization_id: str | None = None,
        *,
        timeout: float = DEFAULT_LOGIN_TIMEOUT_SECS,
        provider_factory: ProviderFactory = CCloudOAuth,
    ) -> OAuthProvider:
        """Return the process's shared OAuth provider, running the browser login if needed.

        The first caller runs the login and every later one shares its result. Blocks for the
        login's duration only on the very first call (or the first after a teardown / a prior
        login failure); afterward it returns immediately.

        Args:
            config: the environment to authenticate against. The *first* caller's config is what
                the login runs against; a later caller naming a different environment is refused
                (see Raises), and one naming the same environment reuses the established provider.
            organization_id: the organization this caller expects. Supplying one that disagrees
                with the established login raises `InterfaceError`; omitting it inherits the
                established org. The first caller's value (or None) is what the login scopes to.
            timeout: seconds this caller will wait for the login -- the browser round-trip if it
                runs the login, or the in-progress shared login if it joins one. Its own timeout
                either way, so a brief-timeout caller is never bound to another's longer deadline.
            provider_factory: builds the provider the winner logs in. Defaults to `CCloudOAuth`.

        Raises:
            InterfaceError: this caller named a different Confluent Cloud environment (`config`)
                or `organization_id` than the process's established login.
            OAuthLoginError: this caller's `timeout` elapsed while it waited on another's
                in-progress login (`reason=TIMED_OUT`).
            Exception: whatever `login()` raised, re-raised on every waiter of a failed login.
        """
        provider = self._obtain(config, organization_id, timeout, provider_factory)

        # One-identity guard, applied after obtaining the provider by whichever path (reuse / join
        # / win). The process holds a single (environment, organization) identity; a caller that
        # disagrees on either axis is refused -- failing only that caller, never disturbing the
        # shared login. The environment check comes first: a config mismatch means a wholly
        # different issuer/API host/client, so returning the established provider would hand this
        # connection tokens minted for the wrong Confluent Cloud.
        with self._lock:
            established_config = self._config
        if established_config is not None and config != established_config:
            raise InterfaceError(
                "This process already has an interactive OAuth login against Confluent Cloud "
                f"environment {established_config.api_host!r}, and a single process supports only "
                f"one OAuth identity. This connection asked for {config.api_host!r}; open it "
                "against the established environment, or tear the login down with shutdown_all() "
                "first."
            )
        if organization_id is not None and organization_id != provider.organization_id:
            raise InterfaceError(
                "This process already has an interactive OAuth login for organization "
                f"{provider.organization_id!r}, and a single process supports only one OAuth "
                f"identity. This connection asked for organization {organization_id!r}; open it "
                "against the established organization, or omit organization_id to inherit it."
            )

        return provider

    def _obtain(
        self,
        config: CCloudOAuthConfig,
        organization_id: str | None,
        timeout: float,
        provider_factory: ProviderFactory,
    ) -> OAuthProvider:
        """Resolve the shared provider: reuse it, join an in-flight login, or become its winner."""
        with self._lock:
            if self._provider is not None:
                # Already logged in and established; return it immediately.
                return self._provider
            if self._inflight is not None:
                # Another thread is already logging in; join it and wait for its result.
                flight, is_winner = self._inflight, False
            else:
                # No login in flight; this thread becomes the winner and runs it.
                flight = self._inflight = Future()
                is_winner = True

        # If I'm the winner, run the login and publish its outcome into `flight` for the joiners.
        # If I'm a joiner, skip this and wait for the winner to finish and return its provider
        # (or re-raise its login exception on my own thread).
        if is_winner:
            return self._run_login(config, organization_id, timeout, provider_factory, flight)

        # Joiner: wait for the winner's login, but no longer than *this* caller's own timeout, so a
        # caller that asked to wait only briefly is not silently bound to the winner's (possibly
        # longer) deadline. The winner's flight is always settled within its own login timeout, so
        # this only actually fires when the joiner's timeout is the shorter of the two.
        try:
            return flight.result(timeout=timeout)  # winner's provider, or its exception re-raised
        except FuturesTimeoutError as e:
            raise OAuthLoginError(
                "Timed out waiting for this process's in-progress Confluent Cloud login to "
                "complete. Another connection is running the browser login; retry, or allow a "
                "longer timeout.",
                OAuthLoginFailure.TIMED_OUT,
            ) from e

    def _run_login(
        self,
        config: CCloudOAuthConfig,
        organization_id: str | None,
        timeout: float,
        provider_factory: ProviderFactory,
        flight: Future[OAuthProvider],
    ) -> OAuthProvider:
        """Run the one browser login and publish its outcome into `flight` for the joiners.

        Whatever goes wrong -- the factory raising, `login()` failing, even cleanup itself throwing
        -- `flight` is always settled and `_inflight` always cleared before this returns or raises.
        A joiner blocked on `flight.result()`, and any later caller that would join the same slot,
        must never be stranded on an abandoned Future.
        """
        logger.info("Starting the process-wide Confluent Cloud OAuth login")
        provider: OAuthProvider | None = None
        try:
            # Both inside the try: a `provider_factory` that raises would otherwise leave `flight`
            # unsettled and `_inflight` populated, wedging every joiner and later caller forever.
            provider = provider_factory(config)
            provider.login(organization_id, timeout=timeout)
        except BaseException as e:
            # Discard the half-built provider and clear the slot so the *next* acquire() gets a
            # fresh browser rather than inheriting this failure. Joiners waiting on the flight
            # receive this same exception re-raised on their own threads.
            self._fail_flight(flight, e, close=provider)
            raise

        # No teardown race to check for: `shutdown()` deliberately leaves an in-flight `_inflight`
        # alone (a login holds the fixed callback port, so a second must never start alongside it),
        # so this flight is still current and installs normally.
        with self._lock:
            self._provider = provider
            self._config = config
            self._inflight = None
        flight.set_result(provider)
        return provider

    def _fail_flight(
        self, flight: Future[OAuthProvider], exc: BaseException, *, close: OAuthProvider | None
    ) -> None:
        """Settle `flight` with `exc`, clear the slot, then best-effort close `close`.

        The flight is settled and unslotted *before* the close, so a cleanup that itself throws
        cannot leave a joiner or a later caller blocked on an abandoned Future -- the permanent
        wedge a failed refresh/login would otherwise cause. Closing is best-effort: its own error
        is logged, never allowed to mask `exc` or skip the settling above.
        """
        self._clear_flight(flight)
        if not flight.done():
            flight.set_exception(exc)
        if close is not None:
            try:
                close.close()
            except Exception:
                logger.exception("Error while closing the discarded OAuth provider")

    def _clear_flight(self, flight: Future[OAuthProvider]) -> None:
        """Drop `flight` from the in-flight slot if it is still the current one."""
        with self._lock:
            if self._inflight is flight:
                self._inflight = None

    def release(self) -> None:
        """No-op -- see the module docstring's "Deferred to #157" note and `release()` above.

        Not `shutdown()`: this must never tear down the shared provider, since other Connections
        in the process may still be using it. #157 turns this into the real refcount decrement /
        park hook.
        """
        return None

    def shutdown(self) -> None:
        """Retire the established shared provider and reset the holder toward pristine. Idempotent.

        Deliberately does **not** clear a login that is *in flight*. With a fixed callback port
        only one login can run at a time, so freeing the slot would let a concurrent `acquire()`
        start a second, colliding login -- a `PORT_IN_USE`, or a stray second browser. An in-flight
        login is instead left to settle and install normally, and concurrent acquires join it;
        `shutdown()` retires an already-established provider, it does not abort an in-progress
        login. Intended for test isolation and tidy teardown, not a mechanism the request path
        relies on.
        """
        with self._lock:
            provider, self._provider = self._provider, None
            self._config = None
        if provider is not None:
            provider.close()
