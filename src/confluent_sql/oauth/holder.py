"""`ProcessOAuthHolder`: at most one interactive OAuth login per process/interpreter, shared by
every `Connection`.

#153's `CCloudOAuth` is one login and one shared `TokenSet`; this is the layer above it that makes
the login itself singular across a whole process. The motivating case is **dbt in multi-threaded
mode**, where each worker thread opens its own DB-API `Connection`: without this holder that is N
browser bounces and N refresh loops; with it, the user sees the browser **exactly once** and every
`Connection` shares the one provider it produces.

There is a single module-level holder, deliberately not a registry keyed by org or client. One
browser session is one `(user, organization)` identity, so "one login per process" and "one
identity per process" are the same statement. The holder does three things:

- **Login single-flight.** The first `acquire()` to find the slot empty becomes the *winner*: it
  marks a shared `Future` in-flight, **releases the lock**, and runs `login()` -- a multi-minute
  human round-trip. Every other `acquire()` arriving meanwhile joins that same `Future` and
  receives the winner's provider, or has the winner's login failure re-raised on its own thread.
  This is the same `Future`-as-single-flight idiom #153 uses for token refresh, lifted one level:
  one browser, never N. On failure the slot is cleared so a later `acquire()` retries with a fresh
  browser.

- **One-identity guard.** Every caller, winner or joiner, checks the org it asked for against the
  org the login settled on. A caller that *supplies* a different `organization_id` is refused with
  `InterfaceError` rather than handed a wrong-org token; one that omits it inherits whatever the
  first login established (supplied by that first caller, or resolved by Confluent Cloud as the
  user's default). The refusal fails only that caller -- the shared login is untouched.

- **Teardown.** `shutdown_all()` closes the provider and resets the holder to pristine. It is an
  escape hatch for test isolation and tidy long-lived hosts, not a mechanism the request path
  relies on.

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
from typing import TypeAlias

from ..exceptions import InterfaceError, OperationalError
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
        # Guards the two-field slot below and nothing else. Held for microseconds, never across
        # login() -- the whole point of the single-flight release.
        self._lock = threading.Lock()
        self._provider: OAuthProvider | None = None
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
            config: the environment to authenticate against. Used only by the login the *first*
                caller triggers; later callers reuse the established provider regardless.
            organization_id: the organization this caller expects. Supplying one that disagrees
                with the established login raises `InterfaceError`; omitting it inherits the
                established org. The first caller's value (or None) is what the login scopes to.
            timeout: seconds to wait for the browser round-trip, passed through to `login()`.
            provider_factory: builds the provider the winner logs in. Defaults to `CCloudOAuth`.

        Raises:
            InterfaceError: this caller named a different `organization_id` than the process's
                established login.
            OperationalError: the holder was shut down while this call's login was in flight.
            Exception: whatever `login()` raised, re-raised on every waiter of a failed login.
        """
        provider = self._obtain(config, organization_id, timeout, provider_factory)

        if organization_id is not None and organization_id != provider.organization_id:
            raise InterfaceError(
                "This process already has an interactive OAuth login for organization "
                f"{provider.organization_id!r}, and a single process supports only one OAuth "
                f"identity. This connection asked for organization {organization_id!r}; open it "
                "against the established organization, or omit organization_id to inherit it."
            )

        # All clear.
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
        
        return flight.result()  # the winner's provider, or its login exception re-raised here

    def _run_login(
        self,
        config: CCloudOAuthConfig,
        organization_id: str | None,
        timeout: float,
        provider_factory: ProviderFactory,
        flight: Future[OAuthProvider],
    ) -> OAuthProvider:
        """Run the one browser login and publish its outcome into `flight` for the joiners."""
        logger.info("Starting the process-wide Confluent Cloud OAuth login")
        provider = provider_factory(config)
        try:
            provider.login(organization_id, timeout=timeout)
        except BaseException as e:
            # Discard the half-built provider and clear the slot so the *next* acquire() gets a
            # fresh browser rather than inheriting this failure. Joiners waiting on the flight
            # receive this same exception re-raised on their own threads.
            provider.close()
            self._clear_flight(flight)
            flight.set_exception(e)
            raise

        with self._lock:
            superseded = self._inflight is not flight
            if not superseded:
                self._provider = provider
                self._inflight = None
        if superseded:
            # shutdown() cleared the slot while we were still logging in. Discard rather than
            # install into a torn-down holder; the caller and any joiners get a clear error.
            provider.close()
            torn_down = OperationalError(
                "The process OAuth holder was shut down while its login was still in progress."
            )
            flight.set_exception(torn_down)
            raise torn_down

        flight.set_result(provider)
        return provider

    def _clear_flight(self, flight: Future[OAuthProvider]) -> None:
        """Drop `flight` from the in-flight slot if it is still the current one."""
        with self._lock:
            if self._inflight is flight:
                self._inflight = None

    def shutdown(self) -> None:
        """Close the shared provider and reset the holder to pristine. Idempotent.

        A login in flight when this runs is left to finish and then discard its provider (it finds
        the slot cleared); this is intended for a quiescent teardown, not to abort an active login.
        """
        with self._lock:
            provider, self._provider = self._provider, None
            self._inflight = None
        if provider is not None:
            provider.close()
