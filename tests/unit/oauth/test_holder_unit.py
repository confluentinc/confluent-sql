"""Unit tests for `ProcessOAuthHolder`, the one-identity-per-process OAuth holder (#154).

The holder's whole job is *coordination*, not login mechanics: one browser bounce shared by
every `Connection` in the process, one identity guarded across them, and a clean teardown. So
these tests drive it against a **`FakeProvider`** injected through `acquire`'s `provider_factory`
seam, never a real `CCloudOAuth`. That is deliberate, not a shortcut:

- The invariants at stake -- login runs once under a race, a failed login clears the slot, the
  module lock is not held across the login, a second organization is refused -- are properties of
  the *holder*, independent of what a real login does over the network. A fake provider whose
  `login()` we can make slow, make fail, or block mid-flight isolates exactly that logic.
- `CCloudOAuth`'s own end-to-end login (PKCE, the callback port, the token chain) is already
  pinned by `test_provider_unit.py`; re-driving it through the holder would retest #153 and add a
  real socket bind to every concurrency case here for nothing.

`FakeProvider.login()` optionally runs a caller-supplied `gate` -- an arbitrary callable -- which
is how a test makes the login block until other threads have piled onto the in-flight `Future`, or
makes it fail. Only the single-flight *winner* ever builds a provider, so a factory that records
what it built lets a test assert "exactly one provider, one login" directly.
"""

from __future__ import annotations

import dataclasses
import threading
from collections.abc import Callable
from concurrent.futures import Future

import httpx
import pytest

from confluent_sql.exceptions import InterfaceError, OAuthLoginError, OAuthLoginFailure
from confluent_sql.oauth import holder as holder_module
from confluent_sql.oauth.config import CCloudOAuthConfig
from confluent_sql.oauth.holder import ProcessOAuthHolder, acquire, shutdown_all
from confluent_sql.oauth.provider import OAuthProvider

pytestmark = pytest.mark.unit

CONFIG = CCloudOAuthConfig(
    auth_service_domain="login.confluent.io",
    api_host="https://confluent.cloud",
    client_id="test-client-id",
    callback_host="127.0.0.1",
    callback_port=26640,
    callback_path="/gateway/v1/callback-local-mcp-docs",
)

OTHER_ENV_CONFIG = CCloudOAuthConfig(
    auth_service_domain="login-stag.confluent-dev.io",
    api_host="https://stag.cpdev.cloud",
    client_id="stag-client-id",
    callback_host="127.0.0.1",
    callback_port=26640,
    callback_path="/gateway/v1/callback-local-mcp-docs",
)
"""A *different* Confluent Cloud environment -- different issuer, API host, and client -- for
pinning that the holder refuses to hand one environment's provider to a connection asking for
another."""

DEFAULT_ORG = "org-resolved-from-the-session"
"""What a `FakeProvider` reports when `login()` is handed no org -- the analogue of Confluent
Cloud resolving the user's default and reporting it back."""

BRIEF_TIMEOUT = 5.0
"""Wait bound for anything that should resolve promptly; a wedged test fails fast instead of
hanging CI."""

DENIED_LOGIN = OAuthLoginError("the user declined consent", OAuthLoginFailure.AUTHORIZATION_DENIED)
"""A representative interactive-login failure. One shared instance, so the concurrency case can
assert every waiter received the winner's *actual* exception object, not one it re-derived."""


class FakeProvider:
    """A stand-in for `CCloudOAuth` covering only the surface the holder touches.

    Implements the `OAuthProvider` protocol structurally.

    `login()` settles `organization_id` the way a real one does -- to the org it was handed, or a
    default when handed none -- and, before doing so, runs an optional `gate` the test supplies to
    block, fail, or coordinate. Everything the holder later reads (`organization_id`) or calls
    (`close()`) is recorded so a test can assert on it.
    """

    def __init__(self, config: CCloudOAuthConfig, *, gate: Callable[[], None] | None = None):
        self.config = config
        self._gate = gate
        self._organization_id: str | None = None
        self.login_calls = 0
        self.closed = False

    def login(self, org_resource_id: str | None = None, *, timeout: float = 0.0) -> None:
        self.login_calls += 1
        if self._gate is not None:
            self._gate()
        self._organization_id = org_resource_id if org_resource_id is not None else DEFAULT_ORG

    @property
    def organization_id(self) -> str | None:
        return self._organization_id

    @property
    def control_plane_auth(self) -> httpx.Auth:
        # Part of the OAuthProvider surface but never exercised here -- the holder shares the
        # provider out untouched; #155's connect() wiring is what stamps requests with these.
        raise NotImplementedError

    @property
    def data_plane_auth(self) -> httpx.Auth:
        raise NotImplementedError

    def close(self) -> None:
        self.closed = True


class RecordingFactory:
    """A `provider_factory` that records every provider it builds.

    Only the single-flight winner ever calls the factory, so `providers` is the ground truth for
    "how many logins were even attempted": a length of one is the single-flight property stated as
    a fact about construction.
    """

    def __init__(self, gate: Callable[[], None] | None = None):
        self._gate = gate
        self.providers: list[FakeProvider] = []
        self._lock = threading.Lock()

    def __call__(self, config: CCloudOAuthConfig) -> FakeProvider:
        provider = FakeProvider(config, gate=self._gate)
        with self._lock:
            self.providers.append(provider)
        return provider


@pytest.fixture(autouse=True)
def _reset_holder():
    """Every case starts and ends with a pristine process holder.

    The holder is module-global by design (one identity per process), so a provider leaked from
    one case into the next is exactly the cross-test contamination this guards -- and the reset
    also exercises `shutdown_all()` as a side effect.
    """
    shutdown_all()
    yield
    shutdown_all()


class TestSingleFlight:
    def test_concurrent_acquires_log_in_once_and_share_one_provider(self, monkeypatch):
        """N Connections opening at once -> one browser bounce, one provider for all of them.

        The login is gated shut until every waiter has piled on, so the win is the *join* path --
        not a first caller finishing before the rest even start and each then reading a stored
        provider. `login_calls == 1` and one built provider is the single-flight property; every
        thread leaving with the *same object* is the sharing property.
        """
        joiner_count = 4
        all_joined = _arm_join_barrier(monkeypatch, joiner_count)
        release = threading.Event()
        entered = threading.Event()

        def gate() -> None:
            entered.set()
            assert release.wait(timeout=BRIEF_TIMEOUT)

        factory = RecordingFactory(gate)
        holder = ProcessOAuthHolder.instance()
        results: list[OAuthProvider] = []
        errors: list[BaseException] = []
        sink = threading.Lock()

        def worker() -> None:
            try:
                provider = holder.acquire(CONFIG, provider_factory=factory)
                with sink:
                    results.append(provider)
            except BaseException as e:  # noqa: BLE001
                with sink:
                    errors.append(e)

        # Start the winner first and let it enter the gated login, so it is guaranteed to be the
        # single-flight winner and the rest must join its Future rather than start their own.
        winner = threading.Thread(target=worker)
        winner.start()
        assert entered.wait(timeout=BRIEF_TIMEOUT)

        joiners = [threading.Thread(target=worker) for _ in range(joiner_count)]
        for joiner in joiners:
            joiner.start()
        assert all_joined.wait(timeout=BRIEF_TIMEOUT)  # every joiner is now parked on the flight
        with sink:  # nothing can complete while the login is gated shut
            assert not results and not errors
        release.set()

        threads = [winner, *joiners]
        for thread in threads:
            thread.join(timeout=BRIEF_TIMEOUT)
        assert not any(thread.is_alive() for thread in threads)

        assert not errors
        assert len(factory.providers) == 1
        the_provider = factory.providers[0]
        assert the_provider.login_calls == 1
        assert len(results) == len(threads)
        assert all(result is the_provider for result in results)

    def test_a_second_acquire_after_login_reuses_the_provider_without_logging_in_again(self):
        factory = RecordingFactory()
        holder = ProcessOAuthHolder.instance()

        first = holder.acquire(CONFIG, provider_factory=factory)
        second = holder.acquire(CONFIG, provider_factory=factory)

        assert first is second
        assert len(factory.providers) == 1

    def test_a_joiner_honors_its_own_timeout_while_the_winner_is_slow(self):
        """A joiner waits on the shared login, but no longer than its *own* `timeout`.

        The winner is parked in a gated login; a joiner asking for only a brief wait must surface a
        login timeout instead of being bound to the winner's longer deadline -- and doing so must
        neither disturb the winner's in-flight login nor start a second one.
        """
        release = threading.Event()
        entered = threading.Event()

        def gate() -> None:
            entered.set()
            assert release.wait(timeout=BRIEF_TIMEOUT)

        factory = RecordingFactory(gate)
        holder = ProcessOAuthHolder.instance()

        winner = threading.Thread(
            target=lambda: holder.acquire(CONFIG, provider_factory=factory, timeout=BRIEF_TIMEOUT)
        )
        winner.start()
        assert entered.wait(timeout=BRIEF_TIMEOUT)  # winner is now inside the gated login

        # A joiner (winner already owns the flight) with a tiny timeout: it must give up quickly.
        with pytest.raises(OAuthLoginError) as caught:
            holder.acquire(CONFIG, provider_factory=factory, timeout=0.05)
        assert caught.value.reason is OAuthLoginFailure.TIMED_OUT

        # The winner's login was untouched: release it and confirm it completes as the one login.
        release.set()
        winner.join(timeout=BRIEF_TIMEOUT)
        assert not winner.is_alive()
        assert len(factory.providers) == 1
        assert factory.providers[0].login_calls == 1


class TestLoginFailure:
    def test_login_failure_clears_the_slot_so_a_later_acquire_retries(self):
        """A failed browser login must not poison the holder: the next `connect()` gets a fresh
        browser, not the first attempt's stranded failure."""
        holder = ProcessOAuthHolder.instance()

        def failing(config: CCloudOAuthConfig) -> FakeProvider:
            return FakeProvider(config, gate=_raising(DENIED_LOGIN))

        with pytest.raises(OAuthLoginError):
            holder.acquire(CONFIG, provider_factory=failing)

        # Slot cleared -> a fresh factory is consulted and a new login runs.
        retry = RecordingFactory()
        provider = holder.acquire(CONFIG, provider_factory=retry)
        assert provider is retry.providers[0]
        assert retry.providers[0].login_calls == 1

    def test_the_half_built_provider_is_closed_when_login_fails(self):
        holder = ProcessOAuthHolder.instance()
        built: list[FakeProvider] = []

        def failing(config: CCloudOAuthConfig) -> FakeProvider:
            provider = FakeProvider(config, gate=_raising(DENIED_LOGIN))
            built.append(provider)
            return provider

        with pytest.raises(OAuthLoginError):
            holder.acquire(CONFIG, provider_factory=failing)

        assert built and built[0].closed

    def test_a_provider_factory_that_raises_does_not_wedge_the_holder(self):
        """A factory that throws *before* login even starts must still settle the flight and clear
        the slot. Otherwise `_inflight` stays populated and every joiner / later caller blocks on
        an abandoned Future forever -- the wedge that comes from building the provider outside the
        guarded region.
        """
        holder = ProcessOAuthHolder.instance()

        def exploding_factory(config: CCloudOAuthConfig) -> OAuthProvider:
            raise RuntimeError("factory blew up before any login")

        with pytest.raises(RuntimeError, match="factory blew up"):
            holder.acquire(CONFIG, provider_factory=exploding_factory)

        # Not wedged: a later acquire runs a fresh login rather than blocking on the dead flight.
        # Driven on a thread so a regression fails as a live thread instead of hanging the suite.
        self._assert_acquire_completes(holder)

    def test_cleanup_that_itself_throws_still_settles_the_flight(self):
        """If discarding a failed login's provider throws from `close()`, the flight must still be
        settled and the slot cleared -- the close error is logged, never left to wedge the holder.
        """
        holder = ProcessOAuthHolder.instance()

        def factory(config: CCloudOAuthConfig) -> FakeProvider:
            provider = FakeProvider(config, gate=_raising(DENIED_LOGIN))
            provider.close = _raising(RuntimeError("close blew up"))  # type: ignore[method-assign]
            return provider

        # The login failure still surfaces (not the close error), and the holder is not wedged.
        with pytest.raises(OAuthLoginError):
            holder.acquire(CONFIG, provider_factory=factory)
        self._assert_acquire_completes(holder)

    @staticmethod
    def _assert_acquire_completes(holder: ProcessOAuthHolder) -> None:
        """Acquire on a thread and assert it returns promptly -- a wedged holder leaves it alive."""
        retry = RecordingFactory()
        got: list[OAuthProvider] = []
        thread = threading.Thread(
            target=lambda: got.append(holder.acquire(CONFIG, provider_factory=retry))
        )
        thread.start()
        thread.join(timeout=BRIEF_TIMEOUT)
        assert not thread.is_alive(), "holder wedged: acquire blocked on an abandoned flight"
        assert got and got[0] is retry.providers[0]

    def test_concurrent_waiters_share_one_failed_login(self, monkeypatch):
        """A failed login costs *one* attempt shared by every waiter, not one browser per thread.

        The winner is held inside a gate until the joiners have queued on its Future, then fails;
        the joiners must receive that same failure rather than each launching its own login.
        """
        joiner_count = 4
        all_joined = _arm_join_barrier(monkeypatch, joiner_count)
        release = threading.Event()
        entered = threading.Event()

        def gate() -> None:
            entered.set()
            assert release.wait(timeout=BRIEF_TIMEOUT)
            raise DENIED_LOGIN

        factory = RecordingFactory(gate)
        holder = ProcessOAuthHolder.instance()

        winner_error: list[BaseException] = []

        def run_winner() -> None:
            try:
                holder.acquire(CONFIG, provider_factory=factory)
            except BaseException as e:  # noqa: BLE001
                winner_error.append(e)

        winner = threading.Thread(target=run_winner)
        winner.start()
        assert entered.wait(timeout=BRIEF_TIMEOUT)

        joiner_results: list[OAuthProvider] = []
        joiner_errors: list[BaseException] = []
        sink = threading.Lock()

        def run_joiner() -> None:
            try:
                joiner_results.append(holder.acquire(CONFIG, provider_factory=factory))
            except BaseException as e:  # noqa: BLE001
                with sink:
                    joiner_errors.append(e)

        joiners = [threading.Thread(target=run_joiner) for _ in range(joiner_count)]
        for joiner in joiners:
            joiner.start()
        # Only once every joiner is provably parked on the winner's Future do we let the winner
        # fail -- otherwise a late joiner could reach the slot after the failure cleared it and
        # start a second login.
        assert all_joined.wait(timeout=BRIEF_TIMEOUT)
        release.set()

        winner.join(timeout=BRIEF_TIMEOUT)
        for joiner in joiners:
            joiner.join(timeout=BRIEF_TIMEOUT)

        assert not any(joiner.is_alive() for joiner in joiners)
        assert not joiner_results
        assert len(factory.providers) == 1  # one attempt, not one per waiter
        assert factory.providers[0].login_calls == 1
        assert len(winner_error) == 1
        assert len(joiner_errors) == joiner_count
        # Every waiter saw the winner's actual failure object, re-raised on its own thread.
        assert all(error is winner_error[0] for error in joiner_errors)


class TestIdentityGuard:
    def test_a_second_environment_is_refused(self):
        """A later acquire naming a *different* Confluent Cloud environment must be refused, not
        silently handed the first environment's provider -- whose tokens a different issuer minted
        for a different API host. The org axis is not enough; environment is guarded too."""
        factory = RecordingFactory()
        holder = ProcessOAuthHolder.instance()

        holder.acquire(CONFIG, provider_factory=factory)
        with pytest.raises(InterfaceError, match="environment"):
            holder.acquire(OTHER_ENV_CONFIG, provider_factory=factory)

        # Refusal fails only that caller; the established (CONFIG) login is intact and reusable.
        assert holder.acquire(CONFIG, provider_factory=factory) is factory.providers[0]
        assert len(factory.providers) == 1

    def test_the_same_environment_by_value_is_accepted(self):
        """The environment check is by value, not identity: a distinct but equal config matches."""
        factory = RecordingFactory()
        holder = ProcessOAuthHolder.instance()

        first = holder.acquire(CONFIG, provider_factory=factory)
        assert holder.acquire(dataclasses.replace(CONFIG), provider_factory=factory) is first
        assert len(factory.providers) == 1

    def test_a_second_organization_is_refused(self):
        factory = RecordingFactory()
        holder = ProcessOAuthHolder.instance()

        holder.acquire(CONFIG, organization_id="org-a", provider_factory=factory)
        with pytest.raises(InterfaceError, match="org-a"):
            holder.acquire(CONFIG, organization_id="org-b", provider_factory=factory)

        # The refusal fails only that caller; the established login is untouched and reusable.
        assert (
            holder.acquire(CONFIG, organization_id="org-a", provider_factory=factory)
            is (factory.providers[0])
        )
        assert len(factory.providers) == 1

    def test_an_omitted_organization_inherits_the_established_one(self):
        factory = RecordingFactory()
        holder = ProcessOAuthHolder.instance()

        holder.acquire(CONFIG, organization_id="org-a", provider_factory=factory)
        provider = holder.acquire(CONFIG, provider_factory=factory)

        assert provider.organization_id == "org-a"
        assert len(factory.providers) == 1

    def test_matching_the_discovered_default_organization_is_accepted(self):
        """A first caller that omitted org gets Confluent Cloud's default resolved onto the
        provider; a later caller naming that same value agrees, and is not refused."""
        factory = RecordingFactory()
        holder = ProcessOAuthHolder.instance()

        first = holder.acquire(CONFIG, provider_factory=factory)
        assert first.organization_id == DEFAULT_ORG
        second = holder.acquire(CONFIG, organization_id=DEFAULT_ORG, provider_factory=factory)
        assert second is first

    def test_a_joiner_wanting_a_different_org_is_refused_while_the_winner_succeeds(
        self, monkeypatch
    ):
        """The guard holds even across the single-flight race: a thread that joins a login it did
        not start, but wanted a different org than that login settled on, is refused -- without
        disturbing the winner or the shared provider.
        """
        all_joined = _arm_join_barrier(monkeypatch, 1)
        release = threading.Event()
        entered = threading.Event()

        def gate() -> None:
            entered.set()
            assert release.wait(timeout=BRIEF_TIMEOUT)

        factory = RecordingFactory(gate)
        holder = ProcessOAuthHolder.instance()

        # Winner omits org -> settles on DEFAULT_ORG.
        winner_result: list[OAuthProvider] = []
        winner = threading.Thread(
            target=lambda: winner_result.append(holder.acquire(CONFIG, provider_factory=factory))
        )
        winner.start()
        assert entered.wait(timeout=BRIEF_TIMEOUT)

        joiner_error: list[BaseException] = []

        def run_joiner() -> None:
            try:
                holder.acquire(CONFIG, organization_id="a-different-org", provider_factory=factory)
            except BaseException as e:  # noqa: BLE001
                joiner_error.append(e)

        joiner = threading.Thread(target=run_joiner)
        joiner.start()
        assert all_joined.wait(timeout=BRIEF_TIMEOUT)  # joiner is parked on the winner's flight
        release.set()

        winner.join(timeout=BRIEF_TIMEOUT)
        joiner.join(timeout=BRIEF_TIMEOUT)

        assert winner_result and winner_result[0].organization_id == DEFAULT_ORG
        assert len(joiner_error) == 1 and isinstance(joiner_error[0], InterfaceError)
        assert len(factory.providers) == 1


class TestLockNotHeldAcrossLogin:
    def test_the_module_lock_is_free_while_a_login_is_in_flight(self):
        """The load-bearing detail of the design: the module lock must be released across the
        multi-minute browser round-trip, never held.

        Observed behaviorally through `shutdown()`, which needs that lock: while the winner is
        parked inside `login()`, a `shutdown()` on another thread must complete promptly. Were the
        lock held across the login, it would block until the browser returned. The winner then
        finds its slot cleared and discards the provider it built rather than installing it into a
        torn-down holder.
        """
        release = threading.Event()
        entered = threading.Event()

        def gate() -> None:
            entered.set()
            assert release.wait(timeout=BRIEF_TIMEOUT)

        factory = RecordingFactory(gate)
        holder = ProcessOAuthHolder.instance()

        winner_error: list[BaseException] = []

        def run_winner() -> None:
            try:
                holder.acquire(CONFIG, provider_factory=factory)
            except BaseException as e:  # noqa: BLE001
                winner_error.append(e)

        winner = threading.Thread(target=run_winner)
        winner.start()
        assert entered.wait(timeout=BRIEF_TIMEOUT)

        # If the lock were held across login(), this shutdown would block until `release`.
        shutdown_thread = threading.Thread(target=holder.shutdown)
        shutdown_thread.start()
        shutdown_thread.join(timeout=BRIEF_TIMEOUT)
        assert not shutdown_thread.is_alive()

        release.set()
        winner.join(timeout=BRIEF_TIMEOUT)
        assert not winner.is_alive()

        # The winner's provider was discarded (closed, not installed) because shutdown cleared the
        # slot while it was still logging in.
        assert factory.providers[0].closed
        assert len(winner_error) == 1


class TestShutdown:
    def test_shutdown_all_closes_the_provider_and_resets_to_pristine(self):
        first_factory = RecordingFactory()
        holder = ProcessOAuthHolder.instance()
        provider = holder.acquire(CONFIG, provider_factory=first_factory)

        shutdown_all()
        assert first_factory.providers[0].closed

        # Pristine: a later acquire builds and logs in anew rather than handing back the closed one.
        second_factory = RecordingFactory()
        again = holder.acquire(CONFIG, provider_factory=second_factory)
        assert again is not provider
        assert again is second_factory.providers[0]

    def test_shutdown_all_is_idempotent(self):
        factory = RecordingFactory()
        ProcessOAuthHolder.instance().acquire(CONFIG, provider_factory=factory)
        shutdown_all()
        shutdown_all()  # no provider left; must not raise

    def test_shutdown_all_on_a_fresh_holder_is_a_no_op(self):
        shutdown_all()

    def test_instance_returns_the_one_singleton(self):
        assert ProcessOAuthHolder.instance() is ProcessOAuthHolder.instance()


class TestModuleLevelFunctions:
    """The `acquire()` / `shutdown_all()` free functions delegate to the one singleton -- the
    everyday API most callers use instead of `ProcessOAuthHolder.instance()`."""

    def test_module_acquire_delegates_to_the_singleton(self):
        factory = RecordingFactory()
        provider = acquire(CONFIG, provider_factory=factory)

        # Same provider the singleton holds, and a second call through either door shares it.
        assert provider is factory.providers[0]
        assert ProcessOAuthHolder.instance().acquire(CONFIG, provider_factory=factory) is provider
        assert len(factory.providers) == 1

    def test_module_shutdown_all_tears_down_what_module_acquire_built(self):
        factory = RecordingFactory()
        acquire(CONFIG, provider_factory=factory)
        shutdown_all()
        assert factory.providers[0].closed


def _raising(exc: BaseException) -> Callable[[], None]:
    """A `gate` that raises `exc` -- how a `FakeProvider.login()` is made to fail."""

    def gate() -> None:
        raise exc

    return gate


def _arm_join_barrier(monkeypatch: pytest.MonkeyPatch, expected: int) -> threading.Event:
    """Make the holder's in-flight Future signal once `expected` joiners are parked on it.

    A real happens-before in place of a timing guess: a test waits on the returned event and
    thereby *knows* every joiner has captured the flight and is blocked in `result()` before it
    lets the gated winner finish or fail. Without it a late joiner could reach the slot only after
    the winner cleared a failed flight, and start a second login -- a genuine flake.

    Patches the `Future` the holder constructs, so it must be armed *before* the winner starts
    (the winner is what builds the Future); only joiners call `result()`, so the count is exactly
    the number of threads that have joined the flight.
    """
    all_waiting = threading.Event()
    arrived = [0]
    lock = threading.Lock()

    class _CountingFuture(Future[OAuthProvider]):
        def result(self, timeout: float | None = None) -> OAuthProvider:
            with lock:
                arrived[0] += 1
                if arrived[0] >= expected:
                    all_waiting.set()
            return super().result(timeout)

    monkeypatch.setattr(holder_module, "Future", _CountingFuture)
    return all_waiting
