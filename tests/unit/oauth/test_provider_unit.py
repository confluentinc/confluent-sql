"""Unit tests for `CCloudOAuth`, the interactive-OAuth orchestrator (#153).

Three fakes stand in for the three things a real login touches, and the split between them is
deliberate:

- **`FakeCCloud`** backs the provider's *own* private `httpx.Client` through a `MockTransport`,
  answering the auth service's token endpoint and Confluent Cloud's `/api/sessions` +
  `/api/access_tokens`. It enforces the property that makes this whole child's locking
  load-bearing: **a refresh token is single-use**, and presenting a spent one earns a
  `403 invalid_grant`, exactly as production would.
- **`FakeResource`** backs a *separate* client whose `auth=` is one of the provider's two
  adapters. Requests go through real `httpx` auth invocation rather than hand-cranking the
  `auth_flow` generator, because "httpx calls this concurrently with no serialization of its
  own" is precisely the condition the adapters are written against.
- **The browser** is a callable that performs a real loopback GET against the real
  `CallbackServer`. The socket is not mocked away: a browser being able to reach the listener is
  the point of the redirect leg.

Tokens are minted as genuine three-segment JWTs carrying `exp`, since that is what
`token_chain._jwt_exp` reads to derive expiry. Their lifetimes are set *per mint* off mutable
`FakeCCloud` attributes, so a test can hand out a born-stale token (lifetime under
`EXPIRY_SKEW`) and then widen the lifetime before the refresh, without any clock injection or
sleeping.
"""

from __future__ import annotations

import base64
import dataclasses
import json
import logging
import socket
import threading
import urllib.parse
from collections.abc import Callable, Iterator
from contextlib import closing, contextmanager
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from confluent_sql.exceptions import (
    OAuthLoginError,
    OAuthLoginFailure,
    OperationalError,
    ProgrammingError,
    ReauthenticationReason,
    ReauthenticationRequired,
)
from confluent_sql.oauth.callback_server import CallbackServer
from confluent_sql.oauth.config import CCloudOAuthConfig
from confluent_sql.oauth.pkce import challenge_for
from confluent_sql.oauth.provider import CCloudOAuth
from confluent_sql.oauth.token_set import ABSOLUTE_LIFETIME, TokenSet

pytestmark = pytest.mark.unit

AUTH_SERVICE_DOMAIN = "login.confluent.io"
API_HOST = "https://confluent.cloud"
CLIENT_ID = "test-client-id"
CALLBACK_PATH = "/gateway/v1/callback-local-mcp-docs"

AUTH_CODE = "the-authorization-code"
INITIAL_REFRESH_TOKEN = "refresh-token-0"
ORG_RESOURCE_ID = "org-resolved-from-the-session"

PII_EMAIL = "someone@example.com"
"""Planted in the fake `/api/sessions` response so a test can prove the body never reaches a log
record. The real response is large and PII-laden; issue #153 calls this out explicitly."""

LONG_LIFETIME = timedelta(minutes=5)
"""Comfortably past `EXPIRY_SKEW`, so a token minted with it is usable."""

BORN_STALE_LIFETIME = timedelta(seconds=5)
"""Inside `EXPIRY_SKEW` (30s), so a token minted with it is never considered usable -- the
no-sleeping way to hand out a token that must trigger a refresh on first use."""

RESOURCE_URL = "https://flink.example.confluent.cloud/v1/statements"
"""Stand-in for whichever surface an adapter is stamping; the URL itself is never inspected."""

BRIEF_TIMEOUT = 5.0
"""Login timeout for tests that are *expected* to succeed -- generous for a loaded CI box, far
below the 120s production default so a wedged wait fails fast."""


def _free_port() -> int:
    """Reserve an ephemeral port number and release it for the CallbackServer to rebind.

    The callback port is fixed in production (it is baked into the client registration), so
    unlike `CallbackServer`'s own tests -- which bind port 0 and read the port back -- these
    tests need the number *before* the login starts: the provider builds `redirect_uri` from
    config, and the fake browser drives that exact URL. Never binds the real 26640, which a
    developer's live mcp-confluent login legitimately holds.
    """
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def _config(port: int) -> CCloudOAuthConfig:
    return CCloudOAuthConfig(
        auth_service_domain=AUTH_SERVICE_DOMAIN,
        api_host=API_HOST,
        client_id=CLIENT_ID,
        callback_host="127.0.0.1",
        callback_port=port,
        callback_path=CALLBACK_PATH,
    )


def _jwt(kind: str, serial: int, lifetime: timedelta) -> str:
    """A three-segment JWT whose payload carries `exp` -- what `token_chain._jwt_exp` reads.

    `kind` and `serial` are carried purely to make every minted token a distinct string, so a
    test can assert that the two adapters stamp *different* tokens, and that a refresh replaced
    the one before it.
    """
    claims = {
        "exp": int((datetime.now(timezone.utc) + lifetime).timestamp()),
        "kind": kind,
        "serial": serial,
    }
    payload = base64.urlsafe_b64encode(json.dumps(claims).encode()).rstrip(b"=").decode("ascii")
    return f"header.{payload}.signature"


class FakeCCloud:
    """The auth service's token endpoint plus Confluent Cloud's two token routes.

    Failure injection is by attribute rather than by subclass so a test reads as a sequence of
    facts about the service ("the next refresh is refused with invalid_grant") rather than as
    fake plumbing.
    """

    def __init__(self) -> None:
        self.cp_lifetime = LONG_LIFETIME
        self.dp_lifetime = LONG_LIFETIME
        self.organization: dict[str, str] | None = {"resource_id": ORG_RESOURCE_ID}

        self.code_grants: list[dict[str, str]] = []
        self.refresh_grants: list[dict[str, str]] = []
        self.sessions_requests: list[dict[str, str]] = []
        self.access_token_authorizations: list[str | None] = []

        self.fail_token_endpoint_with: tuple[int, str | None] | None = None
        """(status, error code) to refuse the *next and every* token-endpoint call with."""
        self.fail_sessions_with: int | None = None
        """Status to fail `/api/sessions` with -- how a test forces a mid-chain failure after
        the refresh token has already been rotated."""

        self._live_refresh_token = INITIAL_REFRESH_TOKEN
        self._serial = 0

    @property
    def current_refresh_token(self) -> str:
        """The only refresh token the service will still honour."""
        return self._live_refresh_token

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self._handle)

    def _handle(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url == f"https://{AUTH_SERVICE_DOMAIN}/oauth/token":
            return self._token_endpoint(request)
        if url == f"{API_HOST}/api/sessions":
            return self._sessions(request)
        if url == f"{API_HOST}/api/access_tokens":
            return self._access_tokens(request)
        raise AssertionError(f"unexpected request to {url}")

    def _token_endpoint(self, request: httpx.Request) -> httpx.Response:
        form = dict(urllib.parse.parse_qsl(request.content.decode()))
        if form["grant_type"] == "authorization_code":
            self.code_grants.append(form)
        else:
            self.refresh_grants.append(form)

        if self.fail_token_endpoint_with is not None:
            status, error_code = self.fail_token_endpoint_with
            body = {"error_description": "refused by the fake"}
            if error_code is not None:
                body["error"] = error_code
            return httpx.Response(status, json=body)

        # The single-use rule, enforced exactly as production does: presenting a token that has
        # already been rotated away is indistinguishable from presenting a revoked one.
        if (
            form["grant_type"] == "refresh_token"
            and form["refresh_token"] != self._live_refresh_token
        ):
            return httpx.Response(
                403, json={"error": "invalid_grant", "error_description": "Unknown or expired"}
            )

        self._serial += 1
        self._live_refresh_token = f"refresh-token-{self._serial}"
        return httpx.Response(
            200,
            json={
                "id_token": f"id-token-{self._serial}",
                "refresh_token": self._live_refresh_token,
                # Present, and deliberately ignored by the chain -- the auth service's own
                # access_token has no downstream use.
                "access_token": "unused-auth-service-access-token",
                "expires_in": 86400,
            },
        )

    def _sessions(self, request: httpx.Request) -> httpx.Response:
        self.sessions_requests.append(json.loads(request.content))
        if self.fail_sessions_with is not None:
            return httpx.Response(self.fail_sessions_with, json={"message": "sessions is down"})
        body: dict[str, object] = {
            "token": _jwt("cp", self._serial, self.cp_lifetime),
            # A stand-in for the real response's large, PII-laden remainder.
            "user": {"email": PII_EMAIL, "first_name": "Ada", "last_name": "Lovelace"},
        }
        if self.organization is not None:
            body["organization"] = self.organization
        return httpx.Response(200, json=body)

    def _access_tokens(self, request: httpx.Request) -> httpx.Response:
        self.access_token_authorizations.append(request.headers.get("Authorization"))
        return httpx.Response(
            200,
            json={
                "token": _jwt("dp", self._serial, self.dp_lifetime),
                "regional_token": "unused-regional-token",
            },
        )


class FakeResource:
    """A stand-in data-plane/control-plane endpoint, recording what each adapter stamped."""

    def __init__(self, *statuses: int) -> None:
        self.authorizations: list[str | None] = []
        self._statuses = list(statuses)
        self._lock = threading.Lock()

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self._handle)

    def _handle(self, request: httpx.Request) -> httpx.Response:
        with self._lock:
            self.authorizations.append(request.headers.get("Authorization"))
            status = self._statuses.pop(0) if self._statuses else 200
        return httpx.Response(status, json={})

    def bearer_tokens(self) -> list[str]:
        return [(a or "").removeprefix("Bearer ") for a in self.authorizations]


def _browser(
    *, code: str = AUTH_CODE, error: str | None = None, state: str | None = None
) -> Callable[[str], bool]:
    """A fake browser: performs the loopback redirect the auth service would perform.

    Runs synchronously inside the provider's `open_browser` call, which the provider makes after
    the listener is already serving and before it blocks in `wait_for_code` -- so by the time the
    provider waits, the outcome is already recorded.
    """

    def open_browser(url: str) -> bool:
        params = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query))
        query = {"state": state if state is not None else params["state"]}
        if error is not None:
            query["error"] = error
        else:
            query["code"] = code
        httpx.get(params["redirect_uri"], params=query, timeout=BRIEF_TIMEOUT)
        return True

    return open_browser


@contextmanager
def _provider(
    fake: FakeCCloud, browser: Callable[[str], bool] | None = None
) -> Iterator[CCloudOAuth]:
    provider = CCloudOAuth(
        _config(_free_port()),
        http_client=httpx.Client(transport=fake.transport()),
        open_browser=browser if browser is not None else _browser(),
    )
    try:
        yield provider
    finally:
        provider.close()


@contextmanager
def _logged_in(fake: FakeCCloud, org_resource_id: str | None = None) -> Iterator[CCloudOAuth]:
    with _provider(fake) as provider:
        provider.login(org_resource_id, timeout=BRIEF_TIMEOUT)
        yield provider


def _resource_client(auth: httpx.Auth, resource: FakeResource) -> httpx.Client:
    return httpx.Client(transport=resource.transport(), auth=auth)


def _tokens(provider: CCloudOAuth) -> TokenSet:
    """The provider's current snapshot, narrowed from the Optional the property returns.

    After a login it is never None; asserting that once here keeps every call site readable
    instead of scattering the same narrowing assert through every test.
    """
    snapshot = provider.token_set
    assert snapshot is not None
    return snapshot


def _doctor_token_set(provider: CCloudOAuth, **changes: datetime) -> None:
    """Rewrite the provider's current snapshot's expiries in place of waiting for a clock.

    Used only where the condition under test *is* an elapsed clock -- chiefly the ~8h absolute
    wall, which no amount of fake-service tuning can reach, since the provider computes it
    locally at mint time. Staleness of the short-lived CP/DP tokens is normally produced through
    `FakeCCloud`'s lifetimes instead; this is the escape hatch for the rest.
    """
    with provider._token_lock:  # noqa: SLF001
        assert provider._token_set is not None  # noqa: SLF001
        provider._token_set = dataclasses.replace(provider._token_set, **changes)  # noqa: SLF001


def _past() -> datetime:
    return datetime.now(timezone.utc) - timedelta(minutes=1)


class TestLogin:
    def test_drives_the_full_chain_and_populates_both_tokens(self):
        fake = FakeCCloud()
        with _logged_in(fake) as provider:
            snapshot = provider.token_set

        assert snapshot is not None
        assert snapshot.cp_token != snapshot.dp_token
        assert snapshot.refresh_token == "refresh-token-1"
        assert len(fake.code_grants) == 1
        assert len(fake.sessions_requests) == 1
        assert len(fake.access_token_authorizations) == 1

    def test_authorize_url_carries_the_pkce_and_client_parameters(self):
        """Pinned against both prior-art implementations (mcp-confluent's
        `buildAuthorizationUrl`, ide-sidecar's `getSignInUri`): the same seven parameters, no
        `audience`. The challenge must be the S256 hash of the verifier that the code exchange
        later presents -- the two are minted together, and a mismatch fails only at the auth
        service, so it is worth pinning here."""
        opened: list[str] = []
        inner = _browser()

        def recording_browser(url: str) -> bool:
            opened.append(url)
            return inner(url)

        fake = FakeCCloud()
        with _provider(fake, recording_browser) as provider:
            provider.login(None, timeout=BRIEF_TIMEOUT)

        (url,) = opened
        split = urllib.parse.urlsplit(url)
        params = dict(urllib.parse.parse_qsl(split.query))
        assert f"https://{split.netloc}{split.path}" == f"https://{AUTH_SERVICE_DOMAIN}/authorize"
        assert params["client_id"] == CLIENT_ID
        assert params["response_type"] == "code"
        assert params["code_challenge_method"] == "S256"
        assert params["scope"] == "email openid offline_access"
        assert params["redirect_uri"].endswith(CALLBACK_PATH)
        assert params["state"]
        assert "audience" not in params

        assert challenge_for(fake.code_grants[0]["code_verifier"]) == params["code_challenge"]

    def test_code_exchange_presents_the_captured_code_and_matching_redirect_uri(self):
        opened: list[str] = []
        inner = _browser()

        def recording_browser(url: str) -> bool:
            opened.append(url)
            return inner(url)

        fake = FakeCCloud()
        with _provider(fake, recording_browser) as provider:
            provider.login(None, timeout=BRIEF_TIMEOUT)

        grant = fake.code_grants[0]
        authorize_params = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(opened[0]).query))
        assert grant["code"] == AUTH_CODE
        assert grant["grant_type"] == "authorization_code"
        # The auth service matches redirect_uri between the two legs; a mismatch is rejected.
        assert grant["redirect_uri"] == authorize_params["redirect_uri"]

    def test_omitted_org_is_resolved_from_the_session(self):
        fake = FakeCCloud()
        with _logged_in(fake) as provider:
            assert provider.organization_id == ORG_RESOURCE_ID
        assert "org_resource_id" not in fake.sessions_requests[0]

    def test_supplied_org_scopes_the_session(self):
        fake = FakeCCloud()
        supplied = "org-the-caller-chose"
        fake.organization = {"resource_id": supplied}
        with _logged_in(fake, supplied) as provider:
            assert provider.organization_id == supplied
        assert fake.sessions_requests[0]["org_resource_id"] == supplied

    def test_session_without_an_organization_raises(self):
        """Every caller in this epic needs an org -- #155 fills the Flink path from it -- so a
        session that resolves none is a failure now rather than a None that surfaces later."""
        fake = FakeCCloud()
        fake.organization = None
        with _provider(fake) as provider, pytest.raises(OperationalError, match="organization"):
            provider.login(None, timeout=BRIEF_TIMEOUT)

    def test_session_response_body_is_never_logged(self, caplog):
        """The `/api/sessions` response is large and PII-laden (issue #153). Only the token and
        the org's resource_id may be taken from it; nothing may log the body."""
        fake = FakeCCloud()
        with caplog.at_level(logging.DEBUG, logger="confluent_sql"), _logged_in(fake):
            pass
        assert PII_EMAIL not in caplog.text

    def test_denied_authorization_surfaces_as_login_error(self):
        fake = FakeCCloud()
        with (
            _provider(fake, _browser(error="access_denied")) as provider,
            pytest.raises(OAuthLoginError) as caught,
        ):
            provider.login(None, timeout=BRIEF_TIMEOUT)
        assert caught.value.reason is OAuthLoginFailure.AUTHORIZATION_DENIED
        assert not fake.code_grants

    def test_unopenable_browser_logs_the_url_and_still_completes(self, caplog):
        """A headless box (no BROWSER, no DISPLAY) is not a dead end: `webbrowser.open` returns
        False, and the user can still paste the URL. Failing here would be gratuitous."""
        inner = _browser()

        def declining_browser(url: str) -> bool:
            inner(url)
            return False

        fake = FakeCCloud()
        with (
            caplog.at_level(logging.INFO, logger="confluent_sql.oauth.provider"),
            _provider(fake, declining_browser) as provider,
        ):
            provider.login(None, timeout=BRIEF_TIMEOUT)
            assert provider.token_set is not None
        assert "/authorize?" in caplog.text

    def test_second_login_on_a_logged_in_provider_is_refused(self):
        """Re-login is `reauthenticate()` (#156), routed through the refresh gate instead --
        see TestReauthenticate. A second `login()` remains refused unconditionally: it would
        silently strand the live session's tokens."""
        fake = FakeCCloud()
        with _logged_in(fake) as provider, pytest.raises(ProgrammingError, match="already"):
            provider.login(None, timeout=BRIEF_TIMEOUT)

    def test_releases_the_callback_port_after_a_successful_login(self):
        """The port is fixed by the client registration, so a leaked listener blocks every later
        login in the process.

        Asserted by binding a fresh `CallbackServer` on the same port -- the thing that would
        actually be attempted next -- rather than a bare socket. A bare probe would fail on
        TIME_WAIT left by the browser's connection and prove nothing about a leak; the real
        listener sets SO_REUSEADDR, so it fails only against a genuinely live listener.
        """
        fake = FakeCCloud()
        port = _free_port()
        provider = CCloudOAuth(
            _config(port),
            http_client=httpx.Client(transport=fake.transport()),
            open_browser=_browser(),
        )
        try:
            provider.login(None, timeout=BRIEF_TIMEOUT)
        finally:
            provider.close()

        with CallbackServer(_config(port), "a-later-logins-state") as later_login:
            assert later_login.port == port


class TestRefresh:
    def test_stale_data_plane_token_is_refreshed_before_the_request_is_stamped(self):
        fake = FakeCCloud()
        fake.dp_lifetime = BORN_STALE_LIFETIME
        with _logged_in(fake) as provider:
            fake.dp_lifetime = LONG_LIFETIME
            resource = FakeResource()
            with _resource_client(provider.data_plane_auth, resource) as client:
                client.get(RESOURCE_URL)

            assert len(fake.refresh_grants) == 1
            assert resource.bearer_tokens() == [_tokens(provider).dp_token]

    def test_refresh_carries_the_resolved_org_even_when_login_omitted_it(self):
        """The session must stay scoped to the org the login settled on; re-resolving a default
        would silently move a multi-org user between orgs mid-session."""
        fake = FakeCCloud()
        fake.cp_lifetime = BORN_STALE_LIFETIME
        with _logged_in(fake) as provider:
            fake.cp_lifetime = LONG_LIFETIME
            resource = FakeResource()
            with _resource_client(provider.control_plane_auth, resource) as client:
                client.get(RESOURCE_URL)

        assert "org_resource_id" not in fake.sessions_requests[0]
        assert fake.sessions_requests[1]["org_resource_id"] == ORG_RESOURCE_ID

    def test_refresh_does_not_extend_the_absolute_wall(self):
        """Rotation resets the *idle* timer but never moves the ~8h absolute cap -- it is a
        server-side policy on the application. Carrying the original expiry forward is what
        makes the wall arrive on schedule instead of receding on every refresh."""
        fake = FakeCCloud()
        fake.dp_lifetime = BORN_STALE_LIFETIME
        with _logged_in(fake) as provider:
            wall_at_login = _tokens(provider).refresh_token_expires_at
            fake.dp_lifetime = LONG_LIFETIME
            resource = FakeResource()
            with _resource_client(provider.data_plane_auth, resource) as client:
                client.get(RESOURCE_URL)

            assert _tokens(provider).refresh_token_expires_at == wall_at_login

    def test_login_sets_the_absolute_wall_one_lifetime_out(self):
        before = datetime.now(timezone.utc)
        fake = FakeCCloud()
        with _logged_in(fake) as provider:
            after = datetime.now(timezone.utc)
            wall = _tokens(provider).refresh_token_expires_at
            assert before + ABSOLUTE_LIFETIME <= wall <= after + ABSOLUTE_LIFETIME

    def test_rotated_refresh_token_is_persisted_before_the_cp_and_dp_legs(self):
        """The persist-before-exchange ordering, observed through its whole reason for existing:
        a refresh that dies *after* rotating must leave the new token in hand. Keeping the old
        one would be a hard lockout -- it is already spent and the service will never honour it
        again."""
        fake = FakeCCloud()
        fake.dp_lifetime = BORN_STALE_LIFETIME
        with _logged_in(fake) as provider:
            fake.dp_lifetime = LONG_LIFETIME
            fake.fail_sessions_with = 500

            resource = FakeResource()
            with (
                _resource_client(provider.data_plane_auth, resource) as client,
                pytest.raises(OperationalError, match="sessions is down"),
            ):
                client.get(RESOURCE_URL)

            assert _tokens(provider).refresh_token == fake.current_refresh_token

            # ...and the proof that this is not a lockout: the very next attempt succeeds,
            # because it presents the rotated token rather than the spent one.
            fake.fail_sessions_with = None
            with _resource_client(provider.data_plane_auth, resource) as client:
                client.get(RESOURCE_URL)
            assert resource.bearer_tokens()[-1] == _tokens(provider).dp_token

    def test_concurrent_refreshes_of_one_stale_snapshot_run_the_chain_once(self):
        """The single-flight gate's headline case, driven at the gate itself.

        Every thread enters holding the *same* stale snapshot -- the situation N workers hitting
        a lapsed token together produce. One must run the chain; the rest must double-check,
        find the winner's newer snapshot in the slot, and return it untouched. Without that
        check each waiter runs its own redundant four-call chain, burning a refresh against the
        service's ~50-refresh cap and paying the latency, for a token it already has.

        Driven through `_refresh` directly rather than through httpx: routed through the request
        path, the losers would re-read the slot and find a *fresh* token before ever reaching
        the gate, so the contention this is about would almost never be reached.
        """
        fake = FakeCCloud()
        with _logged_in(fake) as provider:
            _doctor_token_set(provider, cp_token_expires_at=_past(), dp_token_expires_at=_past())
            stale = _tokens(provider)

            thread_count = 8
            barrier = threading.Barrier(thread_count)
            results: list[TokenSet] = []
            errors: list[BaseException] = []
            results_lock = threading.Lock()

            def refresh() -> None:
                try:
                    barrier.wait(timeout=BRIEF_TIMEOUT)
                    refreshed = provider._refresh(stale)  # noqa: SLF001
                    with results_lock:
                        results.append(refreshed)
                except BaseException as e:  # noqa: BLE001
                    with results_lock:
                        errors.append(e)

            threads = [threading.Thread(target=refresh) for _ in range(thread_count)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=BRIEF_TIMEOUT * 2)

            assert not errors
            assert not any(thread.is_alive() for thread in threads)
            assert len(fake.refresh_grants) == 1
            # Every thread left with the winner's snapshot -- the same object, not merely equal.
            assert len(results) == thread_count
            assert all(result is results[0] for result in results)

    def test_concurrent_waiters_share_one_failed_attempt(self):
        """A failed refresh must cost *one* rotation, not one per waiter.

        This is what the shared `Future` buys over a lock: a thread waking from a lock knows only
        that it may proceed, so it re-derives the outcome by running its own chain -- and every
        such chain spends another rotation against the service's ~50-refresh cap. During a real
        outage that is how a session gets bricked by the very lockout the gate exists to prevent.
        Joining a `Future` instead, the waiters receive the winner's failure verbatim.

        The chain is slowed deliberately: `MockTransport` answers instantly, so without it the
        winner finishes before the other threads arrive and nothing is concurrent at all -- the
        test would pass against a design that collapses nothing.
        """
        fake = FakeCCloud()
        with _logged_in(fake) as provider:
            _doctor_token_set(provider, cp_token_expires_at=_past(), dp_token_expires_at=_past())
            stale = _tokens(provider)

            sessions_entered = threading.Event()
            release_sessions = threading.Event()
            answer_sessions = fake._sessions  # noqa: SLF001

            def slow_failing_sessions(request: httpx.Request) -> httpx.Response:
                sessions_entered.set()
                release_sessions.wait(timeout=BRIEF_TIMEOUT)
                return answer_sessions(request)

            fake._sessions = slow_failing_sessions  # noqa: SLF001
            fake.fail_sessions_with = 500

            thread_count = 8
            raised: list[BaseException] = []
            raised_lock = threading.Lock()

            def refresh() -> None:
                try:
                    provider._refresh(stale)  # noqa: SLF001
                except BaseException as e:  # noqa: BLE001
                    with raised_lock:
                        raised.append(e)

            threads = [threading.Thread(target=refresh) for _ in range(thread_count)]
            threads[0].start()
            # Only once the winner is demonstrably inside the chain do the rest pile on, so they
            # are guaranteed to arrive while the flight is genuinely in progress.
            assert sessions_entered.wait(timeout=BRIEF_TIMEOUT)
            for thread in threads[1:]:
                thread.start()
            release_sessions.set()
            for thread in threads:
                thread.join(timeout=BRIEF_TIMEOUT * 2)

            assert not any(thread.is_alive() for thread in threads)
            assert len(raised) == thread_count
            assert len(fake.refresh_grants) == 1
            assert len(fake.sessions_requests) == 2  # the login's, plus this one attempt
            # Every waiter saw the winner's actual failure, not one it re-derived itself.
            assert all(isinstance(e, OperationalError) for e in raised)
            assert len({id(e) for e in raised}) == 1

    def test_interim_snapshot_is_not_mistaken_for_a_completed_refresh(self):
        """The mid-chain checkpoint must not satisfy another thread's double-check.

        Persist-before-exchange publishes a snapshot carrying the rotated refresh token but the
        *old* CP/DP tokens. If the chain then fails, that checkpoint sits in the slot -- and a
        waiter that entered holding the pre-failure snapshot would see "the slot changed, someone
        refreshed" and take it as finished work. It isn't: its CP/DP tokens are the very ones the
        waiter already knew were stale (or that had just been 401'd), so the waiter would either
        send a knowingly-dead token or, on the 401 path, re-stamp the same rejected bearer, burn
        its one retry, and surface a second 401 -- all while a perfectly usable rotated refresh
        token sat in the slot.
        """
        fake = FakeCCloud()
        with _logged_in(fake) as provider:
            # What a waiter queued at the gate would be holding.
            stale = _tokens(provider)

            _doctor_token_set(provider, cp_token_expires_at=_past(), dp_token_expires_at=_past())
            fake.fail_sessions_with = 500
            resource = FakeResource()
            with (
                _resource_client(provider.data_plane_auth, resource) as client,
                pytest.raises(OperationalError, match="sessions is down"),
            ):
                client.get(RESOURCE_URL)

            interim = _tokens(provider)
            assert interim.refresh_token != stale.refresh_token  # rotated, and persisted
            assert interim.dp_token == stale.dp_token  # ...but the plane tokens are untouched
            assert len(fake.refresh_grants) == 1

            fake.fail_sessions_with = None
            recovered = provider._refresh(stale)  # noqa: SLF001

            # The waiter must get genuinely new plane tokens, not the checkpoint handed back.
            assert recovered is not interim
            assert recovered.dp_token != stale.dp_token
            assert recovered.cp_token != stale.cp_token
            assert len(fake.refresh_grants) == 2
            # ...and it spent the rotated token from the slot, never the one already consumed.
            assert fake.refresh_grants[1]["refresh_token"] == interim.refresh_token

    def test_one_chain_run_serves_both_planes(self):
        """A refresh re-mints CP *and* DP together, so a control-plane request that finds both
        lapsed leaves nothing for a following data-plane request to refresh."""
        fake = FakeCCloud()
        with _logged_in(fake) as provider:
            _doctor_token_set(provider, cp_token_expires_at=_past(), dp_token_expires_at=_past())

            control_plane, data_plane = FakeResource(), FakeResource()
            with _resource_client(provider.control_plane_auth, control_plane) as client:
                client.get(RESOURCE_URL)
            with _resource_client(provider.data_plane_auth, data_plane) as client:
                client.get(RESOURCE_URL)

            assert len(fake.refresh_grants) == 1
            assert control_plane.bearer_tokens() == [_tokens(provider).cp_token]
            assert data_plane.bearer_tokens() == [_tokens(provider).dp_token]

    def test_concurrent_requests_across_both_views_never_double_spend(self):
        """Eight threads hammering both adapters through real httpx auth invocation.

        The double-spend assertion is enforced by `FakeCCloud` rather than by this test's own
        asserts: presenting an already-rotated refresh token earns a `403 invalid_grant`, which
        would surface as `ReauthenticationRequired` on the offending thread. So `not errors` is
        the real claim -- no thread ever spent a token another had already spent.
        """
        fake = FakeCCloud()
        with _logged_in(fake) as provider:
            _doctor_token_set(provider, cp_token_expires_at=_past(), dp_token_expires_at=_past())

            thread_count = 8
            barrier = threading.Barrier(thread_count)
            errors: list[BaseException] = []
            errors_lock = threading.Lock()
            resource = FakeResource()

            def issue_request(auth: httpx.Auth) -> None:
                try:
                    barrier.wait(timeout=BRIEF_TIMEOUT)
                    with _resource_client(auth, resource) as client:
                        client.get(RESOURCE_URL)
                except BaseException as e:  # noqa: BLE001
                    with errors_lock:
                        errors.append(e)

            threads = [
                threading.Thread(
                    target=issue_request,
                    args=(provider.control_plane_auth if i % 2 else provider.data_plane_auth,),
                )
                for i in range(thread_count)
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=BRIEF_TIMEOUT * 2)

            assert not errors
            assert not any(thread.is_alive() for thread in threads)
            assert len(resource.authorizations) == thread_count

    def test_a_valid_token_never_reaches_the_refresh_gate(self):
        fake = FakeCCloud()
        with _logged_in(fake) as provider:
            resource = FakeResource()
            with _resource_client(provider.data_plane_auth, resource) as client:
                client.get(RESOURCE_URL)
                client.get(RESOURCE_URL)
        assert not fake.refresh_grants

    def test_transient_refresh_failure_does_not_wedge_the_session(self):
        """A 503 is a blip. Latching it into the permanent failure flag would turn a momentary
        outage into a session that can only be recovered by a fresh browser login."""
        fake = FakeCCloud()
        fake.dp_lifetime = BORN_STALE_LIFETIME
        with _logged_in(fake) as provider:
            fake.dp_lifetime = LONG_LIFETIME
            fake.fail_token_endpoint_with = (503, None)

            resource = FakeResource()
            with (
                _resource_client(provider.data_plane_auth, resource) as client,
                pytest.raises(OperationalError) as caught,
            ):
                client.get(RESOURCE_URL)
            assert not isinstance(caught.value, ReauthenticationRequired)

            fake.fail_token_endpoint_with = None
            with _resource_client(provider.data_plane_auth, resource) as client:
                client.get(RESOURCE_URL)
            assert resource.bearer_tokens()[-1] == _tokens(provider).dp_token


class TestAuthAdapters:
    def test_the_two_views_stamp_different_tokens_from_one_snapshot(self):
        fake = FakeCCloud()
        with _logged_in(fake) as provider:
            control_plane, data_plane = FakeResource(), FakeResource()
            with _resource_client(provider.control_plane_auth, control_plane) as client:
                client.get(RESOURCE_URL)
            with _resource_client(provider.data_plane_auth, data_plane) as client:
                client.get(RESOURCE_URL)

            assert control_plane.bearer_tokens() == [_tokens(provider).cp_token]
            assert data_plane.bearer_tokens() == [_tokens(provider).dp_token]
            assert control_plane.bearer_tokens() != data_plane.bearer_tokens()
        assert not fake.refresh_grants

    def test_401_forces_one_refresh_and_one_retry(self):
        """A token that looked live but was refused anyway -- a revoked session, a clock skew,
        a server-side invalidation. One refresh, one retry, then the caller sees the result."""
        fake = FakeCCloud()
        with _logged_in(fake) as provider:
            first_dp_token = _tokens(provider).dp_token
            resource = FakeResource(401, 200)
            with _resource_client(provider.data_plane_auth, resource) as client:
                response = client.get(RESOURCE_URL)

            assert response.status_code == 200
            assert len(fake.refresh_grants) == 1
            stamped = resource.bearer_tokens()
            assert stamped[0] == first_dp_token
            assert stamped[1] == _tokens(provider).dp_token
            assert stamped[0] != stamped[1]

    def test_a_second_401_is_surfaced_rather_than_retried_forever(self):
        fake = FakeCCloud()
        with _logged_in(fake) as provider:
            resource = FakeResource(401, 401)
            with _resource_client(provider.data_plane_auth, resource) as client:
                response = client.get(RESOURCE_URL)

            assert response.status_code == 401
            assert len(resource.authorizations) == 2
            assert len(fake.refresh_grants) == 1

    def test_using_an_adapter_before_login_is_a_programming_error(self):
        fake = FakeCCloud()
        with _provider(fake) as provider:
            resource = FakeResource()
            with (
                _resource_client(provider.data_plane_auth, resource) as client,
                pytest.raises(ProgrammingError, match="log in"),
            ):
                client.get(RESOURCE_URL)


class TestReauthenticationWall:
    def test_absolute_expiry_raises_without_spending_a_request(self):
        """Past the wall the refresh token is known-dead locally. Spending a round trip to be
        told so is pure latency on an error path."""
        fake = FakeCCloud()
        with _logged_in(fake) as provider:
            _doctor_token_set(
                provider, dp_token_expires_at=_past(), refresh_token_expires_at=_past()
            )

            resource = FakeResource()
            with (
                _resource_client(provider.data_plane_auth, resource) as client,
                pytest.raises(ReauthenticationRequired) as caught,
            ):
                client.get(RESOURCE_URL)

            assert caught.value.reason is ReauthenticationReason.ABSOLUTE_EXPIRY
            assert not fake.refresh_grants
            assert not resource.authorizations

    def test_invalid_grant_raises_reauthentication_required(self):
        fake = FakeCCloud()
        fake.dp_lifetime = BORN_STALE_LIFETIME
        with _logged_in(fake) as provider:
            fake.fail_token_endpoint_with = (403, "invalid_grant")

            resource = FakeResource()
            with (
                _resource_client(provider.data_plane_auth, resource) as client,
                pytest.raises(ReauthenticationRequired) as caught,
            ):
                client.get(RESOURCE_URL)

            assert caught.value.reason is ReauthenticationReason.REFRESH_REJECTED

    def test_the_failure_latches_so_later_requests_do_not_re_spend(self):
        """Once the refresh token is known dead, every later request must raise straight away
        rather than queue up more doomed exchanges behind the gate."""
        fake = FakeCCloud()
        fake.dp_lifetime = BORN_STALE_LIFETIME
        with _logged_in(fake) as provider:
            fake.fail_token_endpoint_with = (403, "invalid_grant")

            resource = FakeResource()
            for _ in range(3):
                with (
                    _resource_client(provider.data_plane_auth, resource) as client,
                    pytest.raises(ReauthenticationRequired),
                ):
                    client.get(RESOURCE_URL)

            assert len(fake.refresh_grants) == 1
            assert not resource.authorizations

    def test_a_thread_already_queued_at_the_gate_does_not_retry_the_dead_token(self):
        """The race the request path's own failure check cannot cover.

        A thread can read a healthy-looking snapshot, find its token stale, and be waiting on
        the gate at the instant another thread latches the failure. It arrives holding a
        snapshot that still matches the slot, so the double-check waves it through -- and
        without a failure check *inside* the gate it would spend another doomed exchange
        against an already-rejected refresh token.
        """
        fake = FakeCCloud()
        fake.dp_lifetime = BORN_STALE_LIFETIME
        with _logged_in(fake) as provider:
            # The snapshot such a thread would be holding: read before anything failed.
            snapshot_read_before_the_failure = _tokens(provider)

            fake.fail_token_endpoint_with = (403, "invalid_grant")
            resource = FakeResource()
            with (
                _resource_client(provider.data_plane_auth, resource) as client,
                pytest.raises(ReauthenticationRequired),
            ):
                client.get(RESOURCE_URL)
            assert len(fake.refresh_grants) == 1

            with pytest.raises(ReauthenticationRequired) as caught:
                provider._refresh(snapshot_read_before_the_failure)  # noqa: SLF001

            assert caught.value.reason is ReauthenticationReason.REFRESH_REJECTED
            assert len(fake.refresh_grants) == 1

    def test_both_views_observe_the_same_dead_session(self):
        fake = FakeCCloud()
        fake.dp_lifetime = BORN_STALE_LIFETIME
        with _logged_in(fake) as provider:
            fake.fail_token_endpoint_with = (403, "invalid_grant")

            resource = FakeResource()
            with (
                _resource_client(provider.data_plane_auth, resource) as client,
                pytest.raises(ReauthenticationRequired),
            ):
                client.get(RESOURCE_URL)

            with (
                _resource_client(provider.control_plane_auth, resource) as client,
                pytest.raises(ReauthenticationRequired),
            ):
                client.get(RESOURCE_URL)


def _latch_a_dead_session(fake: FakeCCloud, provider: CCloudOAuth) -> None:
    """Drive the provider into the same dead-session state `TestReauthenticationWall` reaches,
    so a `TestReauthenticate` case can start from "the wall has already been hit"."""
    _doctor_token_set(provider, dp_token_expires_at=_past())
    fake.fail_token_endpoint_with = (403, "invalid_grant")

    resource = FakeResource()
    with (
        _resource_client(provider.data_plane_auth, resource) as client,
        pytest.raises(ReauthenticationRequired),
    ):
        client.get(RESOURCE_URL)

    fake.fail_token_endpoint_with = None  # clear the way for reauthenticate()'s fresh login


class TestReauthenticate:
    """#156: recovering a dead session in place via a fresh interactive login, without opening a
    new `Connection`/provider."""

    def test_before_any_login_raises_programming_error(self):
        fake = FakeCCloud()
        with (
            _provider(fake) as provider,
            pytest.raises(ProgrammingError, match="must log in"),
        ):
            provider.reauthenticate(timeout=BRIEF_TIMEOUT)

    def test_clears_the_failure_and_resets_the_absolute_wall(self):
        fake = FakeCCloud()
        with _logged_in(fake) as provider:
            _latch_a_dead_session(fake, provider)
            dead = _tokens(provider)

            provider.reauthenticate(timeout=BRIEF_TIMEOUT)

            fresh = _tokens(provider)
            assert fresh.refresh_token != dead.refresh_token
            assert fresh.dp_token != dead.dp_token
            assert fresh.cp_token != dead.cp_token
            # A fresh login, not a refresh -- the wall is reset from *now*, not carried forward.
            assert fresh.refresh_token_expires_at > datetime.now(timezone.utc)
            # code_grants: the original login's, plus this reauthentication's.
            assert len(fake.code_grants) == 2

            # The failure is cleared -- a request succeeds without raising again.
            resource = FakeResource()
            with _resource_client(provider.data_plane_auth, resource) as client:
                client.get(RESOURCE_URL)
            assert resource.bearer_tokens() == [fresh.dp_token]

    def test_preserves_the_established_organization_even_when_login_omitted_it(self):
        fake = FakeCCloud()
        with _logged_in(fake) as provider:  # org omitted; resolves to ORG_RESOURCE_ID
            assert "org_resource_id" not in fake.sessions_requests[0]

            _latch_a_dead_session(fake, provider)
            provider.reauthenticate(timeout=BRIEF_TIMEOUT)

            # Unlike the original omitted-org login, reauthenticate() always pins the
            # established org explicitly -- it must never let a fresh login silently re-resolve
            # a (possibly different) default for a multi-org user.
            assert fake.sessions_requests[1]["org_resource_id"] == ORG_RESOURCE_ID
            assert provider.organization_id == ORG_RESOURCE_ID

    def test_concurrent_reauthenticates_collapse_to_one_login(self):
        """N callers hitting the wall together must cost exactly one browser bounce, sharing the
        `_refresh()` single-flight gate rather than each running their own login."""
        fake = FakeCCloud()
        with _logged_in(fake) as provider:
            _latch_a_dead_session(fake, provider)

            sessions_entered = threading.Event()
            release_sessions = threading.Event()
            answer_sessions = fake._sessions  # noqa: SLF001

            def slow_sessions(request: httpx.Request) -> httpx.Response:
                sessions_entered.set()
                release_sessions.wait(timeout=BRIEF_TIMEOUT)
                return answer_sessions(request)

            fake._sessions = slow_sessions  # noqa: SLF001

            thread_count = 8
            errors: list[BaseException] = []
            errors_lock = threading.Lock()

            def reauth() -> None:
                try:
                    provider.reauthenticate(timeout=BRIEF_TIMEOUT)
                except BaseException as e:  # noqa: BLE001
                    with errors_lock:
                        errors.append(e)

            threads = [threading.Thread(target=reauth) for _ in range(thread_count)]
            threads[0].start()
            assert sessions_entered.wait(timeout=BRIEF_TIMEOUT)
            for thread in threads[1:]:
                thread.start()
            release_sessions.set()
            for thread in threads:
                thread.join(timeout=BRIEF_TIMEOUT * 2)

            assert not errors
            assert not any(thread.is_alive() for thread in threads)
            # Exactly one fresh login ran: the original login's code grant, plus this one.
            assert len(fake.code_grants) == 2

    def test_a_joiner_receives_the_winners_reauthentication_failure(self):
        """A failed reauthentication must cost one attempt, shared by every waiter -- not one
        doomed login per thread."""
        fake = FakeCCloud()
        with _logged_in(fake) as provider:
            _latch_a_dead_session(fake, provider)

            # The reauthentication attempt itself will also fail, this time with a plain server
            # error rather than invalid_grant, so it surfaces as OperationalError instead of
            # re-latching ReauthenticationRequired.
            fake.fail_token_endpoint_with = (500, None)

            entered = threading.Event()
            release = threading.Event()
            answer_token_endpoint = fake._token_endpoint  # noqa: SLF001

            def slow_token_endpoint(request: httpx.Request) -> httpx.Response:
                entered.set()
                release.wait(timeout=BRIEF_TIMEOUT)
                return answer_token_endpoint(request)

            fake._token_endpoint = slow_token_endpoint  # noqa: SLF001

            thread_count = 8
            raised: list[BaseException] = []
            raised_lock = threading.Lock()

            def reauth() -> None:
                try:
                    provider.reauthenticate(timeout=BRIEF_TIMEOUT)
                except BaseException as e:  # noqa: BLE001
                    with raised_lock:
                        raised.append(e)

            threads = [threading.Thread(target=reauth) for _ in range(thread_count)]
            threads[0].start()
            assert entered.wait(timeout=BRIEF_TIMEOUT)
            for thread in threads[1:]:
                thread.start()
            release.set()
            for thread in threads:
                thread.join(timeout=BRIEF_TIMEOUT * 2)

            assert not any(thread.is_alive() for thread in threads)
            assert len(raised) == thread_count
            assert all(isinstance(e, OperationalError) for e in raised)
            # Every waiter saw the winner's actual failure, not one it re-derived itself.
            assert len({id(e) for e in raised}) == 1
            # Exactly one fresh login attempted: the original login's code grant, plus this one.
            assert len(fake.code_grants) == 2

    def test_a_failed_reauthentication_clears_the_slot_so_a_later_attempt_retries(self):
        """A failed reauthenticate() must not leave a permanently-rejected `Future` sitting in
        `_inflight_refresh` -- that slot is shared with `_refresh()` (see the module docstring),
        so a stranded rejected flight would wedge every later `reauthenticate()` *and* every
        later plain refresh behind the first attempt's stale exception, forever, with no way to
        ever recover the session."""
        fake = FakeCCloud()
        with _logged_in(fake) as provider:
            _latch_a_dead_session(fake, provider)

            fake.fail_token_endpoint_with = (500, None)
            with pytest.raises(OperationalError):
                provider.reauthenticate(timeout=BRIEF_TIMEOUT)

            # Nothing about the service has changed except this -- if the slot weren't cleared,
            # this second attempt would just re-raise the first attempt's exception without ever
            # trying the (now fixable) login again.
            fake.fail_token_endpoint_with = None
            provider.reauthenticate(timeout=BRIEF_TIMEOUT)

            assert provider.token_set is not None


class TestClose:
    def test_close_is_idempotent(self):
        fake = FakeCCloud()
        with _logged_in(fake) as provider:
            provider.close()
            provider.close()

    def test_close_releases_the_private_http_client(self):
        fake = FakeCCloud()
        client = httpx.Client(transport=fake.transport())
        provider = CCloudOAuth(
            _config(_free_port()),
            http_client=client,
            open_browser=_browser(),
        )
        provider.close()
        assert client.is_closed
