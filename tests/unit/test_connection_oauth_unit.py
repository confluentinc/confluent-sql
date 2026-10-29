"""Unit tests for wiring interactive human-login OAuth through connect() (#150).

The OAuth machinery (PKCE primitives, callback server, the CCloudOAuth provider, the
process-wide holder) is exercised end to end by the tests under tests/unit/oauth/.
This file tests only the assembly on top: auth="oauth" mode selection/validation on
connect()/Connection, and wiring the acquired provider's two httpx.Auth views onto the driver's
three clients.

Every test here drives a FakeOAuthProvider injected through the internal oauth_provider_factory
testing hook (mirroring oauth.acquire()'s own provider_factory seam) rather than a real browser
login -- see tests/unit/oauth/test_holder_unit.py's FakeProvider for the same pattern one layer
down.
"""

from __future__ import annotations

from typing import Literal
from unittest.mock import Mock

import httpx
import pytest

from confluent_sql import (
    InterfaceError,
    ReauthenticationReason,
    ReauthenticationRequired,
    Timeouts,
    connect,
)
from confluent_sql.connection import Connection, ControlPlaneSurface
from confluent_sql.oauth.config import CCloudOAuthConfig
from confluent_sql.oauth.provider import OAuthMetrics
from confluent_sql.utils import DEFAULT_HTTP_TIMEOUT_SECS

pytestmark = [pytest.mark.unit, pytest.mark.oauth]

CONFIG = CCloudOAuthConfig(
    auth_service_domain="login.confluent.io",
    api_host="https://confluent.cloud",
    client_id="test-client-id",
)

DEFAULT_ORG = "org-from-session"
"""What a `FakeOAuthProvider` reports when `login()` is handed no org -- the analogue of
Confluent Cloud resolving the user's default and reporting it back."""


class _MarkerAuth(httpx.Auth):
    """A trivial httpx.Auth that stamps an identifiable header, so a request-level test can
    assert the right auth object actually got attached to a client."""

    def __init__(self, marker: str) -> None:
        self.marker = marker

    def auth_flow(self, request: httpx.Request):
        request.headers["X-Marker"] = self.marker
        yield request


class FakeOAuthProvider:
    """A stand-in for `CCloudOAuth` covering the surface Connection touches.

    Implements the `OAuthProvider` protocol structurally. Unlike `test_holder_unit.py`'s
    `FakeProvider` (whose `control_plane_auth`/`data_plane_auth` deliberately raise
    NotImplementedError, since the holder never touches them), this fake needs those views to
    actually work -- they're exactly what `connect()` wires onto the driver's clients.
    """

    def __init__(
        self,
        config: CCloudOAuthConfig,
        http_timeout_secs: float = DEFAULT_HTTP_TIMEOUT_SECS,
        http_user_agent: str = Connection.DEFAULT_USER_AGENT,
    ) -> None:
        self.config = config
        self.http_timeout_secs = http_timeout_secs
        self.http_user_agent = http_user_agent
        self._organization_id: str | None = None
        self.data_plane_auth = _MarkerAuth("dp")
        self.control_plane_auth = _MarkerAuth("cp")
        self.closed = False
        self.metrics_value = OAuthMetrics()
        """Settable by a test, so `Connection.oauth_metrics` can be asserted to delegate to
        whatever this fake's `metrics` property currently returns, rather than to some fixed
        value baked into the fake."""

    def login(self, org_resource_id: str | None = None, *, timeout: float = 0.0) -> None:
        self._organization_id = org_resource_id if org_resource_id is not None else DEFAULT_ORG

    def reauthenticate(self, *, timeout: float = 0.0) -> None:
        # Never exercised here -- reauthenticate() tests live in
        # tests/unit/oauth/test_provider_unit.py and test_holder_unit.py; this fake only needs
        # the method to exist to satisfy the OAuthProvider protocol structurally.
        raise NotImplementedError

    @property
    def organization_id(self) -> str | None:
        return self._organization_id

    @property
    def metrics(self) -> OAuthMetrics:
        return self.metrics_value

    def close(self) -> None:
        self.closed = True


def _oauth_connect(**overrides) -> Connection:
    """connect() in oauth mode with the network-free params filled in, driving a
    `FakeOAuthProvider` instead of a real browser login.

    organization_id defaults to "" (omitted) since, unlike every other mode, auth="oauth" never
    requires it. Tests exercising the org-supplied/mismatch cases override it explicitly.
    """
    params: dict = {
        "auth": "oauth",
        "oauth_config": CONFIG,
        "oauth_provider_factory": FakeOAuthProvider,
        "environment_id": "env-1",
        "organization_id": "",
        "cloud_provider": "aws",
        "cloud_region": "us-east-1",
    }
    params.update(overrides)
    return connect(**params)


def _provider(conn: Connection) -> FakeOAuthProvider:
    """Type-narrowing helper: every oauth-mode Connection in this file is built with
    oauth_provider_factory=FakeOAuthProvider (directly, or via the holder's reuse/join path for
    a second Connection), so conn._oauth_provider is always a FakeOAuthProvider here -- this just
    gives assertions a concrete type instead of the `OAuthProvider | None` Connection declares."""
    provider = conn._oauth_provider
    assert isinstance(provider, FakeOAuthProvider)
    return provider


class TestOauthValidation:
    """connect()/Connection validation of auth="oauth" mode selection and its exclusivity."""

    def test_invalid_auth_value_raises(self):
        with pytest.raises(InterfaceError, match="auth must be 'api_key' or 'oauth'"):
            connect(
                auth="bogus",  # type: ignore[arg-type]
                environment_id="env-1",
                organization_id="org-1",
                cloud_provider="aws",
                cloud_region="us-east-1",
            )

    def test_invalid_auth_value_raises_even_with_organization_id_omitted(self):
        """Regression guard: the org-required gate inspects `auth` itself (`auth != "oauth"`), so
        an invalid `auth` value must be validated *before* that gate -- otherwise a caller who
        also omitted organization_id (no global key either) would see the misleading "Organization
        ID is required" instead of the actual mistake."""
        with pytest.raises(InterfaceError, match="auth must be 'api_key' or 'oauth'"):
            connect(
                auth="bogus",  # type: ignore[arg-type]
                environment_id="env-1",
                cloud_provider="aws",
                cloud_region="us-east-1",
            )

    def test_oauth_config_without_auth_raises_even_with_organization_id_omitted(self):
        """Same masking hazard as test_invalid_auth_value_raises_even_with_organization_id_omitted,
        for the oauth_config-without-auth="oauth" mistake specifically."""
        with pytest.raises(
            InterfaceError, match="oauth_config may only be supplied when auth='oauth'"
        ):
            connect(
                oauth_config=CONFIG,
                environment_id="env-1",
                cloud_provider="aws",
                cloud_region="us-east-1",
            )

    @pytest.mark.parametrize("auth_kwargs", [{}, {"auth": "api_key"}])
    def test_oauth_config_without_auth_oauth_raises(self, auth_kwargs):
        with pytest.raises(
            InterfaceError, match="oauth_config may only be supplied when auth='oauth'"
        ):
            connect(
                oauth_config=CONFIG,
                environment_id="env-1",
                organization_id="org-1",
                cloud_provider="aws",
                cloud_region="us-east-1",
                **auth_kwargs,
            )

    @pytest.mark.parametrize(
        "key_param",
        [
            "global_api_key",
            "global_api_secret",
            "flink_api_key",
            "flink_api_secret",
            "tableflow_api_key",
            "tableflow_api_secret",
            "connect_api_key",
            "connect_api_secret",
            "cloud_api_key",
            "cloud_api_secret",
        ],
    )
    def test_auth_oauth_mutually_exclusive_with_each_api_key_param(self, key_param):
        """auth="oauth" combined with any API-key param raises the specific exclusivity error."""
        with pytest.raises(
            InterfaceError,
            match="auth='oauth' cannot be combined with API key credentials",
        ):
            _oauth_connect(**{key_param: "some-value"})

    @pytest.mark.parametrize(
        "byoidc_kwargs",
        [
            {"external_access_token": "tok-xyz"},
            {"identity_pool_id": "pool-9"},
            {"external_access_token": "tok-xyz", "identity_pool_id": "pool-9"},
        ],
        ids=["token_only", "pool_only", "both"],
    )
    def test_auth_oauth_mutually_exclusive_with_any_byoidc_param(self, byoidc_kwargs):
        """Either BYOIDC parameter alone (not just the complete pair) trips the exclusivity error,
        before the BYOIDC pair-completeness check could report a different mistake."""
        with pytest.raises(
            InterfaceError,
            match="auth='oauth' cannot be combined with external_access_token",
        ):
            _oauth_connect(**byoidc_kwargs)

    def test_auth_oauth_does_not_require_organization_id(self):
        """Unlike every other mode, omitting organization_id under auth="oauth" is not an error --
        it's discovered from the interactive login's session instead."""
        conn = _oauth_connect()
        assert conn.organization_id == DEFAULT_ORG

    def test_organization_id_can_be_omitted_from_the_call_entirely(self):
        """organization_id defaults to "" on connect() itself, so a caller can drop the kwarg
        entirely -- not just pass organization_id="" -- under auth="oauth"."""
        conn = connect(
            auth="oauth",
            oauth_config=CONFIG,
            oauth_provider_factory=FakeOAuthProvider,
            environment_id="env-1",
            cloud_provider="aws",
            cloud_region="us-east-1",
        )
        assert conn.organization_id == DEFAULT_ORG

    def test_http_timeout_secs_reaches_the_oauth_provider(self):
        """The provider's private token-chain client must honor the caller's http_timeout_secs,
        not fall back to httpx's implicit default."""
        conn = _oauth_connect(timeouts=Timeouts(httpx_request_timeout_secs=3.5))
        assert _provider(conn).http_timeout_secs == 3.5

    def test_default_http_timeout_secs_reaches_the_oauth_provider(self):
        conn = _oauth_connect()
        assert _provider(conn).http_timeout_secs == DEFAULT_HTTP_TIMEOUT_SECS

    def test_http_user_agent_reaches_the_oauth_provider(self):
        """The provider's private token-chain client must identify as the caller's
        http_user_agent, not httpx's stock one."""
        conn = _oauth_connect(http_user_agent="my-app/1.0")
        assert _provider(conn).http_user_agent == "my-app/1.0"

    def test_default_http_user_agent_reaches_the_oauth_provider(self):
        conn = _oauth_connect()
        assert _provider(conn).http_user_agent == Connection.DEFAULT_USER_AGENT

    def test_auth_api_key_default_is_unaffected(self):
        """Regression guard: omitting auth= behaves exactly as before (api_key mode)."""
        conn = connect(
            global_api_key="gk",
            global_api_secret="gs",
            environment_id="env-1",
            organization_id="org-1",
            cloud_provider="aws",
            cloud_region="us-east-1",
        )
        assert conn._oauth is False
        assert conn._oauth_provider is None


class TestOauthAuthWiring:
    """The auth objects auth="oauth" installs on the httpx clients."""

    def test_oauth_flink_auth_is_providers_data_plane_auth(self):
        conn = _oauth_connect()
        assert conn._flink_auth is _provider(conn).data_plane_auth

    @pytest.mark.parametrize("surface", list(ControlPlaneSurface))
    def test_oauth_surface_auth_is_providers_control_plane_auth(self, surface):
        """One control-plane token reaches Tableflow, Connect, Artifacts, and CMK alike -- every
        surface shares the very same object."""
        conn = _oauth_connect()
        assert conn._auth_for(surface) is _provider(conn).control_plane_auth

    def test_oauth_global_credentials_is_none(self):
        conn = _oauth_connect()
        assert conn._global_credentials is None

    def test_oauth_byoidc_flag_is_false(self):
        conn = _oauth_connect()
        assert conn._byoidc is False

    def test_api_key_mode_controlplane_auth_is_now_a_basicauth_instance(self):
        """Regression pin for the tuple->httpx.Auth widening oauth mode needed: a global-key
        connection's control-plane auth is now an httpx.BasicAuth object, not a raw (key, secret)
        tuple."""
        conn = connect(
            global_api_key="gk",
            global_api_secret="gs",
            environment_id="env-1",
            organization_id="org-1",
            cloud_provider="aws",
            cloud_region="us-east-1",
        )
        for surface in ControlPlaneSurface:
            assert isinstance(conn._auth_for(surface), httpx.BasicAuth)


class TestOauthMetrics:
    """`Connection.oauth_metrics` -- the supported replacement for peeking at
    `_oauth_provider.metrics` directly."""

    def test_none_outside_oauth_mode(self):
        conn = connect(
            global_api_key="gk",
            global_api_secret="gs",
            environment_id="env-1",
            organization_id="org-1",
            cloud_provider="aws",
            cloud_region="us-east-1",
        )
        assert conn.oauth_metrics is None

    def test_delegates_to_the_providers_metrics(self):
        conn = _oauth_connect()
        provider = _provider(conn)
        provider.metrics_value = OAuthMetrics(refresh_chain_count=3, refresh_chain_secs=1.5)
        assert conn.oauth_metrics == provider.metrics_value


class TestOauthCmkCapabilityGain:
    """The genuine capability gain over API-key mode: CMK/Tableflow/Connect reach with no global
    key, since one control-plane token covers all three."""

    def test_resolve_kafka_cluster_id_succeeds_under_oauth_without_global_key(self):
        conn = _oauth_connect(database="mydb")

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json={
                    "data": [{"id": "lkc-123", "spec": {"display_name": "mydb"}}],
                    "metadata": {},
                },
            )

        # Install the client directly (as test_connection_byoidc_unit.py does for the Flink
        # client) rather than driving the control-plane ServiceClient's own lazy construction, so
        # the mock transport is in place before any request fires.
        conn._controlplane._client = httpx.Client(
            base_url=conn._controlplane_endpoint,
            transport=httpx.MockTransport(handler),
        )
        try:
            assert conn._resolve_kafka_cluster_id() == "lkc-123"
        finally:
            conn.close()

    def test_tableflow_and_connect_clients_build_successfully_under_oauth(self):
        """No surface's ProgrammingError fires under oauth mode -- every surface has a credential,
        and the one control-plane client builds."""
        conn = _oauth_connect()
        try:
            _ = conn._controlplane.httpx_client
            auths = [conn._auth_for(surface) for surface in ControlPlaneSurface]
        finally:
            conn.close()
        assert all(auth is _provider(conn).control_plane_auth for auth in auths)


class TestOauthOrganizationId:
    """organization_id discovery under auth="oauth" and the one-identity guard the
    holder already enforces once `connect()` actually calls into it."""

    def test_organization_id_supplied_scopes_the_session(self):
        conn = _oauth_connect(organization_id="org-X")
        assert conn.organization_id == "org-X"
        assert _provider(conn).organization_id == "org-X"

    def test_organization_id_omitted_resolved_from_session(self):
        conn = _oauth_connect(organization_id="")
        assert conn.organization_id == DEFAULT_ORG

    def test_second_connect_mismatched_organization_id_raises_interface_error(self):
        _oauth_connect(organization_id="org-X")
        with pytest.raises(
            InterfaceError, match="already has an interactive OAuth login for organization"
        ):
            _oauth_connect(organization_id="org-Y")

    def test_second_connect_omitted_organization_id_inherits_established_org(self):
        _oauth_connect(organization_id="org-X")
        conn2 = _oauth_connect(organization_id="")
        assert conn2.organization_id == "org-X"


class TestOauthClose:
    """Connection.close() under auth="oauth" never closes the shared provider -- it is
    process-shared, so tearing it down here could break another live Connection."""

    def test_close_does_not_close_the_shared_provider(self):
        conn = _oauth_connect()
        provider = _provider(conn)

        conn.close()

        assert provider.closed is False

    def test_close_is_idempotent(self):
        conn = _oauth_connect()
        conn.close()
        conn.close()
        assert conn.is_closed


class _FlakyReauthAuth(httpx.Auth):
    """Raises `ReauthenticationRequired` on its first use, then stamps a bearer normally --
    the shape a real oauth-mode auth view takes once a request hits the wall and someone (the
    driver's "auto" policy, or the app itself under "raise") recovers the session."""

    def __init__(self) -> None:
        self.calls = 0

    def auth_flow(self, request: httpx.Request):
        self.calls += 1
        if self.calls == 1:
            raise ReauthenticationRequired("session dead", ReauthenticationReason.ABSOLUTE_EXPIRY)
        request.headers["Authorization"] = "Bearer fresh-token"
        yield request


def _connection_with_flaky_flink_auth(
    reauth: Literal["auto", "raise"],
) -> tuple[Connection, _FlakyReauthAuth]:
    """A plain api_key-mode Connection whose Flink client is swapped for one backed by
    `_FlakyReauthAuth`, so `_send_with_reauth_policy` can be exercised directly without a
    real oauth-mode login. `reauth` is poked onto the private attribute rather than threaded
    through connect()'s validation, since that validation (tested separately below) only allows
    a non-default `reauth` together with `auth="oauth"`.
    """
    conn = connect(
        global_api_key="gk",
        global_api_secret="gs",
        environment_id="env-1",
        organization_id="org-1",
        cloud_provider="aws",
        cloud_region="us-east-1",
    )
    conn._reauth_policy = reauth  # noqa: SLF001
    auth = _FlakyReauthAuth()
    conn._dataplane._client = httpx.Client(  # noqa: SLF001
        auth=auth,
        base_url="https://flink.example.confluent.cloud",
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json={})),
    )
    return conn, auth


class TestReauthPolicy:
    """The `reauth=` policy knob and `_send_with_reauth_policy`, the chokepoint that
    applies it whenever a request raises `ReauthenticationRequired`."""

    def test_validation_rejects_an_invalid_value(self):
        with pytest.raises(InterfaceError, match="reauth must be 'auto' or 'raise'"):
            connect(
                reauth="bogus",  # type: ignore[arg-type]
                environment_id="env-1",
                organization_id="org-1",
                cloud_provider="aws",
                cloud_region="us-east-1",
            )

    def test_validation_rejects_a_non_default_value_combined_with_api_key_mode(self):
        with pytest.raises(InterfaceError, match="reauth may only be supplied when auth='oauth'"):
            connect(
                reauth="raise",
                global_api_key="gk",
                global_api_secret="gs",
                environment_id="env-1",
                organization_id="org-1",
                cloud_provider="aws",
                cloud_region="us-east-1",
            )

    def test_oauth_connection_defaults_reauth_to_auto(self):
        conn = _oauth_connect()
        assert conn._reauth_policy == "auto"  # noqa: SLF001

    def test_oauth_connection_honors_an_explicit_reauth(self):
        conn = _oauth_connect(reauth="raise")
        assert conn._reauth_policy == "raise"  # noqa: SLF001

    def test_auto_policy_reauthenticates_and_retries_transparently(self, monkeypatch):
        conn, auth = _connection_with_flaky_flink_auth("auto")
        reauth_mock = Mock()
        monkeypatch.setattr("confluent_sql.connection.oauth_reauthenticate", reauth_mock)
        try:
            response = conn._dataplane.request("/some/path")
        finally:
            conn.close()

        assert response.status_code == 200
        reauth_mock.assert_called_once_with()
        assert auth.calls == 2

    def test_raise_policy_leaves_the_exception_unhandled_and_unretried(self, monkeypatch):
        conn, auth = _connection_with_flaky_flink_auth("raise")
        reauth_mock = Mock()
        monkeypatch.setattr("confluent_sql.connection.oauth_reauthenticate", reauth_mock)
        try:
            with pytest.raises(ReauthenticationRequired):
                conn._dataplane.request("/some/path")
        finally:
            conn.close()

        reauth_mock.assert_not_called()
        assert auth.calls == 1

    def test_artifact_get_honors_the_auto_policy(self, monkeypatch):
        """Artifact GETs take the retrying path (via `_send_untranslated`), which must go through
        `_send_with_reauth_policy` like every other request path."""
        conn, auth = _connection_with_flaky_flink_auth("auto")
        conn._controlplane._client = httpx.Client(  # noqa: SLF001
            base_url="https://api.example.confluent.cloud",
            transport=httpx.MockTransport(lambda request: httpx.Response(200, json={})),
        )
        conn._surface_auth[ControlPlaneSurface.ARTIFACTS] = auth  # noqa: SLF001
        reauth_mock = Mock()
        monkeypatch.setattr("confluent_sql.connection.oauth_reauthenticate", reauth_mock)
        try:
            response = conn.get_artifacts_api()._request("/artifact/v1/flink-artifacts")
        finally:
            conn.close()

        assert response.status_code == 200
        reauth_mock.assert_called_once_with()
        assert auth.calls == 2

    @pytest.mark.parametrize("policy", ["auto", "raise"])
    def test_retried_control_plane_get_applies_the_reauth_policy_once(
        self, monkeypatch, policy: Literal["auto", "raise"]
    ):
        """`ReauthenticationRequired` is not a transport error, so the GET retry layer must neither
        swallow nor repeat it: `auto` reauthenticates exactly once inside the one attempt, and
        `raise` propagates after a single attempt."""
        conn, auth = _connection_with_flaky_flink_auth(policy)
        conn._controlplane._client = httpx.Client(  # noqa: SLF001
            base_url="https://api.example.confluent.cloud",
            transport=httpx.MockTransport(lambda request: httpx.Response(200, json={})),
        )
        conn._surface_auth[ControlPlaneSurface.CONNECT] = auth  # noqa: SLF001
        reauth_mock = Mock()
        monkeypatch.setattr("confluent_sql.connection.oauth_reauthenticate", reauth_mock)
        try:
            if policy == "raise":
                with pytest.raises(ReauthenticationRequired):
                    conn.get_connector_api()._request("/connect/v1/some/path")
                reauth_mock.assert_not_called()
                assert auth.calls == 1
            else:
                response = conn.get_connector_api()._request("/connect/v1/some/path")
                assert response.status_code == 200
                reauth_mock.assert_called_once_with()
                assert auth.calls == 2
        finally:
            conn.close()
