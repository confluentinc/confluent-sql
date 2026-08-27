"""Unit tests for wiring interactive human-login OAuth through connect() (#155).

#150's OAuth machinery (PKCE primitives #151, callback server #152, the CCloudOAuth provider
#153, the process-wide holder #154) is exercised end to end by the tests under tests/unit/oauth/.
This file tests only the assembly #155 adds on top: auth="oauth" mode selection/validation on
connect()/Connection, and wiring the acquired provider's two httpx.Auth views onto the driver's
three clients.

Every test here drives a FakeOAuthProvider injected through the internal oauth_provider_factory
testing hook (mirroring oauth.acquire()'s own provider_factory seam) rather than a real browser
login -- see tests/unit/oauth/test_holder_unit.py's FakeProvider for the same pattern one layer
down.
"""

from __future__ import annotations

from unittest.mock import Mock

import httpx
import pytest

from confluent_sql import InterfaceError, connect
from confluent_sql.connection import Connection
from confluent_sql.oauth.config import CCloudOAuthConfig

pytestmark = pytest.mark.unit

CONFIG = CCloudOAuthConfig(
    auth_service_domain="login.confluent.io",
    api_host="https://confluent.cloud",
    client_id="test-client-id",
    callback_host="127.0.0.1",
    callback_port=26640,
    callback_path="/gateway/v1/callback-local-mcp-docs",
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
    actually work -- they're exactly what #155 wires onto the driver's clients.
    """

    def __init__(self, config: CCloudOAuthConfig) -> None:
        self.config = config
        self._organization_id: str | None = None
        self.data_plane_auth = _MarkerAuth("dp")
        self.control_plane_auth = _MarkerAuth("cp")
        self.closed = False

    def login(self, org_resource_id: str | None = None, *, timeout: float = 0.0) -> None:
        self._organization_id = org_resource_id if org_resource_id is not None else DEFAULT_ORG

    @property
    def organization_id(self) -> str | None:
        return self._organization_id

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
        ],
    )
    def test_auth_oauth_mutually_exclusive_with_each_api_key_param(self, key_param):
        """auth="oauth" combined with any API-key param raises the specific exclusivity error."""
        with pytest.raises(
            InterfaceError,
            match="auth='oauth' cannot be combined with API key credentials",
        ):
            _oauth_connect(**{key_param: "some-value"})

    def test_auth_oauth_mutually_exclusive_with_byoidc_pair(self):
        with pytest.raises(
            InterfaceError,
            match="auth='oauth' cannot be combined with external_access_token",
        ):
            _oauth_connect(external_access_token="tok-xyz", identity_pool_id="pool-9")

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

    def test_oauth_controlplane_auth_is_providers_control_plane_auth(self):
        conn = _oauth_connect()
        assert conn._controlplane_auth is _provider(conn).control_plane_auth

    def test_oauth_connect_auth_is_providers_control_plane_auth(self):
        """One control-plane token reaches Tableflow, Connect, and CMK alike -- both control-plane
        slots share the very same object."""
        conn = _oauth_connect()
        assert conn._connect_auth is _provider(conn).control_plane_auth
        assert conn._connect_auth is conn._controlplane_auth

    def test_oauth_global_credentials_is_none(self):
        conn = _oauth_connect()
        assert conn._global_credentials is None

    def test_oauth_byoidc_flag_is_false(self):
        conn = _oauth_connect()
        assert conn._byoidc is False

    def test_api_key_mode_controlplane_auth_is_now_a_basicauth_instance(self):
        """Regression pin for the tuple->httpx.Auth widening #155 needed: a global-key connection's
        control-plane auth is now an httpx.BasicAuth object, not a raw (key, secret) tuple."""
        conn = connect(
            global_api_key="gk",
            global_api_secret="gs",
            environment_id="env-1",
            organization_id="org-1",
            cloud_provider="aws",
            cloud_region="us-east-1",
        )
        assert isinstance(conn._controlplane_auth, httpx.BasicAuth)
        assert isinstance(conn._connect_auth, httpx.BasicAuth)


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
        # client) rather than driving _get_controlplane_client()'s own lazy construction, so the
        # mock transport is in place before any request fires.
        conn._controlplane_client = httpx.Client(
            auth=conn._controlplane_auth,
            base_url=conn._controlplane_endpoint,
            transport=httpx.MockTransport(handler),
        )
        try:
            assert conn._resolve_kafka_cluster_id() == "lkc-123"
        finally:
            conn.close()

    def test_tableflow_and_connect_clients_build_successfully_under_oauth(self):
        """Neither guard's ProgrammingError fires under oauth mode -- _controlplane_auth and
        _connect_auth are never None here."""
        conn = _oauth_connect()
        try:
            controlplane_client = conn._get_controlplane_client()
            connect_client = conn._get_connect_controlplane_client()
        finally:
            conn.close()
        assert controlplane_client.auth is _provider(conn).control_plane_auth
        assert connect_client.auth is _provider(conn).control_plane_auth


class TestOauthOrganizationId:
    """organization_id discovery under auth="oauth" (#155), and the one-identity guard #154's
    holder already enforces once #155 actually calls into it."""

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
    """Connection.close() under auth="oauth" releases the holder's hold, never the shared
    provider itself (#155) -- the provider is process-shared, so tearing it down here could break
    another live Connection."""

    def test_close_calls_holder_release_not_provider_close(self, monkeypatch):
        conn = _oauth_connect()
        provider = _provider(conn)
        release_mock = Mock()
        monkeypatch.setattr("confluent_sql.connection.release", release_mock)

        conn.close()

        release_mock.assert_called_once_with()
        assert provider.closed is False

    def test_close_is_idempotent(self):
        conn = _oauth_connect()
        conn.close()
        conn.close()
        assert conn.is_closed

    def test_non_oauth_close_does_not_touch_oauth_release(self, monkeypatch):
        release_mock = Mock()
        monkeypatch.setattr("confluent_sql.connection.release", release_mock)
        conn = connect(
            global_api_key="gk",
            global_api_secret="gs",
            environment_id="env-1",
            organization_id="org-1",
            cloud_provider="aws",
            cloud_region="us-east-1",
        )

        conn.close()

        release_mock.assert_not_called()
