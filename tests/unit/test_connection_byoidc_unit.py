"""Unit tests for BYOIDC bearer-token authentication (#100).

BYOIDC lets a caller authenticate to the Flink data plane with a bearer token minted by their own
OAuth/OIDC identity provider plus a Confluent-Identity-Pool-Id, in place of API key + secret. It
reaches Flink only; every control-plane surface fails closed (see test_*_fail_closed below).
"""

import httpx
import pytest

from confluent_sql import InterfaceError, ProgrammingError, connect
from confluent_sql.auth import FlinkBearerAuth
from confluent_sql.connection import Connection
from confluent_sql.execution_mode import ExecutionMode

pytestmark = pytest.mark.unit


def _byoidc_connect(**overrides) -> Connection:
    """connect() in BYOIDC mode with the network-free params filled in.

    Only the BYOIDC pair and location params are supplied; every API-key param is deliberately
    omitted so the baseline is a pure BYOIDC connection. Tests exercising the mutual-exclusion
    guard add exactly one API-key param via ``overrides`` to isolate what they mean to trip.
    """
    params: dict = {
        "external_access_token": "tok-xyz",
        "identity_pool_id": "pool-9",
        "environment_id": "env-1",
        "organization_id": "org-1",
        "cloud_provider": "aws",
        "cloud_region": "us-east-1",
    }
    params.update(overrides)
    return connect(**params)


class TestByoidcValidation:
    """connect()/Connection validation of the BYOIDC parameter pair and its exclusivity."""

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
    def test_external_access_token_mutually_exclusive_with_each_api_key_param(self, key_param):
        """A bearer token combined with any API-key param raises the specific exclusivity error."""
        with pytest.raises(
            InterfaceError,
            match="external_access_token cannot be combined with API key credentials",
        ):
            _byoidc_connect(**{key_param: "some-value"})

    def test_external_access_token_without_identity_pool_id_raises(self):
        """A half-supplied BYOIDC pair (token without pool) raises the specific pairing error."""
        with pytest.raises(
            InterfaceError,
            match="external_access_token and identity_pool_id must be provided together",
        ):
            connect(
                external_access_token="tok-xyz",
                identity_pool_id=None,
                environment_id="env-1",
                organization_id="org-1",
                cloud_provider="aws",
                cloud_region="us-east-1",
            )

    def test_identity_pool_id_without_external_access_token_raises(self):
        """A half-supplied BYOIDC pair (pool without token) raises the specific pairing error."""
        with pytest.raises(
            InterfaceError,
            match="external_access_token and identity_pool_id must be provided together",
        ):
            connect(
                external_access_token=None,
                identity_pool_id="pool-9",
                environment_id="env-1",
                organization_id="org-1",
                cloud_provider="aws",
                cloud_region="us-east-1",
            )

    def test_byoidc_requires_organization_id(self):
        """BYOIDC has no control-plane reach to infer org, so omitting it raises (like a Flink-only
        key). global keys are absent by construction, so the org-required gate fires."""
        with pytest.raises(InterfaceError, match="Organization ID is required"):
            _byoidc_connect(organization_id="")


class TestByoidcAuthWiring:
    """The auth objects each mode installs on the httpx clients."""

    def test_byoidc_installs_flink_bearer_auth(self):
        """BYOIDC mode makes _flink_auth a FlinkBearerAuth, not an httpx.BasicAuth."""
        conn = _byoidc_connect()
        assert isinstance(conn._flink_auth, FlinkBearerAuth)

    def test_api_key_mode_still_uses_basic_auth(self):
        """Regression guard: an API-key connection still authenticates Flink with httpx.BasicAuth,
        unchanged by the BYOIDC branch."""
        conn = connect(
            flink_api_key="valid-key",
            flink_api_secret="valid-secret",
            environment_id="env-1",
            organization_id="org-1",
            cloud_provider="aws",
            cloud_region="us-east-1",
        )
        assert isinstance(conn._flink_auth, httpx.BasicAuth)

    def test_global_key_mode_control_plane_uses_basic_auth(self):
        """Regression guard: a global-key connection builds its control-plane client with
        httpx.BasicAuth, unchanged by the BYOIDC branch."""
        conn = connect(
            global_api_key="gk",
            global_api_secret="gs",
            environment_id="env-1",
            organization_id="org-1",
            cloud_provider="aws",
            cloud_region="us-east-1",
        )
        assert isinstance(conn._get_controlplane_client().auth, httpx.BasicAuth)


class TestByoidcFlinkRequest:
    """The outbound Flink request a BYOIDC connection actually sends."""

    def test_flink_request_carries_bearer_and_identity_pool_headers_and_path(self):
        """Driving a real GET through the connection's flink client (via a mock transport, so
        FlinkBearerAuth.auth_flow actually runs) stamps both headers and hits the org/env path."""
        conn = _byoidc_connect()
        captured: dict[str, httpx.Request] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["request"] = request
            return httpx.Response(200, json={"data": [], "metadata": {}})

        # Build base_url the way _get_flink_client() does rather than instantiating a real client
        # (with its own connection pool) just to read it back and then discard it unclosed.
        base_url = (
            f"{conn._flink_endpoint}/sql/v1/organizations/{conn.organization_id}"
            f"/environments/{conn.environment_id}"
        )
        conn._flink_client = httpx.Client(
            auth=conn._flink_auth,
            base_url=base_url,
            transport=httpx.MockTransport(handler),
        )

        try:
            conn.list_statements()
        finally:
            conn.close()  # closes the live _flink_client the test installed above

        request = captured["request"]
        assert request.headers["Authorization"] == "Bearer tok-xyz"
        assert request.headers["Confluent-Identity-Pool-Id"] == "pool-9"
        assert "/sql/v1/organizations/org-1/environments/env-1" in str(request.url)


class TestByoidcControlPlaneFailsClosed:
    """Every control-plane surface fails closed under BYOIDC with an honest, API-key-naming
    error."""

    def test_tableflow_client_raises_byoidc_programming_error(self):
        conn = _byoidc_connect()
        with pytest.raises(ProgrammingError, match="BYOIDC bearer token"):
            conn._get_controlplane_client()

    def test_connect_client_raises_byoidc_programming_error(self):
        conn = _byoidc_connect()
        with pytest.raises(ProgrammingError, match="BYOIDC bearer token"):
            conn._get_connect_controlplane_client()

    def test_cmk_cluster_id_resolution_raises_byoidc_programming_error(self):
        conn = _byoidc_connect(database="mydb")
        with pytest.raises(ProgrammingError, match="BYOIDC bearer token"):
            conn._resolve_kafka_cluster_id()


class TestByoidcDatabaseOptional:
    """database is optional under BYOIDC. Omitted, the connection still constructs and runs Flink
    statements (no default-database property); supplied, its name seeds sql.current-database with no
    CMK lookup -- the name->lkc-id lookup that fails closed is Tableflow-only. The README's BYOIDC
    example (database shown as optional) rests on both halves."""

    def test_constructs_without_database(self):
        """A BYOIDC connection built with no database name constructs and reports no database."""
        conn = _byoidc_connect()
        assert conn._database is None

    @pytest.mark.parametrize(
        "database, expected_current_database",
        [
            pytest.param(None, None, id="no-database"),
            pytest.param("mydb", "mydb", id="with-database"),
        ],
    )
    def test_current_database_property_tracks_database_param(
        self, database, expected_current_database
    ):
        """sql.current-database rides along only when a database name was given -- and resolving it
        under BYOIDC never trips the control-plane fail-closed guard (no CMK lookup involved)."""
        conn = _byoidc_connect(database=database)

        properties = conn._resolve_properties(None, ExecutionMode.SNAPSHOT)

        assert properties["sql.current-catalog"] == "env-1"
        if expected_current_database is None:
            assert "sql.current-database" not in properties
        else:
            assert properties["sql.current-database"] == expected_current_database
