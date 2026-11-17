"""Unit tests for `ConnectionCredentials`, the collaborator that owns a `Connection`'s
credential mode: which `httpx.Auth` each plane and control-plane surface carries, and what happens
when an oauth session can no longer be refreshed (#290).

`Connection`-level behavior (error precedence in `__init__`, the oauth login callback, ...) is
covered by the `test_connection_*_unit.py` modules; this file drives the collaborator directly.
"""

from __future__ import annotations

import dataclasses
from unittest.mock import Mock, patch

import httpx
import pytest

from confluent_sql import (
    InterfaceError,
    InternalError,
    ProgrammingError,
    ReauthenticationReason,
    ReauthenticationRequired,
)
from confluent_sql.auth import FlinkBearerAuth
from confluent_sql.credentials import (
    ApiKeyParams,
    ByoidcParams,
    ConnectionCredentials,
    ControlPlaneSurface,
    resolve_oauth_config,
)

GLOBAL_KEY = {"global_api_key": "gk", "global_api_secret": "gs"}
FLINK_KEY = {"flink_api_key": "fk", "flink_api_secret": "fs"}
BYOIDC = {"external_access_token": "tok", "identity_pool_id": "pool-1"}


def _api_key_auth(
    *, external_access_token: str | None = None, identity_pool_id: str | None = None, **keys: str
) -> ConnectionCredentials:
    """A `ConnectionCredentials` configured from flat keywords, the way `Connection` spells them."""
    auth = ConnectionCredentials()
    auth.configure_api_key_mode(
        byoidc=ByoidcParams(external_access_token, identity_pool_id), keys=ApiKeyParams(**keys)
    )
    return auth


def _fake_provider() -> Mock:
    return Mock(name="provider", data_plane_auth=Mock(), control_plane_auth=Mock())


def _reauth_required() -> ReauthenticationRequired:
    return ReauthenticationRequired("session dead", ReauthenticationReason.ABSOLUTE_EXPIRY)


class TestUnconfigured:
    def test_reports_no_oauth_login(self) -> None:
        auth = ConnectionCredentials()

        assert not auth.is_oauth
        assert auth.oauth_metrics is None

    def test_credentials_before_configuration_are_a_driver_bug(self) -> None:
        auth = ConnectionCredentials()

        with pytest.raises(InternalError):
            auth.flink_auth  # noqa: B018
        with pytest.raises(InternalError):
            auth.for_surface(ControlPlaneSurface.CMK)


class TestApiKeyMode:
    def test_global_key_authenticates_every_plane_and_surface(self) -> None:
        auth = _api_key_auth(**GLOBAL_KEY)

        assert isinstance(auth.flink_auth, httpx.BasicAuth)
        assert not auth.is_oauth
        for surface in ControlPlaneSurface:
            assert isinstance(auth.for_surface(surface), httpx.BasicAuth)

    def test_flink_only_key_reaches_no_control_plane_surface(self) -> None:
        auth = _api_key_auth(**FLINK_KEY)

        assert isinstance(auth.flink_auth, httpx.BasicAuth)
        with pytest.raises(ProgrammingError, match="global API key"):
            auth.for_surface(ControlPlaneSurface.CMK)
        with pytest.raises(ProgrammingError, match="tableflow_api_key/tableflow_api_secret"):
            auth.for_surface(ControlPlaneSurface.TABLEFLOW)

    def test_dedicated_pair_reaches_only_its_own_surface(self) -> None:
        auth = _api_key_auth(connect_api_key="ck", connect_api_secret="cs", **FLINK_KEY)

        assert isinstance(auth.for_surface(ControlPlaneSurface.CONNECT), httpx.BasicAuth)
        with pytest.raises(ProgrammingError, match="Artifacts require"):
            auth.for_surface(ControlPlaneSurface.ARTIFACTS)

    def test_no_usable_credential_is_rejected_at_configuration(self) -> None:
        with pytest.raises(InterfaceError, match="global_api_key/global_api_secret or flink_api"):
            _api_key_auth()


class TestByoidcMode:
    def test_bearer_token_on_flink_and_fail_closed_control_plane(self) -> None:
        auth = _api_key_auth(**BYOIDC)

        assert isinstance(auth.flink_auth, FlinkBearerAuth)
        assert not auth.is_oauth
        for surface in ControlPlaneSurface:
            with pytest.raises(ProgrammingError, match="BYOIDC"):
                auth.for_surface(surface)

    def test_mutually_exclusive_with_api_keys(self) -> None:
        with pytest.raises(InterfaceError, match="mutually exclusive"):
            _api_key_auth(**BYOIDC, **GLOBAL_KEY)

    def test_pair_must_be_supplied_together(self) -> None:
        with pytest.raises(InterfaceError, match="must be provided together"):
            _api_key_auth(external_access_token="tok")


class TestOAuthMode:
    def test_adopted_provider_supplies_every_credential(self) -> None:
        auth = ConnectionCredentials()
        provider = _fake_provider()

        auth.adopt_oauth_provider(provider)

        assert auth.is_oauth
        assert auth.flink_auth is provider.data_plane_auth
        for surface in ControlPlaneSurface:
            assert auth.for_surface(surface) is provider.control_plane_auth
        assert auth.oauth_metrics is provider.metrics

    def test_adoption_replaces_earlier_credentials(self) -> None:
        auth = _api_key_auth(**GLOBAL_KEY)
        provider = _fake_provider()

        auth.adopt_oauth_provider(provider)

        assert auth.flink_auth is provider.data_plane_auth


class TestSendWithPolicy:
    def test_passes_the_response_through(self) -> None:
        response = Mock(spec=httpx.Response)

        assert ConnectionCredentials().send_with_policy(lambda: response) is response

    def test_raise_policy_propagates_reauthentication_required(self) -> None:
        auth = ConnectionCredentials(reauth="raise")
        do_request = Mock(side_effect=_reauth_required())

        with (
            patch("confluent_sql.credentials.oauth_reauthenticate") as reauthenticate,
            pytest.raises(ReauthenticationRequired),
        ):
            auth.send_with_policy(do_request)

        reauthenticate.assert_not_called()
        do_request.assert_called_once()

    def test_auto_policy_reauthenticates_then_retries_once(self) -> None:
        auth = ConnectionCredentials(reauth="auto")
        response = Mock(spec=httpx.Response)
        do_request = Mock(side_effect=[_reauth_required(), response])

        with patch("confluent_sql.credentials.oauth_reauthenticate") as reauthenticate:
            assert auth.send_with_policy(do_request) is response

        reauthenticate.assert_called_once_with()
        assert do_request.call_count == 2

    def test_auto_policy_does_not_retry_a_second_time(self) -> None:
        auth = ConnectionCredentials(reauth="auto")
        do_request = Mock(side_effect=[_reauth_required(), _reauth_required()])

        with (
            patch("confluent_sql.credentials.oauth_reauthenticate"),
            pytest.raises(ReauthenticationRequired),
        ):
            auth.send_with_policy(do_request)

        assert do_request.call_count == 2


class TestReauthRaises:
    def test_forces_raise_inside_and_restores_the_configured_policy(self) -> None:
        auth = ConnectionCredentials(reauth="auto")

        with auth.reauth_raises():
            assert auth.reauth_policy == "raise"

        assert auth.reauth_policy == "auto"

    def test_restores_the_policy_when_the_body_raises(self) -> None:
        auth = ConnectionCredentials(reauth="auto")

        with pytest.raises(RuntimeError, match="boom"), auth.reauth_raises():
            raise RuntimeError("boom")

        assert auth.reauth_policy == "auto"


ALL_KEY_FIELDS = [field.name for field in dataclasses.fields(ApiKeyParams)]


class TestApiKeyParams:
    def test_every_field_is_a_key_or_secret_and_all_default_to_absent(self) -> None:
        assert len(ALL_KEY_FIELDS) == 10
        assert all(name.endswith(("_api_key", "_api_secret")) for name in ALL_KEY_FIELDS)
        assert not ApiKeyParams().any_supplied

    @pytest.mark.parametrize("name", ALL_KEY_FIELDS)
    def test_any_one_field_counts_as_supplied(self, name: str) -> None:
        assert ApiKeyParams(**{name: "x"}).any_supplied

    def test_empty_strings_count_as_absent(self) -> None:
        assert not ApiKeyParams(**dict.fromkeys(ALL_KEY_FIELDS, "")).any_supplied

    @pytest.mark.parametrize(
        ("key", "secret", "material"),
        [("gk", "gs", True), ("gk", None, True), (None, "gs", True), ("", "", False)],
    )
    def test_global_material_is_either_half_of_the_pair(
        self, key: str | None, secret: str | None, material: bool
    ) -> None:
        keys = ApiKeyParams(global_api_key=key, global_api_secret=secret)

        assert keys.has_global_material is material

    def test_global_pair_needs_both_halves(self) -> None:
        assert ApiKeyParams(**GLOBAL_KEY).global_pair == ("gk", "gs")
        assert ApiKeyParams(global_api_key="gk").global_pair is None
        assert ApiKeyParams().global_pair is None

    def test_service_pairs_follow_the_global_then_dedicated_rule(self) -> None:
        dedicated = ApiKeyParams(
            tableflow_api_key="tk",
            tableflow_api_secret="ts",
            connect_api_key="ck",
            connect_api_secret="cs",
            cloud_api_key="ak",
            cloud_api_secret="as",
        )
        assert dedicated.tableflow_pair == ("tk", "ts")
        assert dedicated.connect_pair == ("ck", "cs")
        assert dedicated.cloud_pair == ("ak", "as")
        assert ApiKeyParams(**GLOBAL_KEY, **{"tableflow_api_key": "tk"}).tableflow_pair == (
            "gk",
            "gs",
        )
        assert ApiKeyParams().cloud_pair is None

    def test_half_supplied_dedicated_pair_raises_naming_the_service(self) -> None:
        with pytest.raises(InterfaceError, match="connect_api_key and connect_api_secret"):
            _ = ApiKeyParams(connect_api_key="ck").connect_pair


class TestResolveOAuthConfig:
    @staticmethod
    def _resolve(
        auth: str = "oauth",
        *,
        byoidc: ByoidcParams | None = None,
        keys: ApiKeyParams | None = None,
        reauth: str = "auto",
    ):
        return resolve_oauth_config(
            auth,
            oauth_config=None,
            reauth=reauth,
            byoidc=byoidc or ByoidcParams(),
            keys=keys or ApiKeyParams(),
        )

    def test_api_key_mode_selects_no_oauth(self) -> None:
        assert self._resolve("api_key") is None

    def test_oauth_mode_returns_a_config(self) -> None:
        assert self._resolve("oauth") is not None

    @pytest.mark.parametrize("name", ALL_KEY_FIELDS)
    def test_oauth_rejects_any_single_api_key_parameter(self, name: str) -> None:
        with pytest.raises(InterfaceError, match="API key credentials"):
            self._resolve(keys=ApiKeyParams(**{name: "x"}))

    @pytest.mark.parametrize("name", ["external_access_token", "identity_pool_id"])
    def test_oauth_rejects_either_byoidc_parameter(self, name: str) -> None:
        with pytest.raises(InterfaceError, match="BYOIDC"):
            self._resolve(byoidc=ByoidcParams(**{name: "x"}))
