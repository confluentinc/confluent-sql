"""Credential and authentication wiring for a `Connection` (#290).

A `Connection` authenticates in one of three modes -- API key, BYOIDC bearer token, or interactive
OAuth login -- and the mode decides which `httpx.Auth` the Flink data plane carries, which each
control-plane surface carries (or whether it fails closed), and what to do when an oauth session
can no longer be refreshed. `ConnectionCredentials` resolves that once and answers those
questions, so `Connection` holds one collaborator instead of the half-dozen attributes that used to
encode them.

The module-level `_resolve_*` helpers are the pure, network-free validation of the
credential-bearing `connect()` parameters.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Generator
from contextlib import contextmanager
from dataclasses import dataclass, fields
from enum import Enum
from typing import Literal

import httpx

from .auth import FlinkBearerAuth
from .exceptions import (
    InterfaceError,
    InternalError,
    ProgrammingError,
    ReauthenticationRequired,
)
from .oauth.config import PROD, CCloudOAuthConfig
from .oauth.holder import reauthenticate as oauth_reauthenticate
from .oauth.provider import OAuthMetrics, OAuthProvider

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ByoidcParams:
    """The BYOIDC pair of `connect()` parameters, as supplied (empty strings count as absent)."""

    external_access_token: str | None = None
    identity_pool_id: str | None = None

    @property
    def any_supplied(self) -> bool:
        return bool(self.external_access_token or self.identity_pool_id)


@dataclass(frozen=True)
class ApiKeyParams:
    """Every API key/secret parameter of `connect()`, as supplied (empty strings count as absent).

    Built once at the top of `Connection.__init__` so the credential-bearing parameters travel as
    one value rather than being re-spelled at every hop. Holds no validation of its own; the pair
    rules live in the `*_pair` properties and `_resolve_*` helpers. A new key pair is a new field
    pair here (and its own `*_pair` property), not a new argument at every call site.
    """

    global_api_key: str | None = None
    global_api_secret: str | None = None
    flink_api_key: str | None = None
    flink_api_secret: str | None = None
    tableflow_api_key: str | None = None
    tableflow_api_secret: str | None = None
    connect_api_key: str | None = None
    connect_api_secret: str | None = None
    cloud_api_key: str | None = None
    cloud_api_secret: str | None = None

    @property
    def any_supplied(self) -> bool:
        """True if any key or secret parameter is non-empty -- what makes API-key credentials
        mutually exclusive with BYOIDC and oauth."""
        # Every field of this class is a key or a secret; a field that isn't would be wrongly
        # counted here. test_every_field_is_a_key_or_secret... enforces that.
        return any(getattr(self, field.name) for field in fields(self))

    @property
    def has_global_material(self) -> bool:
        """True if either half of the global pair is non-empty, even a half-supplied pair. The
        distinction from `global_pair` matters: a half-supplied pair is a credential mistake to be
        reported as such, not a reason to mis-diagnose some other omission."""
        return bool(self.global_api_key) or bool(self.global_api_secret)

    @property
    def global_pair(self) -> tuple[str, str] | None:
        """The complete global (key, secret) pair, or None if either half is missing."""
        if self.global_api_key and self.global_api_secret:
            return (self.global_api_key, self.global_api_secret)
        return None

    @property
    def tableflow_pair(self) -> tuple[str, str] | None:
        """The (key, secret) the Tableflow routes authenticate with: the complete global pair if
        there is one, else the dedicated `tableflow_api_key/tableflow_api_secret` pair, else None.

        Unlike the other properties here this can raise: a half-supplied dedicated pair is
        rejected rather than treated as absent (a complete global pair short-circuits that check).

        Raises:
            InterfaceError: If no complete global pair is supplied and the dedicated pair is
                half-supplied.
        """
        return _resolve_service_credentials(
            "tableflow",
            self.global_api_key,
            self.global_api_secret,
            self.tableflow_api_key,
            self.tableflow_api_secret,
        )

    @property
    def connect_pair(self) -> tuple[str, str] | None:
        """The (key, secret) the Connect routes authenticate with: the complete global pair if
        there is one, else the dedicated `connect_api_key/connect_api_secret` pair, else None.

        Unlike the other properties here this can raise: a half-supplied dedicated pair is
        rejected rather than treated as absent (a complete global pair short-circuits that check).

        Raises:
            InterfaceError: If no complete global pair is supplied and the dedicated pair is
                half-supplied.
        """
        return _resolve_service_credentials(
            "connect",
            self.global_api_key,
            self.global_api_secret,
            self.connect_api_key,
            self.connect_api_secret,
        )

    @property
    def cloud_pair(self) -> tuple[str, str] | None:
        """The (key, secret) the Artifacts routes authenticate with: the complete global pair if
        there is one, else the dedicated `cloud_api_key/cloud_api_secret` pair, else None.

        Unlike the other properties here this can raise: a half-supplied dedicated pair is
        rejected rather than treated as absent (a complete global pair short-circuits that check).

        Raises:
            InterfaceError: If no complete global pair is supplied and the dedicated pair is
                half-supplied.
        """
        return _resolve_service_credentials(
            "cloud",
            self.global_api_key,
            self.global_api_secret,
            self.cloud_api_key,
            self.cloud_api_secret,
        )


def _resolve_api_credentials(
    global_api_key: str | None,
    global_api_secret: str | None,
    flink_api_key: str | None,
    flink_api_secret: str | None,
) -> tuple[str, str]:
    """Pick the (key, secret) pair a connection authenticates with.

    A 'global' Confluent Cloud API key works against every route this driver touches (Flink
    today, Tableflow and friends later), whereas a Flink-region key only works against Flink.
    The global pair therefore wins when both are supplied. Either pair may be omitted, but a
    half-supplied pair (key without secret, or vice versa) is a configuration mistake worth
    surfacing rather than silently treating as absent.

    Empty strings count as absent (matching how the rest of connect() treats blank parameters).

    Raises:
        InterfaceError: If either pair is half-supplied, or if neither pair is fully supplied.
    """
    global_key, global_secret = global_api_key or "", global_api_secret or ""
    flink_key, flink_secret = flink_api_key or "", flink_api_secret or ""

    if bool(global_key) != bool(global_secret):
        raise InterfaceError("global_api_key and global_api_secret must be provided together")
    if bool(flink_key) != bool(flink_secret):
        raise InterfaceError("flink_api_key and flink_api_secret must be provided together")

    if global_key:
        if flink_key:
            logger.warning(
                "Both global and Flink API credentials provided; using the global credentials "
                "and ignoring flink_api_key/flink_api_secret."
            )
        return (global_key, global_secret)

    if flink_key:
        return (flink_key, flink_secret)

    raise InterfaceError(
        "Either global_api_key/global_api_secret or flink_api_key/flink_api_secret must be provided"
    )


def _resolve_service_credentials(
    config_prefix: str,
    global_api_key: str | None,
    global_api_secret: str | None,
    service_api_key: str | None,
    service_api_secret: str | None,
) -> tuple[str, str] | None:
    """Pick the (key, secret) pair a control-plane service's routes authenticate with.

    `service` names the dedicated pair ("tableflow", "connect", "cloud") in error messages. A
    complete global pair covers every route, so it wins. Otherwise the service's dedicated pair is
    used. Returns None when neither is available, so the error can be deferred to the first actual
    use of the service rather than raised at connect() time.

    A global key without its secret doesn't count as a global pair: it falls through to the
    dedicated pair (the half-supplied global pair itself is rejected by _resolve_api_credentials,
    which always runs for API-key connections).

    Raises:
        InterfaceError: If no complete global pair is supplied and the dedicated pair is
            half-supplied (key without secret, or vice versa).
    """
    global_key, global_secret = global_api_key or "", global_api_secret or ""
    key, secret = service_api_key or "", service_api_secret or ""

    # A complete global pair wins and covers everything, so short-circuit before validating the
    # dedicated pair: a caller who supplied a usable global pair shouldn't be tripped up by an
    # incidental half-supplied dedicated pair (e.g. one leaked in from the environment).
    if global_key and global_secret:
        return (global_key, global_secret)

    if bool(key) != bool(secret):
        raise InterfaceError(
            f"{config_prefix}_api_key and {config_prefix}_api_secret must be provided together"
        )
    if key:
        return (key, secret)
    return None


def _resolve_flink_auth(byoidc: ByoidcParams, keys: ApiKeyParams) -> tuple[httpx.Auth, bool]:
    """Select the Flink data-plane auth, and report whether the connection is in BYOIDC mode.

    An external_access_token/identity_pool_id pair selects BYOIDC mode -- the driver stamps a
    customer-minted external token on Flink requests instead of using HTTP Basic auth. BYOIDC and
    API-key auth are mutually exclusive, and (like the key/secret pairs) the two BYOIDC parameters
    must be supplied together. In API-key mode the (key, secret) selection is delegated to
    _resolve_api_credentials.

    Empty strings count as absent, matching the rest of connect()'s parameter handling.

    Returns:
        (auth, byoidc): the httpx.Auth for the Flink client, and True iff BYOIDC mode.

    Raises:
        InterfaceError: If only one of the BYOIDC pair is supplied, or if an external_access_token
            is combined with any API-key parameter.
    """
    token, pool = byoidc.external_access_token or "", byoidc.identity_pool_id or ""

    if bool(token) != bool(pool):
        raise InterfaceError("external_access_token and identity_pool_id must be provided together")

    if token:
        if keys.any_supplied:
            raise InterfaceError(
                "external_access_token cannot be combined with API key credentials "
                "(global/flink/tableflow/connect/cloud); BYOIDC bearer-token auth and API-key auth "
                "are mutually exclusive."
            )
        return FlinkBearerAuth(bearer_token=token, identity_pool_id=pool), True

    auth_key, auth_secret = _resolve_api_credentials(
        keys.global_api_key, keys.global_api_secret, keys.flink_api_key, keys.flink_api_secret
    )
    return httpx.BasicAuth(username=auth_key, password=auth_secret), False


def _basic_auth(pair: tuple[str, str] | None) -> httpx.BasicAuth | None:
    return httpx.BasicAuth(username=pair[0], password=pair[1]) if pair else None


@dataclass(frozen=True)
class _SurfaceSpec:
    """What distinguishes one control-plane API surface from another: how a missing credential is
    reported, and whether its responses may be DEBUG-logged."""

    subject: str
    """Names the surface in error messages."""
    plural: bool
    """Whether `subject` takes plural verb agreement ("Connectors are", "Tableflow is")."""
    key_pair: str
    """The dedicated key/secret parameter names that authenticate it when no global key is given."""
    log_body: bool = True
    """False for responses carrying secrets."""
    global_only: bool = False
    """True for routes that take only the global key (or an oauth login), never a dedicated one."""


class ControlPlaneSurface(Enum):
    """The control-plane API surfaces, which share one host and one `ServiceClient` and differ only
    in the credential each request carries (see `ConnectionCredentials.for_surface`)."""

    TABLEFLOW = _SurfaceSpec("Tableflow", False, "tableflow_api_key/tableflow_api_secret")
    CONNECT = _SurfaceSpec("Connectors", True, "connect_api_key/connect_api_secret")
    ARTIFACTS = _SurfaceSpec("Artifacts", True, "cloud_api_key/cloud_api_secret", log_body=False)
    """Never logs response bodies: the presign response carries a live upload URL."""
    CMK = _SurfaceSpec(
        "Kafka cluster lookups", True, "global_api_key/global_api_secret", global_only=True
    )
    ORGANIZATION = _SurfaceSpec(
        "Organization lookups", True, "global_api_key/global_api_secret", global_only=True
    )


def resolve_oauth_config(
    auth: str,
    *,
    oauth_config: CCloudOAuthConfig | None,
    reauth: str,
    byoidc: ByoidcParams,
    keys: ApiKeyParams,
) -> CCloudOAuthConfig | None:
    """Validate the `auth=`/`reauth=` mode selection and return the environment to log in against.

    Pure and network-free, mirroring `_resolve_flink_auth`'s role for the older two modes --
    called before any network or browser activity so a bad parameter combination fails fast,
    never after a browser has already been popped for the login.

    Returns:
        The `CCloudOAuthConfig` to log in against (`oauth_config` if supplied, else `PROD`) when
        `auth == "oauth"`; `None` for `auth == "api_key"` (BYOIDC/API-key mode, resolved
        separately by `_resolve_flink_auth`).

    Raises:
        InterfaceError: If `auth` names neither mode; if `oauth_config` is supplied while
            `auth != "oauth"`; if `auth == "oauth"` is combined with the BYOIDC pair or any
            API-key parameter -- interactive OAuth supplies its own credentials for every surface;
            if `reauth` names neither `"auto"` nor `"raise"`; or if a non-default `reauth` is
            supplied while `auth != "oauth"` (there is no wall to hit outside interactive OAuth).
    """
    if reauth not in ("auto", "raise"):
        raise InterfaceError(f"reauth must be 'auto' or 'raise', got {reauth!r}")

    if auth not in ("api_key", "oauth"):
        raise InterfaceError(f"auth must be 'api_key' or 'oauth', got {auth!r}")

    if auth != "oauth":
        if oauth_config is not None:
            raise InterfaceError("oauth_config may only be supplied when auth='oauth'")
        if reauth != "auto":
            raise InterfaceError("reauth may only be supplied when auth='oauth'")
        return None

    if byoidc.any_supplied:
        raise InterfaceError(
            "auth='oauth' cannot be combined with external_access_token/identity_pool_id; "
            "interactive OAuth and BYOIDC bearer-token auth are mutually exclusive."
        )
    if keys.any_supplied:
        raise InterfaceError(
            "auth='oauth' cannot be combined with API key credentials "
            "(global/flink/tableflow/connect/cloud); interactive OAuth and API-key auth are "
            "mutually exclusive."
        )
    return oauth_config if oauth_config is not None else PROD


class ConnectionCredentials:
    """A `Connection`'s credential mode, resolved once.

    Built early from the `reauth` policy alone (the `ServiceClient`s need its `send_with_policy`
    before any credential exists, and the oauth login callback needs a working one mid-login), then
    given its credentials by exactly one of `configure_api_key_mode` (API key or BYOIDC) or
    `adopt_oauth_provider`. Until then every credential accessor raises `InternalError`.

    Not thread-safe to reconfigure, and doesn't need to be: configuration happens inside
    `Connection.__init__`, before anything else can observe the connection.
    """

    reauth_policy: Literal["auto", "raise"]
    """How to respond when a request raises `ReauthenticationRequired` -- see connect()'s `reauth`
    docstring. Consulted by `send_with_policy` regardless of mode, though it is only ever meaningful
    under auth="oauth", the only mode that can raise that exception."""

    _byoidc: bool
    """True when authenticating with a customer-supplied BYOIDC bearer token (Flink data plane
    only); drives the honest fail-closed messages on the control-plane guards."""
    _oauth_provider: OAuthProvider | None
    """The process-wide shared provider under auth="oauth"; None otherwise. Never closed here --
    other Connections in the process may be using it."""
    _flink_auth: httpx.Auth | None
    """The Flink data-plane authentication: httpx.BasicAuth in API-key mode, FlinkBearerAuth in
    BYOIDC mode, or the shared OAuth provider's data-plane view under auth="oauth". Widened to the
    httpx.Auth base so all three fit the one polymorphic auth= slot. None until configured."""
    _surface_auth: dict[ControlPlaneSurface, httpx.Auth | None]
    """Each control-plane surface's credential: httpx.BasicAuth over a global or dedicated key pair
    in API-key mode, or the one shared control-plane token view under oauth mode (one token reaches
    every surface) -- or None if no usable credential exists for that surface. Empty until
    configured."""

    def __init__(self, reauth: Literal["auto", "raise"] = "auto") -> None:
        self.reauth_policy = reauth
        self._byoidc = False
        self._oauth_provider = None
        self._flink_auth = None
        self._surface_auth = {}

    # --- Mode
    @property
    def is_oauth(self) -> bool:
        """True once an interactive-login OAuth provider has been adopted. Lifts the control-plane
        guards' global-key requirement."""
        return self._oauth_provider is not None

    @property
    def oauth_metrics(self) -> OAuthMetrics | None:
        """The adopted provider's refresh metrics, or None under every other mode."""
        return self._oauth_provider.metrics if self._oauth_provider is not None else None

    # --- Configuration
    def configure_api_key_mode(self, *, byoidc: ByoidcParams, keys: ApiKeyParams) -> None:
        """Resolve the non-oauth modes: BYOIDC if the token pair is given, else API key.

        `_resolve_flink_auth` validates the BYOIDC pair and its mutual exclusion with every API-key
        parameter *before* `_resolve_service_credentials` runs, so BYOIDC mode never reaches (and
        never trips) the "either global or flink" API-key check. In BYOIDC mode every key
        parameter is empty by construction, so the control-plane resolvers below return None and
        the control plane fails closed -- see `for_surface`'s `_byoidc` branch.

        Raises:
            InterfaceError: On any malformed or conflicting credential parameters.
        """
        self._flink_auth, self._byoidc = _resolve_flink_auth(byoidc, keys)
        global_pair = keys.global_pair
        self._surface_auth = {
            ControlPlaneSurface.TABLEFLOW: _basic_auth(keys.tableflow_pair),
            ControlPlaneSurface.CONNECT: _basic_auth(keys.connect_pair),
            ControlPlaneSurface.ARTIFACTS: _basic_auth(keys.cloud_pair),
            ControlPlaneSurface.CMK: _basic_auth(global_pair),
            ControlPlaneSurface.ORGANIZATION: _basic_auth(global_pair),
        }
        self._oauth_provider = None

    def adopt_oauth_provider(self, provider: OAuthProvider) -> None:
        """Adopt the shared OAuth provider's two `httpx.Auth` views as the credentials: the
        data-plane view for Flink, the control-plane view for every surface."""
        self._oauth_provider = provider
        self._flink_auth = provider.data_plane_auth
        self._byoidc = False
        # One control-plane token reaches Tableflow, Connect, Artifact, and CMK alike -- the
        # capability gain over API-key mode, which needs a separate credential per surface.
        # Safe to share the same httpx.Auth object across every client: it holds no per-client
        # state, only a reference back to the shared, lock-guarded provider.
        self._surface_auth = dict.fromkeys(ControlPlaneSurface, provider.control_plane_auth)

    # --- Credentials
    @property
    def flink_auth(self) -> httpx.Auth:
        """The `httpx.Auth` for the Flink data-plane client."""
        if self._flink_auth is None:
            raise InternalError("driver bug: ConnectionCredentials used before it was configured")
        return self._flink_auth

    def for_surface(self, surface: ControlPlaneSurface) -> httpx.Auth:
        """The credential for one control-plane surface.

        A global key covers every surface; without one each takes only its own dedicated key pair
        (a Tableflow-scoped key isn't valid for artifact routes, and so on). Under oauth mode they
        all share one control-plane token view.

        Raises:
            ProgrammingError: If no usable credential exists for the surface; worded for BYOIDC
                (which the control plane never accepts) or for the missing key pair.
            InternalError: If called before the credentials were configured.
        """
        if self._flink_auth is None:
            raise InternalError("driver bug: ConnectionCredentials used before it was configured")
        auth = self._surface_auth[surface]
        if auth is not None:
            return auth
        spec = surface.value
        accepted = spec.key_pair if spec.global_only else f"global or {spec.key_pair}"
        if self._byoidc:
            raise ProgrammingError(
                f"{spec.subject} {'are' if spec.plural else 'is'} not reachable with a BYOIDC "
                "bearer token: Confluent's authorization model accepts only API keys on the "
                f"control plane. Use an API-key connection ({accepted}) for {spec.subject}."
            )
        verb = "require" if spec.plural else "requires"
        if spec.global_only:
            raise ProgrammingError(
                f"{spec.subject} {verb} a global API key ({spec.key_pair}) or auth='oauth'; "
                "neither was supplied to connect()."
            )
        raise ProgrammingError(
            f"{spec.subject} {verb} a global API key or a {spec.key_pair} pair; neither was "
            "supplied to connect()."
        )

    # --- Reauthentication
    def send_with_policy(self, do_request: Callable[[], httpx.Response]) -> httpx.Response:
        """Run one HTTP call, applying `self.reauth_policy` if it raises `ReauthenticationRequired`
        -- the only exception an `auth="oauth"` client's `httpx.Auth` view can raise instead
        of stamping a request, once its session's refresh token can no longer be used.

        `"raise"` lets it propagate untouched. `"auto"` (the default) instead blocks on
        the holder's `reauthenticate()` -- itself single-flighted process-wide, so concurrent
        callers hitting the wall together still cost one browser bounce -- and retries
        `do_request` exactly once; a `reauthenticate()` failure (a denied/timed-out login, a
        failed token exchange) propagates as-is, and is not retried further.

        The sole chokepoint for this: every HTTP call a `Connection` makes, Flink or
        control-plane, funnels through here as every `ServiceClient`'s `send_policy`.
        """
        try:
            return do_request()
        except ReauthenticationRequired:
            if self.reauth_policy == "raise":
                raise
            oauth_reauthenticate()
            return do_request()

    @contextmanager
    def reauth_raises(self) -> Generator[None, None, None]:
        """Force the `"raise"` policy for the duration, then restore the configured one.

        For the oauth login's validation callback, which runs before the holder has published the
        provider: there is no established login to re-authenticate, so under `reauth="auto"`,
        `ReauthenticationRequired` would otherwise be "recovered" into a ProgrammingError about a
        missing login, masking the real authentication failure. Nothing else can observe the
        connection yet, so the temporary swap is safe.
        """
        configured_policy, self.reauth_policy = self.reauth_policy, "raise"
        try:
            yield
        finally:
            self.reauth_policy = configured_policy
