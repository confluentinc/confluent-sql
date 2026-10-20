"""The four HTTP exchanges that make up the interactive-OAuth token chain.

Each function is standalone: it takes the `httpx.Client` to send through as a parameter rather
than owning one, so this module stays network-capable but lifecycle-agnostic -- the caller (the
provider) decides the client's lifetime, and this module never touches any of a
`Connection`'s three request clients.

Hop 1 (`exchange_code_for_tokens`) and the refresh exchange (`exchange_refresh_token`) both hit
the auth service's token endpoint and differ only in `grant_type`, so they share one result type,
`CodeExchangeResult`. The auth service's `access_token`/`expires_in` in that response are unused
downstream and dropped.

Hops 2 and 3 (`exchange_id_token_for_cp_token`, `exchange_cp_for_dp_token`) hit Confluent Cloud's
`/api/sessions` and `/api/access_tokens`. Neither response documents an `expires_in` field, so
rather than hardcode a guessed lifetime constant, `_jwt_exp` decodes the **unverified** `exp`
claim out of the returned token itself -- standard client-side practice for reading a token's own
stated lifetime (no signature verification needed; we are not authorizing anything with the
decode), and it self-corrects if Confluent ever changes the server-side lifetime. This assumes CP
and DP are three-segment JWTs rather than opaque bearer strings -- the mcp-confluent prior art
models both as opaque with a separate `expires_at`.
**Confirmed empirically against production** (2026-08-11, via a one-off diagnostic making raw
requests independent of this module's own code): both tokens are genuine JWTs carrying `exp`
(CP payload keys: `aud, exp, iat, iss, jti, may_act, orgResourceId, organizationId, scope, sub,
userId, userResourceId`; DP adds `authenticated_identity, clusters` in place of `aud`/`scope`). If
a token ever isn't a decodable JWT (or has no usable numeric `exp`), `_jwt_exp` falls back to a
fixed lifetime (`FALLBACK_CP_LIFETIME`/`FALLBACK_DP_LIFETIME`, matching mcp-confluent's
`token-lifetimes.ts`) rather than hard-failing the login/refresh.

Every exchange funnels its request through `http_json.post_json`, which is what keeps this
module's DB-API promise: only `Error` subclasses ever escape to a caller -- `OperationalError`,
or its `OAuthTokenEndpointError` subclass when the auth service's token endpoint itself refuses
the grant. See `http_json`'s docstring for the shared hardening this and later oauth-package
modules (the callback server, the provider) build on.
"""

from __future__ import annotations

import base64
import binascii
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import httpx

from ..exceptions import OAuthTokenEndpointError, OperationalError
from .config import CCloudOAuthConfig
from .http_json import best_effort_json_object, optional_object_field, post_json, require_field

UNKNOWN_ORG_REASON = "Cannot find the organization for this user"
"""The reason CCloud's `/api/sessions` gives (with a 403) when the supplied org_resource_id is
unusable for the logged-in user. A 403 can have other reasons, so this is what we key on."""

FALLBACK_CP_LIFETIME = timedelta(minutes=5)
"""Used when a control-plane token isn't a decodable JWT (or has no usable `exp`) -- matches
mcp-confluent's CONTROL_PLANE_TOKEN_LIFETIME_MS."""

FALLBACK_DP_LIFETIME = timedelta(minutes=10)
"""Used when a data-plane token isn't a decodable JWT (or has no usable `exp`) -- matches
mcp-confluent's DATA_PLANE_TOKEN_LIFETIME_MS."""


@dataclass(frozen=True)
class CodeExchangeResult:
    """The outcome of trading an authorization code -- or a refresh token -- for a fresh
    id_token and a rotated refresh_token."""

    id_token: str
    refresh_token: str


@dataclass(frozen=True)
class ControlPlaneTokenResult:
    """The outcome of exchanging an id_token for a Confluent Cloud control-plane token.

    organization_resource_id is None only when the /api/sessions response omits an organization
    block; callers that need an org (every caller in this driver) should treat that as unexpected.
    """

    token: str
    expires_at: datetime
    organization_resource_id: str | None


@dataclass(frozen=True)
class DataPlaneTokenResult:
    """The outcome of exchanging a control-plane token for a Confluent Cloud data-plane token."""

    token: str
    expires_at: datetime


def exchange_code_for_tokens(
    client: httpx.Client, config: CCloudOAuthConfig, *, code: str, verifier: str
) -> CodeExchangeResult:
    """Hop 1's code leg: POST config.token_url with the PKCE verifier, trading the
    authorization code the callback server captured for an id_token + refresh_token."""
    body = post_json(
        client,
        config.token_url,
        check_response=_raise_for_auth_service_error,
        data={
            "grant_type": "authorization_code",
            "client_id": config.client_id,
            "code": code,
            "code_verifier": verifier,
            "redirect_uri": config.redirect_uri,
        },
    )
    context = "the code exchange"
    return CodeExchangeResult(
        id_token=require_field(body, "id_token", context=context),
        refresh_token=require_field(body, "refresh_token", context=context),
    )


def exchange_refresh_token(
    client: httpx.Client, config: CCloudOAuthConfig, *, refresh_token: str
) -> CodeExchangeResult:
    """The refresh leg: POST config.token_url with grant_type=refresh_token. The auth service's
    refresh tokens are single-use and rotating -- the returned refresh_token is a *different*
    string that must replace the one just spent. This function performs the exchange only;
    persisting the rotated token before anything else is the caller's responsibility (the
    persist-before-exchange ordering)."""
    body = post_json(
        client,
        config.token_url,
        check_response=_raise_for_auth_service_error,
        data={
            "grant_type": "refresh_token",
            "client_id": config.client_id,
            "refresh_token": refresh_token,
        },
    )
    context = "the refresh exchange"
    return CodeExchangeResult(
        id_token=require_field(body, "id_token", context=context),
        refresh_token=require_field(body, "refresh_token", context=context),
    )


def exchange_id_token_for_cp_token(
    client: httpx.Client,
    config: CCloudOAuthConfig,
    *,
    id_token: str,
    org_resource_id: str | None,
    now: datetime | None = None,
) -> ControlPlaneTokenResult:
    """Hop 2: POST {api_host}/api/sessions with the id_token, minting a control-plane token.

    org_resource_id scopes the session to one organization for a multi-org user; omitted, CCloud
    resolves the caller's default org and returns it in the response's organization block. `now`
    is the fallback-expiry anchor (see `_jwt_exp`); defaults to the real clock, overridable so
    tests can pin exact expiry math."""
    request_body: dict[str, str] = {"id_token": id_token}
    if org_resource_id is not None:
        request_body["org_resource_id"] = org_resource_id
    try:
        payload = post_json(
            client,
            f"{config.api_host}/api/sessions",
            check_response=_raise_for_confluent_api_error,
            json=request_body,
        )
    except OperationalError as e:
        # This is the only hop that sends an org, and a 403 with this specific reason means CCloud
        # refused it for this user. Say so, rather than leaving the caller to guess the org id was
        # the problem. Other 403 reasons pass through untouched. The reason is nested in the error
        # body, so it appears verbatim inside the generic message's text.
        if (
            org_resource_id is not None
            and e.http_status_code == 403
            and UNKNOWN_ORG_REASON in str(e)
        ):
            username = _id_token_email(id_token)
            logged_in_as = f" (logged in as {username})" if username else ""
            raise OperationalError(
                f"Confluent Cloud rejected organization_id {org_resource_id!r} for the "
                f"logged-in user{logged_in_as}: they are not a member of it, or no such "
                "organization exists. "
                "Omit organization_id to use the login's default organization, or log in as a "
                f"user who belongs to that organization. ({e})",
                http_status_code=e.http_status_code,
            ) from e
        raise
    token = require_field(payload, "token", context="the control-plane token exchange")
    organization = optional_object_field(payload, "organization")
    organization_resource_id = (
        require_field(organization, "resource_id", context="the control-plane token's organization")
        if organization
        else None
    )
    return ControlPlaneTokenResult(
        token=token,
        expires_at=_jwt_exp(
            token,
            now=now if now is not None else datetime.now(timezone.utc),
            fallback_lifetime=FALLBACK_CP_LIFETIME,
        ),
        organization_resource_id=organization_resource_id,
    )


def exchange_cp_for_dp_token(
    client: httpx.Client, config: CCloudOAuthConfig, *, cp_token: str, now: datetime | None = None
) -> DataPlaneTokenResult:
    """Hop 3: POST {api_host}/api/access_tokens, bearing the control-plane token, minting a
    data-plane token. The response's regional_token is unused -- Flink, like every other
    data-plane consumer, wants the plain token. `now` is the fallback-expiry anchor (see
    `_jwt_exp`); defaults to the real clock, overridable so tests can pin exact expiry math."""
    payload = post_json(
        client,
        f"{config.api_host}/api/access_tokens",
        check_response=_raise_for_confluent_api_error,
        headers={"Authorization": f"Bearer {cp_token}"},
        json={},
    )
    token = require_field(payload, "token", context="the data-plane token exchange")
    return DataPlaneTokenResult(
        token=token,
        expires_at=_jwt_exp(
            token,
            now=now if now is not None else datetime.now(timezone.utc),
            fallback_lifetime=FALLBACK_DP_LIFETIME,
        ),
    )


def _jwt_exp(token: str, *, now: datetime, fallback_lifetime: timedelta) -> datetime:
    """Decode the unverified `exp` claim out of a JWT's payload segment. Falls back to
    `now + fallback_lifetime` if the token isn't a decodable JWT, its payload isn't an object,
    its `exp` claim is missing or non-numeric, or `exp` is out of datetime's representable range
    -- an opaque bearer token should degrade the caller's expiry tracking, not hard-fail the
    login/refresh. The fromtimestamp conversion is deliberately inside the try: an
    out-of-range exp raises OverflowError/OSError there, which must be caught same as a decode
    failure. `now` is anchored to UTC before use in the fallback so this always returns tz-aware
    UTC, same as the JWT-decode path, regardless of whether the caller's `now` was naive."""
    try:
        _header, payload_segment, _signature = token.split(".")
        padded = payload_segment + "=" * (-len(payload_segment) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded))
        exp = payload["exp"]
        return datetime.fromtimestamp(exp, tz=timezone.utc)
    except (ValueError, KeyError, TypeError, OverflowError, OSError, binascii.Error):
        anchor = (
            now.astimezone(timezone.utc)
            if now.tzinfo is not None
            else now.replace(tzinfo=timezone.utc)
        )
        return anchor + fallback_lifetime


def _id_token_email(id_token: str) -> str | None:
    """Best-effort: the unverified `email` claim of the id_token, for naming who is logged in
    in an error message. None for anything that isn't a decodable JWT with a string `email` --
    a diagnostic nicety must never mask the real failure it is decorating."""
    try:
        _header, payload_segment, _signature = id_token.split(".")
        padded = payload_segment + "=" * (-len(payload_segment) % 4)
        email = json.loads(base64.urlsafe_b64decode(padded))["email"]
    except (ValueError, KeyError, TypeError):
        return None
    return email if isinstance(email, str) and email else None


def _raise_for_auth_service_error(response: httpx.Response) -> None:
    """Translate a non-2xx from the auth service's token endpoint into OAuthTokenEndpointError.

    The OAuth 2.0 `error` field (RFC 6749 section 5.2) is carried through as `error_code`, not
    just folded into the message: `invalid_grant` is the token endpoint's way of saying the
    refresh token is expired, revoked, or already spent, and that is the one failure a retry can
    never fix. The refresh path branches on exactly that distinction, so the code has to reach it
    as data. A body with no usable `error` string yields `error_code=None`, which callers treat
    as "unclassified, so assume retryable" -- the safe direction, since the alternative is
    permanently wedging a session over a transient 502.
    """
    if response.is_success:
        return
    body = best_effort_json_object(response)
    error_code = body.get("error")
    message = body.get("error_description") or error_code or response.text
    raise OAuthTokenEndpointError(
        f"Confluent auth service token request failed: {message}",
        error_code=error_code if isinstance(error_code, str) else None,
        http_status_code=response.status_code,
    )


def _raise_for_confluent_api_error(response: httpx.Response) -> None:
    if response.is_success:
        return
    body = best_effort_json_object(response)
    message = body.get("message") or body.get("error") or response.text
    raise OperationalError(
        f"Confluent Cloud API request failed: {message}", http_status_code=response.status_code
    )
