"""The four HTTP exchanges that make up the interactive-OAuth token chain.

Each function is standalone: it takes the `httpx.Client` to send through as a parameter rather
than owning one, so this module stays network-capable but lifecycle-agnostic -- the caller (the
provider landing in #153) decides the client's lifetime, and this module never touches any of a
`Connection`'s three request clients.

Hop 1 (`exchange_code_for_tokens`) and the refresh exchange (`exchange_refresh_token`) both hit
Auth0's token endpoint and differ only in `grant_type`, so they share one result type,
`CodeExchangeResult`. Per oauth-research-and-plan.md, the Auth0 `access_token`/`expires_in` in
that response are unused downstream and dropped.

Hops 2 and 3 (`exchange_id_token_for_cp_token`, `exchange_cp_for_dp_token`) hit Confluent Cloud's
`/api/sessions` and `/api/access_tokens`. Neither response documents an `expires_in` field, so
rather than hardcode a guessed lifetime constant, `_jwt_exp` decodes the **unverified** `exp`
claim out of the returned token itself -- standard client-side practice for reading a token's own
stated lifetime (no signature verification needed; we are not authorizing anything with the
decode), and it self-corrects if Confluent ever changes the server-side lifetime.

Every exchange funnels its request through `_post_json`, which is what keeps this module's DB-API
promise: only `Error` subclasses (here, always `OperationalError`) ever escape to a caller. Left
unwrapped, a transport failure would raise a bare `httpx.RequestError`, and a malformed or
short-of-a-required-field response body would raise `json.JSONDecodeError`/`KeyError` -- all
non-DB-API exceptions with no place in this driver's public surface. `_post_json` normalizes the
first two; `_required_field` normalizes the third for every `dict` field this module reads out of
a parsed response.
"""

from __future__ import annotations

import base64
import binascii
import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import httpx

from ..exceptions import OperationalError
from .config import CCloudOAuthConfig


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
    block; callers that need an org (every caller in this epic) should treat that as unexpected.
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
    body = _post_json(
        client,
        config.token_url,
        check_response=_raise_for_auth0_error,
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
        id_token=_required_field(body, "id_token", context=context),
        refresh_token=_required_field(body, "refresh_token", context=context),
    )


def exchange_refresh_token(
    client: httpx.Client, config: CCloudOAuthConfig, *, refresh_token: str
) -> CodeExchangeResult:
    """The refresh leg: POST config.token_url with grant_type=refresh_token. Auth0 refresh
    tokens are single-use and rotating -- the returned refresh_token is a *different* string
    that must replace the one just spent. This function performs the exchange only; persisting
    the rotated token before anything else is the caller's responsibility (see
    oauth-research-and-plan.md §2's persist-before-exchange ordering, landing with #153)."""
    body = _post_json(
        client,
        config.token_url,
        check_response=_raise_for_auth0_error,
        data={
            "grant_type": "refresh_token",
            "client_id": config.client_id,
            "refresh_token": refresh_token,
        },
    )
    context = "the refresh exchange"
    return CodeExchangeResult(
        id_token=_required_field(body, "id_token", context=context),
        refresh_token=_required_field(body, "refresh_token", context=context),
    )


def exchange_id_token_for_cp_token(
    client: httpx.Client,
    config: CCloudOAuthConfig,
    *,
    id_token: str,
    org_resource_id: str | None,
) -> ControlPlaneTokenResult:
    """Hop 2: POST {api_host}/api/sessions with the id_token, minting a control-plane token.

    org_resource_id scopes the session to one organization for a multi-org user; omitted, CCloud
    resolves the caller's default org and returns it in the response's organization block."""
    request_body: dict[str, str] = {"id_token": id_token}
    if org_resource_id is not None:
        request_body["org_resource_id"] = org_resource_id
    payload = _post_json(
        client,
        f"{config.api_host}/api/sessions",
        check_response=_raise_for_confluent_api_error,
        json=request_body,
    )
    token = _required_field(payload, "token", context="the control-plane token exchange")
    organization = payload.get("organization") or {}
    return ControlPlaneTokenResult(
        token=token,
        expires_at=_jwt_exp(token),
        organization_resource_id=organization.get("resource_id"),
    )


def exchange_cp_for_dp_token(
    client: httpx.Client, config: CCloudOAuthConfig, *, cp_token: str
) -> DataPlaneTokenResult:
    """Hop 3: POST {api_host}/api/access_tokens, bearing the control-plane token, minting a
    data-plane token. The response's regional_token is unused -- Flink, like every other
    data-plane consumer, wants the plain token (see oauth-research-and-plan.md)."""
    payload = _post_json(
        client,
        f"{config.api_host}/api/access_tokens",
        check_response=_raise_for_confluent_api_error,
        headers={"Authorization": f"Bearer {cp_token}"},
        json={},
    )
    token = _required_field(payload, "token", context="the data-plane token exchange")
    return DataPlaneTokenResult(token=token, expires_at=_jwt_exp(token))


def _post_json(
    client: httpx.Client,
    url: str,
    *,
    check_response: Callable[[httpx.Response], None],
    **kwargs: Any,
) -> dict[str, Any]:
    """POST url and return its parsed JSON body, translating every failure mode into
    OperationalError so no bare httpx or json exception escapes this module: a transport-level
    failure (no response at all), check_response's non-2xx handling (_raise_for_auth0_error /
    _raise_for_confluent_api_error), and a 2xx body that isn't valid JSON are all normalized
    here."""
    try:
        response = client.post(url, **kwargs)
    except httpx.RequestError as e:
        raise OperationalError(f"error sending OAuth request: {type(e).__name__}: {e}") from e
    check_response(response)
    try:
        return response.json()
    except ValueError as e:
        raise OperationalError(f"Could not parse OAuth response as JSON: {e}") from e


def _required_field(body: dict[str, Any], field: str, *, context: str) -> Any:
    """Read body[field], raising OperationalError instead of KeyError if a 2xx response is
    missing a field this module depends on -- a server contract violation, which belongs in the
    same DB-API exception hierarchy as every other OAuth chain failure, not a bare KeyError."""
    try:
        return body[field]
    except KeyError as e:
        raise OperationalError(f"OAuth response for {context} is missing '{field}'") from e


def _jwt_exp(token: str) -> datetime:
    """Decode the unverified `exp` claim out of a JWT's payload segment. Raises OperationalError
    if the token isn't a decodable JWT or carries no `exp` claim -- fail fast on a data-shape
    mismatch rather than silently guessing a fallback lifetime."""
    try:
        _header, payload_segment, _signature = token.split(".")
        padded = payload_segment + "=" * (-len(payload_segment) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded))
        exp = payload["exp"]
    except (ValueError, KeyError, binascii.Error) as e:
        raise OperationalError(f"Could not parse 'exp' claim from token: {e}") from e
    return datetime.fromtimestamp(exp, tz=timezone.utc)


def _raise_for_auth0_error(response: httpx.Response) -> None:
    if response.is_success:
        return
    try:
        body = response.json()
    except ValueError:
        body = {}
    message = body.get("error_description") or body.get("error") or response.text
    raise OperationalError(
        f"Auth0 token request failed: {message}", http_status_code=response.status_code
    )


def _raise_for_confluent_api_error(response: httpx.Response) -> None:
    if response.is_success:
        return
    try:
        body = response.json()
    except ValueError:
        body = {}
    message = body.get("message") or body.get("error") or response.text
    raise OperationalError(
        f"Confluent Cloud API request failed: {message}", http_status_code=response.status_code
    )
