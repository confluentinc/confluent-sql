"""Interactive human-login OAuth for Confluent Cloud (epic #150).

This package holds the three-hop PKCE token chain that mints Confluent's own control-plane
and data-plane tokens. #151 built the pure, network- and thread-free core: PKCE parameter
generation, the token chain's four exchanges, the immutable TokenSet snapshot, and the
per-environment config. #152 added the loopback CallbackServer that catches the auth service's
redirect. #153 added `CCloudOAuth`, the provider that drives a login end to end, keeps the tokens
current by synchronous on-request refresh, and vends two `httpx.Auth` views over the one shared
snapshot. #154 added `ProcessOAuthHolder`, the module-level holder that makes that login singular
across a whole process -- one browser bounce shared by every `Connection`. #155 wired `acquire()`
and `release()` into `connect()`/`Connection`, exposing `auth="oauth"`. #156 added
`reauthenticate()`, recovering a session in place once its refresh token can no longer be used
(the ~8h absolute wall, idle expiry, or revocation) without opening a new `Connection`.
"""

from __future__ import annotations

from .callback_server import DEFAULT_LOGIN_TIMEOUT_SECS, CallbackServer
from .config import DEVEL, PROD, CCloudOAuthConfig
from .holder import ProcessOAuthHolder, acquire, reauthenticate, release, shutdown_all
from .pkce import challenge_for, generate_state, generate_verifier
from .provider import CCloudOAuth, OAuthMetrics, OAuthProvider
from .token_chain import (
    CodeExchangeResult,
    ControlPlaneTokenResult,
    DataPlaneTokenResult,
    exchange_code_for_tokens,
    exchange_cp_for_dp_token,
    exchange_id_token_for_cp_token,
    exchange_refresh_token,
)
from .token_set import TokenSet

__all__ = [
    "DEFAULT_LOGIN_TIMEOUT_SECS",
    "DEVEL",
    "PROD",
    "CCloudOAuth",
    "CCloudOAuthConfig",
    "CallbackServer",
    "CodeExchangeResult",
    "ControlPlaneTokenResult",
    "DataPlaneTokenResult",
    "OAuthMetrics",
    "OAuthProvider",
    "ProcessOAuthHolder",
    "TokenSet",
    "acquire",
    "challenge_for",
    "exchange_code_for_tokens",
    "exchange_cp_for_dp_token",
    "exchange_id_token_for_cp_token",
    "exchange_refresh_token",
    "generate_state",
    "generate_verifier",
    "reauthenticate",
    "release",
    "shutdown_all",
]
