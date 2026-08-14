"""Interactive human-login OAuth for Confluent Cloud (epic #150).

This package holds the three-hop PKCE token chain that mints Confluent's own control-plane
and data-plane tokens. #151 built the pure, network- and thread-free core: PKCE parameter
generation, the token chain's four exchanges, the immutable TokenSet snapshot, and the
per-environment config. #152 added the loopback CallbackServer that catches the auth service's
redirect. Nothing here is wired into `connect()` yet -- the stateful provider that drives a login
end to end arrives with #153, and lands in `connect()` at #155.
"""

from __future__ import annotations

from .callback_server import DEFAULT_LOGIN_TIMEOUT_SECS, CallbackServer
from .config import PROD, CCloudOAuthConfig
from .pkce import challenge_for, generate_state, generate_verifier
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
    "PROD",
    "CCloudOAuthConfig",
    "CallbackServer",
    "CodeExchangeResult",
    "ControlPlaneTokenResult",
    "DataPlaneTokenResult",
    "TokenSet",
    "challenge_for",
    "exchange_code_for_tokens",
    "exchange_cp_for_dp_token",
    "exchange_id_token_for_cp_token",
    "exchange_refresh_token",
    "generate_state",
    "generate_verifier",
]
