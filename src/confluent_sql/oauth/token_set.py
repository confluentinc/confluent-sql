"""TokenSet: the immutable snapshot of the three tokens the OAuth chain mints.

An instance is a frozen dataclass -- refresh never mutates one in place (that's the provider's
job, landing in #153); a refresh instead builds a brand-new TokenSet and swaps the provider's
reference to it. That copy-on-write shape is what lets a caller who has already grabbed a
reference read its fields with zero locking and zero risk of a torn read mid-update; the lock
lives on the provider's reference slot, not on this object. All three `*_valid` helpers are pure
arithmetic over fields that never change after construction -- no lock, no I/O.

The *idle* (4h) refresh-token cap from oauth-research-and-plan.md is deliberately not modeled
here: it requires tracking last-use time, which only matters once something actually refreshes
(#153) or proactively re-auths ahead of the wall (#158). This snapshot only carries the
*absolute* (8h) wall, computed by the caller at mint time and passed in as
`refresh_token_expires_at` -- ABSOLUTE_LIFETIME is exposed for that purpose.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

ABSOLUTE_LIFETIME = timedelta(hours=8)
"""Auth service application policy: a refresh token is unusable after 8h absolute lifetime,
regardless of activity. The caller minting a TokenSet computes refresh_token_expires_at as
`mint_time + ABSOLUTE_LIFETIME`."""

EXPIRY_SKEW = timedelta(seconds=30)
"""Subtracted from every expiry before comparing against `now`, so a token is never treated as
live for a request that lands a moment before its real expiry. Matches the 30s skew the Flink
Table API plugin's OAuthCredentialsProvider uses (see oauth-research-and-plan.md)."""


@dataclass(frozen=True)
class TokenSet:
    """An immutable snapshot of the refresh, control-plane, and data-plane tokens minted by one
    login or refresh cycle."""

    refresh_token: str
    refresh_token_expires_at: datetime
    cp_token: str
    cp_token_expires_at: datetime
    dp_token: str
    dp_token_expires_at: datetime

    def cp_token_valid(self, now: datetime) -> bool:
        """Whether cp_token is safe to use as of now (i.e. not within EXPIRY_SKEW of expiring)."""
        return now < self.cp_token_expires_at - EXPIRY_SKEW

    def dp_token_valid(self, now: datetime) -> bool:
        """Whether dp_token is safe to use as of now (i.e. not within EXPIRY_SKEW of expiring)."""
        return now < self.dp_token_expires_at - EXPIRY_SKEW

    def refresh_token_valid(self, now: datetime) -> bool:
        """Whether refresh_token is still usable as of now -- i.e. the 8h absolute wall hasn't
        been crossed. False means only a fresh interactive login (not a refresh) can recover."""
        return now < self.refresh_token_expires_at - EXPIRY_SKEW
