"""Custom httpx authentication schemes for Confluent SQL connections.

Today this holds BYOIDC bearer-token auth for the Flink data plane (#100). The interactive-login
OAuth effort's two adapters deliberately live elsewhere -- in `oauth/provider.py`, next to the
`CCloudOAuth` whose lock-guarded token slot and refresh gate they reach into. A `FlinkBearerAuth`
is self-contained (it holds its own token and never refreshes); those are views onto a shared,
mutating provider, and splitting them from it would buy nothing but an import cycle.
"""

from __future__ import annotations

from collections.abc import Generator

import httpx


class FlinkBearerAuth(httpx.Auth):
    """Stamps a customer-supplied (BYOIDC) bearer token and its identity-pool id onto every
    Flink data-plane request.

    The identity-pool id lives inside the auth object rather than the client's static headers so
    that `_flink_auth` is one cohesive "this is the BYOIDC identity" unit -- the same shape the
    interactive-OAuth adapters will take.
    """

    def __init__(self, bearer_token: str, identity_pool_id: str) -> None:
        self._bearer_token = bearer_token
        self._identity_pool_id = identity_pool_id

    def auth_flow(self, request: httpx.Request) -> Generator[httpx.Request, httpx.Response, None]:
        request.headers["Authorization"] = f"Bearer {self._bearer_token}"
        request.headers["Confluent-Identity-Pool-Id"] = self._identity_pool_id
        yield request
