"""Custom httpx authentication schemes for Confluent SQL connections.

Today this holds BYOIDC bearer-token auth for the Flink data plane (#100). The interactive-login
OAuth effort will add its own httpx.Auth adapters alongside FlinkBearerAuth here.
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
