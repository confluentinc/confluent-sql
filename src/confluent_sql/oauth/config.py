"""Per-environment configuration for the interactive-OAuth login.

The auth service's domain, API host, and client_id vary per Confluent Cloud environment
(prod/stag/devel); callback host/port/path are properties of the registered client, not the
environment, so they stay constant across the three rows below.

`PROD`'s client_id/port/path are **borrowed** from mcp-confluent's already-registered
public client (`oauth/auth0-config.ts`), pending this driver getting its own dedicated
registration (see oauth-research-and-plan.md decision 1). Swapping to our own client, once
registered, is a one-constant change here -- nothing downstream depends on whose client_id it is.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class CCloudOAuthConfig:
    """Everything the token chain needs to talk to one Confluent Cloud environment's auth
    service and API host."""

    auth_service_domain: str
    api_host: str
    client_id: str
    callback_host: str
    callback_port: int
    callback_path: str
    scopes: tuple[str, ...] = field(default=("email", "openid", "offline_access"))

    @property
    def authorize_url(self) -> str:
        return f"https://{self.auth_service_domain}/authorize"

    @property
    def token_url(self) -> str:
        return f"https://{self.auth_service_domain}/oauth/token"

    @property
    def redirect_uri(self) -> str:
        return f"http://{self.callback_host}:{self.callback_port}{self.callback_path}"


_CALLBACK_HOST = "127.0.0.1"

# This port, the callback path, and the per-env client_id are all baked into the auth service's
# public client registration and are temporarily borrowed from mcp-confluent. Before this package
# ships, it will get its own dedicated client registration, and then this port/path/client_id will
# be updated to match (issue #177).
_CALLBACK_PORT = 26640
_CALLBACK_PATH = "/gateway/v1/callback-local-mcp-docs"

_ENVIRONMENTS: dict[str, CCloudOAuthConfig] = {
    "prod": CCloudOAuthConfig(
        auth_service_domain="login.confluent.io",
        api_host="https://confluent.cloud",
        client_id="cZ0wejEDJLNocYDJ54mAmGK21klrv21h",
        callback_host=_CALLBACK_HOST,
        callback_port=_CALLBACK_PORT,
        callback_path=_CALLBACK_PATH,
    ),
    "stag": CCloudOAuthConfig(
        auth_service_domain="login-stag.confluent-dev.io",
        api_host="https://stag.cpdev.cloud",
        client_id="adtjckxmHbjddhNK36PvcXIDDbrJUMDH",
        callback_host=_CALLBACK_HOST,
        callback_port=_CALLBACK_PORT,
        callback_path=_CALLBACK_PATH,
    ),
    "devel": CCloudOAuthConfig(
        auth_service_domain="login.confluent-dev.io",
        api_host="https://devel.cpdev.cloud",
        client_id="D8DV9ee7XrKX4ncAc6vJtBFgIzTMNgoY",
        callback_host=_CALLBACK_HOST,
        callback_port=_CALLBACK_PORT,
        callback_path=_CALLBACK_PATH,
    ),
}
"""The full per-environment table, encoded now per oauth-research-and-plan.md decision 2 even
though only PROD ships in this epic's first pass -- exposing STAG/DEVEL later is then a one-line
export, not new machinery."""

PROD = _ENVIRONMENTS["prod"]
