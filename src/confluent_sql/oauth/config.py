"""Per-environment configuration for the interactive-OAuth login.

The auth service's domain, API host, and client_id vary per Confluent Cloud environment
(prod/stag/devel). Every environment (`PROD`, `STAG`, `DEVEL`) has its own dedicated client
registration, but all three share the same loopback callback host, port, and path, so those are
field defaults rather than per-environment values.
"""

from __future__ import annotations

from dataclasses import dataclass, field

_CALLBACK_HOST = "127.0.0.1"
_CALLBACK_PORT = 26642
_CALLBACK_PATH = "/callback-confluent-sql-docs"


@dataclass(frozen=True)
class CCloudOAuthConfig:
    """Everything the token chain needs to talk to one Confluent Cloud environment's auth
    service and API host."""

    auth_service_domain: str
    # Also doubles as the top-level URL that invites logins for this lane -- what the callback
    # server's success page links "Confluent Cloud" to.
    api_host: str
    client_id: str
    # Shared by every environment; overridable so tests can bind an ephemeral port.
    callback_host: str = _CALLBACK_HOST
    callback_port: int = _CALLBACK_PORT
    callback_path: str = _CALLBACK_PATH
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


PROD = CCloudOAuthConfig(
    auth_service_domain="login.confluent.io",
    api_host="https://confluent.cloud",
    client_id="3OP4CbmWzyLp9mB0asHmt0YZVlOmNqwi",
)

STAG = CCloudOAuthConfig(
    auth_service_domain="login-stag.confluent-dev.io",
    api_host="https://stag.cpdev.cloud",
    client_id="kBIUXUKdg1RiEtg4cJjTCF58MEexw8EO",
)

DEVEL = CCloudOAuthConfig(
    auth_service_domain="login.confluent-dev.io",
    api_host="https://devel.cpdev.cloud",
    client_id="txYV6dvI8PWu6OEoADXv9PVs1nyMrbCr",
)
