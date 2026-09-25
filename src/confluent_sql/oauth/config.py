"""Per-environment configuration for the interactive-OAuth login.

The auth service's domain, API host, and client_id vary per Confluent Cloud environment
(prod/stag/devel); callback host/port/path are properties of the registered client, not the
environment as such, but until every environment has its own dedicated client they track
whichever client that environment's row currently borrows or owns.

`PROD`'s client_id/port/path is still **borrowed** from mcp-confluent's already-registered public
client (`oauth/auth0-config.ts`), pending this driver getting its own dedicated registration in
that environment (see oauth-research-and-plan.md decision 1). `DEVEL` and `STAG` each have their
own dedicated client -- identity-login-static#977 ("Confluent SQL Python Driver") for DEVEL,
identity-login-static#984 for STAG -- closing #177 for those two environments; PROD gets the same
treatment -- a one-row edit here, nothing downstream depends on whose client_id it is -- once it's
registered too.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class CCloudOAuthConfig:
    """Everything the token chain needs to talk to one Confluent Cloud environment's auth
    service and API host."""

    auth_service_domain: str
    # Also doubles as the top-level URL that invites logins for this lane -- what the callback
    # server's success page links "Confluent Cloud" to.
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

# This port, callback path, and client_id are baked into mcp-confluent's public client
# registration in the auth service and are still temporarily borrowed here for PROD. Once it gets
# its own dedicated client registration (issue #177), that row's port/path/client_id moves off
# these shared constants, same as DEVEL/STAG already have below.
_BORROWED_CALLBACK_PORT = 26640
_BORROWED_CALLBACK_PATH = "/gateway/v1/callback-local-mcp-docs"

_ENVIRONMENTS: dict[str, CCloudOAuthConfig] = {
    "prod": CCloudOAuthConfig(
        auth_service_domain="login.confluent.io",
        api_host="https://confluent.cloud",
        client_id="cZ0wejEDJLNocYDJ54mAmGK21klrv21h",
        callback_host=_CALLBACK_HOST,
        callback_port=_BORROWED_CALLBACK_PORT,
        callback_path=_BORROWED_CALLBACK_PATH,
    ),
    "stag": CCloudOAuthConfig(
        auth_service_domain="login-stag.confluent-dev.io",
        api_host="https://stag.cpdev.cloud",
        # Dedicated "Confluent SQL Python Driver" client (identity-login-static#984), not
        # borrowed -- registered with the same callback port/path as DEVEL's dedicated client
        # below (this environment's login is never run concurrently with DEVEL's).
        client_id="kBIUXUKdg1RiEtg4cJjTCF58MEexw8EO",
        callback_host=_CALLBACK_HOST,
        callback_port=26642,
        callback_path="/callback-confluent-sql-docs",
    ),
    "devel": CCloudOAuthConfig(
        auth_service_domain="login.confluent-dev.io",
        api_host="https://devel.cpdev.cloud",
        # Dedicated "Confluent SQL Python Driver" client (identity-login-static#977), not
        # borrowed -- own port/callback path too, avoiding any collision with a borrowed-client
        # login (PROD above, or mcp-confluent itself) running at the same time.
        client_id="txYV6dvI8PWu6OEoADXv9PVs1nyMrbCr",
        callback_host=_CALLBACK_HOST,
        callback_port=26642,
        callback_path="/callback-confluent-sql-docs",
    ),
}
"""The full per-environment table, encoded now per oauth-research-and-plan.md decision 2 even
though only PROD ships in this epic's first pass -- exposing STAG/DEVEL later is then a one-line
export, not new machinery."""

PROD = _ENVIRONMENTS["prod"]
STAG = _ENVIRONMENTS["stag"]
DEVEL = _ENVIRONMENTS["devel"]
