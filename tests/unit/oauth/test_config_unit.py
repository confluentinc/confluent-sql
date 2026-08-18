import dataclasses

import pytest

from confluent_sql.oauth.config import PROD, CCloudOAuthConfig

pytestmark = pytest.mark.unit


def test_prod_config_values():
    """PROD borrows mcp-confluent's registered auth service client (see the module docstring) --
    pinning the exact values here makes any accidental drift a loud test failure rather than a
    silent redirect_uri mismatch discovered against a live auth service tenant."""
    assert PROD.auth_service_domain == "login.confluent.io"
    assert PROD.api_host == "https://confluent.cloud"
    assert PROD.client_id == "cZ0wejEDJLNocYDJ54mAmGK21klrv21h"
    assert PROD.callback_host == "127.0.0.1"
    assert PROD.callback_port == 26640
    assert PROD.callback_path == "/gateway/v1/callback-local-mcp-docs"
    assert PROD.scopes == ("email", "openid", "offline_access")


def test_authorize_url():
    assert PROD.authorize_url == "https://login.confluent.io/authorize"


def test_token_url():
    assert PROD.token_url == "https://login.confluent.io/oauth/token"


def test_redirect_uri():
    assert PROD.redirect_uri == "http://127.0.0.1:26640/gateway/v1/callback-local-mcp-docs"


def test_config_is_frozen():
    with pytest.raises(dataclasses.FrozenInstanceError):
        PROD.client_id = "something-else"  # type: ignore[misc]


def test_custom_config_renders_its_own_urls():
    config = CCloudOAuthConfig(
        auth_service_domain="login-stag.confluent-dev.io",
        api_host="https://stag.cpdev.cloud",
        client_id="test-client",
        callback_host="127.0.0.1",
        callback_port=12345,
        callback_path="/callback",
    )
    assert config.authorize_url == "https://login-stag.confluent-dev.io/authorize"
    assert config.token_url == "https://login-stag.confluent-dev.io/oauth/token"
    assert config.redirect_uri == "http://127.0.0.1:12345/callback"
