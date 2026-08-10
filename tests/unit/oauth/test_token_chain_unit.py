import base64
import json
import urllib.parse
from datetime import datetime, timezone

import httpx
import pytest

from confluent_sql.exceptions import OperationalError
from confluent_sql.oauth.config import CCloudOAuthConfig
from confluent_sql.oauth.token_chain import (
    exchange_code_for_tokens,
    exchange_cp_for_dp_token,
    exchange_id_token_for_cp_token,
    exchange_refresh_token,
)

pytestmark = pytest.mark.unit

CONFIG = CCloudOAuthConfig(
    auth0_domain="login.confluent.io",
    api_host="https://confluent.cloud",
    client_id="test-client-id",
    callback_host="127.0.0.1",
    callback_port=26640,
    callback_path="/gateway/v1/callback-local-mcp-docs",
)


def _make_jwt(exp: int) -> str:
    """A hand-built, unsigned JWT carrying only an `exp` claim -- enough for _jwt_exp to decode,
    since we never verify the signature (see token_chain.py's docstring)."""

    def _segment(payload: dict) -> str:
        return base64.urlsafe_b64encode(json.dumps(payload).encode()).rstrip(b"=").decode("ascii")

    return f"{_segment({'alg': 'none'})}.{_segment({'exp': exp})}.sig"


def _client(handler) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler))


class TestExchangeCodeForTokens:
    def test_sends_expected_request_and_parses_response(self):
        captured = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["request"] = request
            return httpx.Response(200, json={"id_token": "id-tok", "refresh_token": "refresh-tok"})

        with _client(handler) as client:
            result = exchange_code_for_tokens(
                client, CONFIG, code="auth-code-123", verifier="verifier-abc"
            )

        request = captured["request"]
        assert request.method == "POST"
        assert str(request.url) == CONFIG.token_url
        body = urllib.parse.parse_qs(request.content.decode())
        assert body["grant_type"] == ["authorization_code"]
        assert body["client_id"] == [CONFIG.client_id]
        assert body["code"] == ["auth-code-123"]
        assert body["code_verifier"] == ["verifier-abc"]
        assert body["redirect_uri"] == [CONFIG.redirect_uri]
        assert result.id_token == "id-tok"
        assert result.refresh_token == "refresh-tok"

    def test_auth0_error_body_raises_operational_error(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                403,
                json={"error": "invalid_grant", "error_description": "Invalid authorization code"},
            )

        with (
            _client(handler) as client,
            pytest.raises(OperationalError, match="Invalid authorization code"),
        ):
            exchange_code_for_tokens(client, CONFIG, code="bad-code", verifier="verifier-abc")

    def test_transport_failure_raises_operational_error_not_httpx_error(self):
        """A network-level failure (no HTTP response at all) must not leak httpx.RequestError
        past this module -- every caller-visible exception must be one of this driver's own
        Error subclasses."""

        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("connection refused", request=request)

        with _client(handler) as client, pytest.raises(OperationalError, match="ConnectError"):
            exchange_code_for_tokens(client, CONFIG, code="auth-code-123", verifier="verifier-abc")

    def test_non_json_response_raises_operational_error_not_value_error(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, content=b"not json")

        with _client(handler) as client, pytest.raises(OperationalError, match="JSON"):
            exchange_code_for_tokens(client, CONFIG, code="auth-code-123", verifier="verifier-abc")

    def test_response_missing_id_token_raises_operational_error_not_key_error(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"refresh_token": "refresh-tok"})

        with _client(handler) as client, pytest.raises(OperationalError, match="id_token"):
            exchange_code_for_tokens(client, CONFIG, code="auth-code-123", verifier="verifier-abc")

    def test_non_object_response_raises_operational_error_not_attribute_error(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=["id-tok", "refresh-tok"])

        with _client(handler) as client, pytest.raises(OperationalError, match="object"):
            exchange_code_for_tokens(client, CONFIG, code="auth-code-123", verifier="verifier-abc")

    def test_non_object_auth0_error_body_still_raises_operational_error(self):
        """A non-2xx response whose valid JSON body is a list, not an object, must still surface
        as OperationalError -- not AttributeError out of _raise_for_auth0_error's own body.get()."""

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(403, json=["unexpected", "error", "shape"])

        with _client(handler) as client, pytest.raises(OperationalError, match="Auth0"):
            exchange_code_for_tokens(client, CONFIG, code="bad-code", verifier="verifier-abc")


class TestExchangeRefreshToken:
    def test_sends_expected_request_and_parses_response(self):
        captured = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["request"] = request
            return httpx.Response(
                200, json={"id_token": "new-id-tok", "refresh_token": "rotated-refresh-tok"}
            )

        with _client(handler) as client:
            result = exchange_refresh_token(client, CONFIG, refresh_token="old-refresh-tok")

        body = urllib.parse.parse_qs(captured["request"].content.decode())
        assert body["grant_type"] == ["refresh_token"]
        assert body["client_id"] == [CONFIG.client_id]
        assert body["refresh_token"] == ["old-refresh-tok"]
        assert result.id_token == "new-id-tok"
        assert result.refresh_token == "rotated-refresh-tok"

    def test_auth0_error_body_raises_operational_error(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                401, json={"error": "invalid_grant", "error_description": "Refresh token expired"}
            )

        with (
            _client(handler) as client,
            pytest.raises(OperationalError, match="Refresh token expired"),
        ):
            exchange_refresh_token(client, CONFIG, refresh_token="dead-refresh-tok")


class TestExchangeIdTokenForCpToken:
    def test_sends_id_token_and_parses_response(self):
        exp = int(datetime(2026, 8, 10, 13, 0, tzinfo=timezone.utc).timestamp())
        cp_jwt = _make_jwt(exp)
        captured = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["request"] = request
            return httpx.Response(
                200,
                json={
                    "token": cp_jwt,
                    "organization": {"resource_id": "org-resource-abc"},
                },
            )

        with _client(handler) as client:
            result = exchange_id_token_for_cp_token(
                client, CONFIG, id_token="id-tok-xyz", org_resource_id=None
            )

        request = captured["request"]
        assert str(request.url) == f"{CONFIG.api_host}/api/sessions"
        sent_body = json.loads(request.content.decode())
        assert sent_body == {"id_token": "id-tok-xyz"}
        assert result.token == cp_jwt
        assert result.expires_at == datetime.fromtimestamp(exp, tz=timezone.utc)
        assert result.organization_resource_id == "org-resource-abc"

    def test_supplied_org_resource_id_is_sent_in_body(self):
        cp_jwt = _make_jwt(int(datetime(2026, 8, 10, 13, 0, tzinfo=timezone.utc).timestamp()))
        captured = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["request"] = request
            return httpx.Response(
                200, json={"token": cp_jwt, "organization": {"resource_id": "org-1"}}
            )

        with _client(handler) as client:
            exchange_id_token_for_cp_token(
                client, CONFIG, id_token="id-tok-xyz", org_resource_id="org-1"
            )

        sent_body = json.loads(captured["request"].content.decode())
        assert sent_body == {"id_token": "id-tok-xyz", "org_resource_id": "org-1"}

    def test_confluent_api_error_raises_operational_error(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(400, json={"message": "invalid id_token"})

        with _client(handler) as client, pytest.raises(OperationalError, match="invalid id_token"):
            exchange_id_token_for_cp_token(client, CONFIG, id_token="bad", org_resource_id=None)

    def test_token_with_unparseable_exp_raises_operational_error(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"token": "not-a-jwt", "organization": {}})

        with _client(handler) as client, pytest.raises(OperationalError, match="exp"):
            exchange_id_token_for_cp_token(client, CONFIG, id_token="id-tok", org_resource_id=None)

    def test_response_missing_token_raises_operational_error_not_key_error(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"organization": {"resource_id": "org-1"}})

        with _client(handler) as client, pytest.raises(OperationalError, match="token"):
            exchange_id_token_for_cp_token(client, CONFIG, id_token="id-tok", org_resource_id=None)

    def test_non_object_organization_field_raises_operational_error_not_attribute_error(self):
        cp_jwt = _make_jwt(int(datetime(2026, 8, 10, 13, 0, tzinfo=timezone.utc).timestamp()))

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"token": cp_jwt, "organization": "not-an-object"})

        with _client(handler) as client, pytest.raises(OperationalError, match="not an object"):
            exchange_id_token_for_cp_token(client, CONFIG, id_token="id-tok", org_resource_id=None)


class TestJwtExpHardening:
    """_jwt_exp must turn every malformed-token shape into OperationalError, never a bare
    TypeError/OverflowError/OSError -- exercised through exchange_cp_for_dp_token since both hops
    2 and 3 route their token through the same _jwt_exp helper."""

    def _dp_token_response(self, token: str) -> httpx.Response:
        return httpx.Response(200, json={"token": token})

    def test_non_object_payload_raises_operational_error(self):
        header = base64.urlsafe_b64encode(json.dumps({"alg": "none"}).encode()).rstrip(b"=")
        payload = base64.urlsafe_b64encode(json.dumps(["not", "an", "object"]).encode()).rstrip(
            b"="
        )
        token = f"{header.decode()}.{payload.decode()}.sig"

        def handler(request: httpx.Request) -> httpx.Response:
            return self._dp_token_response(token)

        with _client(handler) as client, pytest.raises(OperationalError, match="exp"):
            exchange_cp_for_dp_token(client, CONFIG, cp_token="cp-tok")

    def test_non_numeric_exp_raises_operational_error(self):
        header = base64.urlsafe_b64encode(json.dumps({"alg": "none"}).encode()).rstrip(b"=")
        payload = base64.urlsafe_b64encode(json.dumps({"exp": "not-a-number"}).encode()).rstrip(
            b"="
        )
        token = f"{header.decode()}.{payload.decode()}.sig"

        def handler(request: httpx.Request) -> httpx.Response:
            return self._dp_token_response(token)

        with _client(handler) as client, pytest.raises(OperationalError, match="exp"):
            exchange_cp_for_dp_token(client, CONFIG, cp_token="cp-tok")

    def test_out_of_range_exp_raises_operational_error(self):
        header = base64.urlsafe_b64encode(json.dumps({"alg": "none"}).encode()).rstrip(b"=")
        payload = base64.urlsafe_b64encode(json.dumps({"exp": 99999999999999999}).encode()).rstrip(
            b"="
        )
        token = f"{header.decode()}.{payload.decode()}.sig"

        def handler(request: httpx.Request) -> httpx.Response:
            return self._dp_token_response(token)

        with _client(handler) as client, pytest.raises(OperationalError, match="exp"):
            exchange_cp_for_dp_token(client, CONFIG, cp_token="cp-tok")


class TestExchangeCpForDpToken:
    def test_sends_bearer_cp_token_and_parses_response(self):
        exp = int(datetime(2026, 8, 10, 12, 10, tzinfo=timezone.utc).timestamp())
        dp_jwt = _make_jwt(exp)
        captured = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["request"] = request
            return httpx.Response(200, json={"token": dp_jwt, "regional_token": "unused"})

        with _client(handler) as client:
            result = exchange_cp_for_dp_token(client, CONFIG, cp_token="cp-tok-xyz")

        request = captured["request"]
        assert str(request.url) == f"{CONFIG.api_host}/api/access_tokens"
        assert request.headers["Authorization"] == "Bearer cp-tok-xyz"
        assert result.token == dp_jwt
        assert result.expires_at == datetime.fromtimestamp(exp, tz=timezone.utc)

    def test_confluent_api_error_raises_operational_error(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(401, json={"message": "cp token expired"})

        with _client(handler) as client, pytest.raises(OperationalError, match="cp token expired"):
            exchange_cp_for_dp_token(client, CONFIG, cp_token="expired-cp-tok")
