import base64
import json
import urllib.parse
from datetime import datetime, timezone

import httpx
import pytest

from confluent_sql.exceptions import OperationalError
from confluent_sql.oauth.config import CCloudOAuthConfig
from confluent_sql.oauth.token_chain import (
    FALLBACK_CP_LIFETIME,
    FALLBACK_DP_LIFETIME,
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


def _now() -> datetime:
    return datetime(2026, 8, 11, 9, 0, tzinfo=timezone.utc)


def _jwt_segment(value: object) -> str:
    return base64.urlsafe_b64encode(json.dumps(value).encode()).rstrip(b"=").decode("ascii")


def _jwt_with_payload(payload: object) -> str:
    """Build a token (header.payload.sig) with an arbitrary -- possibly malformed -- JSON
    payload segment, for testing _jwt_exp's handling of shapes a real JWT would never have."""
    return f"{_jwt_segment({'alg': 'none'})}.{_jwt_segment(payload)}.sig"


def _make_jwt(exp: int) -> str:
    """A hand-built, unsigned JWT carrying only an `exp` claim -- enough for _jwt_exp to decode,
    since we never verify the signature (see token_chain.py's docstring)."""
    return _jwt_with_payload({"exp": exp})


def _client(handler) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler))


class TestExchangeCodeForTokens:
    def test_sends_expected_request_and_parses_response(self):
        code = "auth-code-123"
        verifier = "verifier-abc"
        id_token = "id-tok"
        refresh_token = "refresh-tok"
        captured = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["request"] = request
            return httpx.Response(200, json={"id_token": id_token, "refresh_token": refresh_token})

        with _client(handler) as client:
            result = exchange_code_for_tokens(client, CONFIG, code=code, verifier=verifier)

        request = captured["request"]
        assert request.method == "POST"
        assert str(request.url) == CONFIG.token_url
        body = urllib.parse.parse_qs(request.content.decode())
        assert body["grant_type"] == ["authorization_code"]
        assert body["client_id"] == [CONFIG.client_id]
        assert body["code"] == [code]
        assert body["code_verifier"] == [verifier]
        assert body["redirect_uri"] == [CONFIG.redirect_uri]
        assert result.id_token == id_token
        assert result.refresh_token == refresh_token

    def test_auth0_error_body_raises_operational_error(self):
        error_description = "Invalid authorization code"

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                403, json={"error": "invalid_grant", "error_description": error_description}
            )

        with (
            _client(handler) as client,
            pytest.raises(OperationalError, match=error_description),
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
        old_refresh_token = "old-refresh-tok"
        new_id_token = "new-id-tok"
        rotated_refresh_token = "rotated-refresh-tok"
        captured = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["request"] = request
            return httpx.Response(
                200, json={"id_token": new_id_token, "refresh_token": rotated_refresh_token}
            )

        with _client(handler) as client:
            result = exchange_refresh_token(client, CONFIG, refresh_token=old_refresh_token)

        body = urllib.parse.parse_qs(captured["request"].content.decode())
        assert body["grant_type"] == ["refresh_token"]
        assert body["client_id"] == [CONFIG.client_id]
        assert body["refresh_token"] == [old_refresh_token]
        assert result.id_token == new_id_token
        assert result.refresh_token == rotated_refresh_token

    def test_auth0_error_body_raises_operational_error(self):
        error_description = "Refresh token expired"

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                401, json={"error": "invalid_grant", "error_description": error_description}
            )

        with (
            _client(handler) as client,
            pytest.raises(OperationalError, match=error_description),
        ):
            exchange_refresh_token(client, CONFIG, refresh_token="dead-refresh-tok")


class TestExchangeIdTokenForCpToken:
    def test_sends_id_token_and_parses_response(self):
        id_token = "id-tok-xyz"
        returned_org_resource_id = "org-resource-abc"
        exp = int(datetime(2026, 8, 10, 13, 0, tzinfo=timezone.utc).timestamp())
        cp_jwt = _make_jwt(exp)
        captured = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["request"] = request
            return httpx.Response(
                200,
                json={"token": cp_jwt, "organization": {"resource_id": returned_org_resource_id}},
            )

        with _client(handler) as client:
            result = exchange_id_token_for_cp_token(
                client, CONFIG, id_token=id_token, org_resource_id=None
            )

        request = captured["request"]
        assert str(request.url) == f"{CONFIG.api_host}/api/sessions"
        sent_body = json.loads(request.content.decode())
        assert sent_body == {"id_token": id_token}
        assert result.token == cp_jwt
        assert result.expires_at == datetime.fromtimestamp(exp, tz=timezone.utc)
        assert result.organization_resource_id == returned_org_resource_id

    def test_supplied_org_resource_id_is_sent_in_body(self):
        id_token = "id-tok-xyz"
        org_resource_id = "org-1"
        cp_jwt = _make_jwt(int(datetime(2026, 8, 10, 13, 0, tzinfo=timezone.utc).timestamp()))
        captured = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["request"] = request
            return httpx.Response(
                200, json={"token": cp_jwt, "organization": {"resource_id": org_resource_id}}
            )

        with _client(handler) as client:
            exchange_id_token_for_cp_token(
                client, CONFIG, id_token=id_token, org_resource_id=org_resource_id
            )

        sent_body = json.loads(captured["request"].content.decode())
        assert sent_body == {"id_token": id_token, "org_resource_id": org_resource_id}

    def test_confluent_api_error_raises_operational_error(self):
        error_message = "invalid id_token"

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(400, json={"message": error_message})

        with _client(handler) as client, pytest.raises(OperationalError, match=error_message):
            exchange_id_token_for_cp_token(client, CONFIG, id_token="bad", org_resource_id=None)

    def test_token_with_unparseable_exp_falls_back_to_fixed_lifetime(self):
        now = _now()

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"token": "not-a-jwt", "organization": {}})

        with _client(handler) as client:
            result = exchange_id_token_for_cp_token(
                client, CONFIG, id_token="id-tok", org_resource_id=None, now=now
            )

        assert result.expires_at == now + FALLBACK_CP_LIFETIME

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

    def test_absent_organization_yields_none_resource_id(self):
        cp_jwt = _make_jwt(int(datetime(2026, 8, 10, 13, 0, tzinfo=timezone.utc).timestamp()))

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"token": cp_jwt})

        with _client(handler) as client:
            result = exchange_id_token_for_cp_token(
                client, CONFIG, id_token="id-tok", org_resource_id=None
            )
        assert result.organization_resource_id is None

    def test_organization_present_without_resource_id_raises_operational_error(self):
        """organization_resource_id is documented as None only when the organization block is
        entirely absent -- a present-but-incomplete block is a server-contract violation, not
        another way to spell "no org"."""
        cp_jwt = _make_jwt(int(datetime(2026, 8, 10, 13, 0, tzinfo=timezone.utc).timestamp()))

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"token": cp_jwt, "organization": {"id": 542039}})

        with _client(handler) as client, pytest.raises(OperationalError, match="resource_id"):
            exchange_id_token_for_cp_token(client, CONFIG, id_token="id-tok", org_resource_id=None)

    def test_organization_resource_id_wrong_type_raises_operational_error(self):
        cp_jwt = _make_jwt(int(datetime(2026, 8, 10, 13, 0, tzinfo=timezone.utc).timestamp()))

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200, json={"token": cp_jwt, "organization": {"resource_id": 12345}}
            )

        with _client(handler) as client, pytest.raises(OperationalError, match="expected str"):
            exchange_id_token_for_cp_token(client, CONFIG, id_token="id-tok", org_resource_id=None)


class TestJwtExpHardening:
    """_jwt_exp must turn every malformed-token shape into the fixed-lifetime fallback, never a
    bare TypeError/OverflowError/OSError -- exercised through exchange_cp_for_dp_token since both
    hops 2 and 3 route their token through the same _jwt_exp helper."""

    def _dp_token_response(self, token: str) -> httpx.Response:
        return httpx.Response(200, json={"token": token})

    def _assert_falls_back(self, token: str) -> None:
        now = _now()

        def handler(request: httpx.Request) -> httpx.Response:
            return self._dp_token_response(token)

        with _client(handler) as client:
            result = exchange_cp_for_dp_token(client, CONFIG, cp_token="cp-tok", now=now)

        assert result.expires_at == now + FALLBACK_DP_LIFETIME

    def test_non_object_payload_falls_back_to_fixed_lifetime(self):
        self._assert_falls_back(_jwt_with_payload(["not", "an", "object"]))

    def test_non_numeric_exp_falls_back_to_fixed_lifetime(self):
        self._assert_falls_back(_jwt_with_payload({"exp": "not-a-number"}))

    def test_out_of_range_exp_falls_back_to_fixed_lifetime(self):
        self._assert_falls_back(_jwt_with_payload({"exp": 99999999999999999}))

    def test_opaque_non_jwt_token_falls_back_to_fixed_lifetime(self):
        """A plainly opaque bearer token -- no dots at all -- is a different failure shape than
        the malformed-payload cases above, which are still well-formed 3-segment JWTs."""
        self._assert_falls_back("opaque-bearer-token-abc123")

    def test_naive_now_still_yields_tz_aware_utc_expiry(self):
        """A caller passing a naive `now` must not get back a naive expiry -- the JWT-decode
        path always returns tz-aware UTC, and a fallback expiry that silently went naive would
        later raise TypeError when compared against it (e.g. TokenSet's *_valid helpers)."""
        naive_now = datetime(2026, 8, 11, 9, 0)

        def handler(request: httpx.Request) -> httpx.Response:
            return self._dp_token_response("opaque-bearer-token-abc123")

        with _client(handler) as client:
            result = exchange_cp_for_dp_token(client, CONFIG, cp_token="cp-tok", now=naive_now)

        assert result.expires_at == naive_now.replace(tzinfo=timezone.utc) + FALLBACK_DP_LIFETIME
        assert result.expires_at.tzinfo is not None


class TestExchangeCpForDpToken:
    def test_sends_bearer_cp_token_and_parses_response(self):
        cp_token = "cp-tok-xyz"
        exp = int(datetime(2026, 8, 10, 12, 10, tzinfo=timezone.utc).timestamp())
        dp_jwt = _make_jwt(exp)
        captured = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["request"] = request
            return httpx.Response(200, json={"token": dp_jwt, "regional_token": "unused"})

        with _client(handler) as client:
            result = exchange_cp_for_dp_token(client, CONFIG, cp_token=cp_token)

        request = captured["request"]
        assert str(request.url) == f"{CONFIG.api_host}/api/access_tokens"
        assert request.headers["Authorization"] == f"Bearer {cp_token}"
        assert result.token == dp_jwt
        assert result.expires_at == datetime.fromtimestamp(exp, tz=timezone.utc)

    def test_confluent_api_error_raises_operational_error(self):
        error_message = "cp token expired"

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(401, json={"message": error_message})

        with _client(handler) as client, pytest.raises(OperationalError, match=error_message):
            exchange_cp_for_dp_token(client, CONFIG, cp_token="expired-cp-tok")
