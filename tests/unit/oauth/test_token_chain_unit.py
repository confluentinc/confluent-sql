import base64
import json
import urllib.parse
from datetime import datetime, timezone

import httpx
import pytest

from confluent_sql.exceptions import OAuthTokenEndpointError, OperationalError
from confluent_sql.oauth.config import CCloudOAuthConfig
from confluent_sql.oauth.token_chain import (
    FALLBACK_CP_LIFETIME,
    FALLBACK_DP_LIFETIME,
    exchange_code_for_tokens,
    exchange_cp_for_dp_token,
    exchange_id_token_for_cp_token,
    exchange_refresh_token,
)

pytestmark = [pytest.mark.unit, pytest.mark.oauth]

CONFIG = CCloudOAuthConfig(
    auth_service_domain="login.confluent.io",
    api_host="https://confluent.cloud",
    client_id="test-client-id",
)


CCLOUD_UNKNOWN_ORG_ERROR = {"code": 403, "message": "Cannot find the organization for this user"}
CCLOUD_UNKNOWN_ORG_BODY = {"error": CCLOUD_UNKNOWN_ORG_ERROR}
"""What CCloud's `/api/sessions` answers (403) when the supplied org_resource_id is unusable --
captured from real CCloud (#271): the error object is nested under `error`, so the generic
message embeds its dict repr."""


def _now() -> datetime:
    return datetime(2026, 8, 11, 9, 0, tzinfo=timezone.utc)


def _jwt_segment(value: object) -> str:
    return base64.urlsafe_b64encode(json.dumps(value).encode()).rstrip(b"=").decode("ascii")


def _jwt_with_payload(payload: object) -> str:
    """Build a token (header.payload.sig) with an arbitrary -- possibly malformed -- JSON
    payload segment, for testing _jwt_exp's handling of shapes a real JWT would never have."""
    return f"{_jwt_segment({'alg': 'none'})}.{_jwt_segment(payload)}.sig"


def _jwt_with_raw_payload_segment(segment: str) -> str:
    """Like _jwt_with_payload, but with the payload segment verbatim, so it can be bad base64."""
    return f"{_jwt_segment({'alg': 'none'})}.{segment}.sig"


def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


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

    def test_auth_service_error_body_raises_operational_error(self):
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

    def test_non_object_auth_service_error_body_still_raises_operational_error(self):
        """A non-2xx response whose valid JSON body is a list, not an object, must still surface
        as OperationalError -- not AttributeError out of _raise_for_auth_service_error's own
        body.get()."""

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(403, json=["unexpected", "error", "shape"])

        with _client(handler) as client, pytest.raises(OperationalError, match="auth service"):
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

    def test_auth_service_error_body_raises_operational_error(self):
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

    def test_rejection_carries_the_machine_readable_error_code(self):
        """A refused refresh must be distinguishable from a transient blip *structurally*.

        `invalid_grant` is how the token endpoint reports a refresh token that is expired,
        revoked, or already spent -- the one condition no retry can fix. The provider decides
        between "give up and demand a fresh login" and "retry on the next request" off this
        attribute, so it has to survive as data rather than dissolve into the message text.
        """

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                403, json={"error": "invalid_grant", "error_description": "Unknown or expired"}
            )

        with _client(handler) as client, pytest.raises(OAuthTokenEndpointError) as caught:
            exchange_refresh_token(client, CONFIG, refresh_token="dead-refresh-tok")

        assert caught.value.error_code == "invalid_grant"
        assert caught.value.http_status_code == 403

    def test_rejection_without_an_error_field_carries_no_error_code(self):
        """An HTML error page from an intermediary, or any body without an `error` field, still
        raises -- with error_code None rather than a fabricated one. The provider treats an
        unknown code as retryable, so inventing a value here would be the harmful direction."""

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(502, text="<html>Bad Gateway</html>")

        with _client(handler) as client, pytest.raises(OAuthTokenEndpointError) as caught:
            exchange_refresh_token(client, CONFIG, refresh_token="some-refresh-tok")

        assert caught.value.error_code is None
        assert caught.value.http_status_code == 502


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

    def test_rejected_supplied_org_raises_org_aware_error(self):
        """A 403 on a session that named an org says so, naming the org and the way out, while
        keeping the status, CCloud's own message, and the original exception chained."""
        org_resource_id = "org-bogus-123"

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(403, json=CCLOUD_UNKNOWN_ORG_BODY)

        username = "ada@example.com"
        id_token = _jwt_with_payload({"email": username})
        with _client(handler) as client, pytest.raises(OperationalError) as caught:
            exchange_id_token_for_cp_token(
                client, CONFIG, id_token=id_token, org_resource_id=org_resource_id
            )

        message = str(caught.value)
        assert org_resource_id in message
        assert username in message
        assert "Omit organization_id" in message
        assert CCLOUD_UNKNOWN_ORG_ERROR["message"] in message
        assert caught.value.http_status_code == 403
        assert isinstance(caught.value.__cause__, OperationalError)

    @pytest.mark.parametrize(
        "id_token",
        [
            pytest.param("opaque-id-token", id="not-three-segments-ValueError"),
            pytest.param(
                _jwt_with_raw_payload_segment("a"), id="bad-base64-padding-binascii-Error"
            ),
            pytest.param(
                _jwt_with_raw_payload_segment(_b64url(b"not json")), id="not-json-JSONDecodeError"
            ),
            pytest.param(
                _jwt_with_raw_payload_segment(_b64url(b"\xff\xfe")),
                id="not-utf8-UnicodeDecodeError",
            ),
            pytest.param(_jwt_with_payload({"sub": "u-1"}), id="no-email-claim-KeyError"),
            pytest.param(_jwt_with_payload(["email"]), id="payload-is-list-TypeError"),
            pytest.param(_jwt_with_payload("email"), id="payload-is-string-TypeError"),
            pytest.param(_jwt_with_payload(5), id="payload-is-int-TypeError"),
            pytest.param(_jwt_with_payload(None), id="payload-is-null-TypeError"),
            pytest.param(_jwt_with_payload({"email": 42}), id="non-string-email"),
            pytest.param(_jwt_with_payload({"email": ""}), id="empty-email"),
        ],
    )
    def test_rejected_org_without_a_readable_username_omits_it(self, id_token):
        """Test that when the organization is rejected and the username is not readable, it is
        omitted from the error message. Exercises _id_token_email() call from
        exchange_id_token_for_cp_token()."""

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(403, json=CCLOUD_UNKNOWN_ORG_BODY)

        with _client(handler) as client, pytest.raises(OperationalError) as caught:
            exchange_id_token_for_cp_token(
                client, CONFIG, id_token=id_token, org_resource_id="org-1"
            )

        assert "org-1" in str(caught.value)
        assert "logged in as" not in str(caught.value)

    @pytest.mark.parametrize(
        ("status", "org_resource_id"),
        [
            pytest.param(403, None, id="403-but-no-org-was-sent"),
            pytest.param(401, "org-1", id="401-with-org"),
            pytest.param(403, "org-1", id="403-with-org-other-reason"),
            pytest.param(500, "org-1", id="500-with-org"),
        ],
    )
    def test_other_failures_keep_the_generic_message(self, status, org_resource_id):
        body = (
            {"error": {"code": 403, "message": "Some other forbidden reason"}}
            if (status, org_resource_id) == (403, "org-1")
            else CCLOUD_UNKNOWN_ORG_BODY
        )

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(status, json=body)

        with _client(handler) as client, pytest.raises(OperationalError) as caught:
            exchange_id_token_for_cp_token(
                client, CONFIG, id_token="id-tok", org_resource_id=org_resource_id
            )

        assert str(caught.value) == f"Confluent Cloud API request failed: {body['error']}"
        assert caught.value.http_status_code == status

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


class TestCpTokenUserIdentity:
    """The CP token's unverified `userResourceId` (else `sub`) names who logged in (#277)."""

    @staticmethod
    def _user_resource_id_for(token: str) -> str | None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200, json={"token": token, "organization": {"resource_id": "org-abc"}}
            )

        with _client(handler) as client:
            result = exchange_id_token_for_cp_token(
                client, CONFIG, id_token="id-tok", org_resource_id=None
            )
        return result.user_resource_id

    @pytest.mark.parametrize(
        ("claims", "expected"),
        [
            pytest.param(
                {"userResourceId": "u-abc123", "sub": "auth0|1"},
                "u-abc123",
                id="prefers-user-resource-id",
            ),
            pytest.param({"sub": "auth0|1"}, "auth0|1", id="falls-back-to-sub"),
            pytest.param(
                {"userResourceId": "", "sub": "auth0|1"},
                "auth0|1",
                id="empty-user-resource-id-falls-back",
            ),
            pytest.param({"userResourceId": 42, "sub": 7}, None, id="non-string-claims-ignored"),
            pytest.param({"exp": 1}, None, id="no-identity-claims"),
        ],
    )
    def test_extracts_identity_from_payload(self, claims, expected):
        assert self._user_resource_id_for(_jwt_with_payload(claims)) == expected

    @pytest.mark.parametrize(
        "token",
        [
            pytest.param("not-a-jwt", id="opaque"),
            pytest.param(_jwt_with_payload(["not", "an", "object"]), id="non-object-payload"),
            pytest.param(_jwt_with_raw_payload_segment("!!!"), id="bad-base64"),
        ],
    )
    def test_undecodable_token_yields_no_identity(self, token):
        assert self._user_resource_id_for(token) is None


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

    def test_403_keeps_the_generic_message_with_no_org_text(self):
        """This hop never sends an org, so the shared error helper must not grow org hints."""

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(403, json=CCLOUD_UNKNOWN_ORG_BODY)

        with _client(handler) as client, pytest.raises(OperationalError) as caught:
            exchange_cp_for_dp_token(client, CONFIG, cp_token="cp-tok")

        assert str(caught.value) == (
            f"Confluent Cloud API request failed: {CCLOUD_UNKNOWN_ORG_ERROR}"
        )
        assert caught.value.http_status_code == 403
