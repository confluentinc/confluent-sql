import httpx
import pytest

from confluent_sql.exceptions import OperationalError
from confluent_sql.oauth.http_json import (
    best_effort_json_object,
    optional_object_field,
    post_json,
    require_field,
)

pytestmark = pytest.mark.unit


def _client(handler) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler))


def _noop_check_response(response: httpx.Response) -> None:
    pass


class TestPostJson:
    def test_returns_parsed_dict_body(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"a": 1})

        with _client(handler) as client:
            body = post_json(client, "https://example.test/x", check_response=_noop_check_response)
        assert body == {"a": 1}

    def test_transport_failure_raises_operational_error(self):
        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("boom", request=request)

        with (
            _client(handler) as client,
            pytest.raises(OperationalError, match="ConnectError"),
        ):
            post_json(client, "https://example.test/x", check_response=_noop_check_response)

    def test_check_response_is_invoked_and_can_raise(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(500, json={})

        def check_response(response: httpx.Response) -> None:
            if response.status_code >= 500:
                raise OperationalError("server exploded")

        with _client(handler) as client, pytest.raises(OperationalError, match="server exploded"):
            post_json(client, "https://example.test/x", check_response=check_response)

    def test_non_json_body_raises_operational_error(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, content=b"not json")

        with _client(handler) as client, pytest.raises(OperationalError, match="JSON"):
            post_json(client, "https://example.test/x", check_response=_noop_check_response)

    def test_non_object_json_body_raises_operational_error(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=["a", "list"])

        with _client(handler) as client, pytest.raises(OperationalError, match="object"):
            post_json(client, "https://example.test/x", check_response=_noop_check_response)


class TestRequireField:
    def test_returns_value_when_present_and_correct_type(self):
        assert require_field({"k": "v"}, "k", context="ctx") == "v"

    def test_missing_field_raises_operational_error(self):
        with pytest.raises(OperationalError, match="missing 'k'"):
            require_field({}, "k", context="ctx")

    def test_wrong_type_raises_operational_error(self):
        with pytest.raises(OperationalError, match="expected str"):
            require_field({"k": 123}, "k", context="ctx")

    def test_none_value_raises_operational_error(self):
        with pytest.raises(OperationalError, match="expected str"):
            require_field({"k": None}, "k", context="ctx")

    def test_custom_type_accepted(self):
        assert require_field({"k": 123}, "k", context="ctx", type_=int) == 123

    def test_bool_rejected_when_int_requested(self):
        """isinstance(True, int) is True in Python, but a caller asking for type_=int almost
        certainly doesn't mean to silently accept a JSON true/false."""
        with pytest.raises(OperationalError, match="expected int"):
            require_field({"k": True}, "k", context="ctx", type_=int)

    def test_bool_accepted_when_explicitly_requested(self):
        assert require_field({"k": True}, "k", context="ctx", type_=bool) is True

    def test_bool_accepted_when_included_in_type_tuple(self):
        assert require_field({"k": True}, "k", context="ctx", type_=(int, bool)) is True


class TestOptionalObjectField:
    def test_absent_field_returns_empty_dict(self):
        assert optional_object_field({}, "organization") == {}

    def test_present_object_field_returned(self):
        assert optional_object_field(
            {"organization": {"resource_id": "org-1"}}, "organization"
        ) == {"resource_id": "org-1"}

    def test_present_non_object_field_raises_operational_error(self):
        with pytest.raises(OperationalError, match="not an object"):
            optional_object_field({"organization": "not-an-object"}, "organization")

    def test_present_null_field_raises_operational_error_not_treated_as_absent(self):
        """A present-but-null field is a different shape than an absent one: an absent
        `organization` key means "no organization" (a valid, expected shape), but Confluent
        Cloud explicitly sending `"organization": null` would be a shape we've never actually
        observed and shouldn't silently swallow into the same {} default."""
        with pytest.raises(OperationalError, match="not an object"):
            optional_object_field({"organization": None}, "organization")


class TestBestEffortJsonObject:
    def test_returns_dict_body(self):
        response = httpx.Response(400, json={"error": "bad"})
        assert best_effort_json_object(response) == {"error": "bad"}

    def test_non_json_body_returns_empty_dict(self):
        response = httpx.Response(400, content=b"not json")
        assert best_effort_json_object(response) == {}

    def test_non_object_json_body_returns_empty_dict(self):
        response = httpx.Response(400, json=["a", "list"])
        assert best_effort_json_object(response) == {}
