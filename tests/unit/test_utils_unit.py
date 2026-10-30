"""Unit tests for the shared leaf helpers in `confluent_sql.utils`."""

from __future__ import annotations

import httpx
import pytest

from confluent_sql.utils import ExtensibleEnum, extract_error_detail

pytestmark = pytest.mark.unit


class _Color(ExtensibleEnum):
    RED = "RED"
    UNKNOWN = "UNKNOWN"


class _NoFallback(ExtensibleEnum):
    ONLY = "ONLY"


class TestExtensibleEnum:
    def test_known_value_round_trips(self) -> None:
        assert _Color("RED") is _Color.RED

    @pytest.mark.parametrize("value", ["SOMETHING_NEW", "red", "", None, 42])
    def test_unrecognized_value_maps_to_unknown(self, value: object) -> None:
        assert _Color(value) is _Color.UNKNOWN

    def test_members_are_strs_equal_to_their_wire_value(self) -> None:
        assert isinstance(_Color.RED, str)
        assert _Color.RED == "RED"

    def test_subclass_without_unknown_member_raises_value_error(self) -> None:
        with pytest.raises(ValueError):
            _NoFallback("SOMETHING_NEW")


def _response(*, json_body: object = None, text: str | None = None) -> httpx.Response:
    if text is not None:
        return httpx.Response(500, text=text)
    return httpx.Response(500, json=json_body)


class TestExtractErrorDetail:
    @pytest.mark.parametrize(
        ("response", "expected"),
        [
            pytest.param(
                _response(json_body={"errors": [{"detail": "a"}, {"detail": "b"}]}),
                "a; b",
                id="errors-details-joined",
            ),
            pytest.param(
                _response(json_body={"errors": [{"title": "t"}, {"detail": "b"}]}),
                "b",
                id="errors-entries-without-detail-skipped",
            ),
            pytest.param(
                _response(json_body={"error_code": 401, "message": "kafka.api.key invalid"}),
                "kafka.api.key invalid",
                id="connect-top-level-message",
            ),
            pytest.param(
                _response(json_body={"errors": [{"detail": "a"}], "message": "ignored"}),
                "a",
                id="errors-win-over-message",
            ),
            pytest.param(
                _response(text="  upstream went away\n"), "upstream went away", id="plain-text"
            ),
            pytest.param(_response(json_body={"errors": []}), "no more details", id="empty-errors"),
            pytest.param(
                _response(json_body={"message": ""}), "no more details", id="empty-message"
            ),
            pytest.param(
                _response(json_body=["not", "a", "dict"]), "no more details", id="json-list"
            ),
            pytest.param(_response(text=""), "no more details", id="empty-body"),
        ],
    )
    def test_extract_error_detail(self, response: httpx.Response, expected: str) -> None:
        assert extract_error_detail(response) == expected
