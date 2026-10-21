"""Unit tests for the shared leaf helpers in `confluent_sql.utils`."""

from __future__ import annotations

import pytest

from confluent_sql.utils import ExtensibleEnum

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
