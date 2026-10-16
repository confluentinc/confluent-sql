"""Unit tests for the per-service control-plane credential pick."""

from __future__ import annotations

import pytest

from confluent_sql import InterfaceError
from confluent_sql.connection import _resolve_service_credentials

pytestmark = pytest.mark.unit

GLOBAL_PAIR = ("gk", "gs")


@pytest.fixture(params=["tableflow", "connect", "cloud"])
def service(request: pytest.FixtureRequest) -> str:
    return request.param


class TestResolveServiceCredentials:
    """Global pair wins, else the service's dedicated pair, else None."""

    def test_global_wins_over_dedicated(self, service: str) -> None:
        assert _resolve_service_credentials(service, "gk", "gs", "k", "s") == GLOBAL_PAIR

    def test_dedicated_pair_when_no_global(self, service: str) -> None:
        assert _resolve_service_credentials(service, "", "", "k", "s") == ("k", "s")

    def test_none_when_neither(self, service: str) -> None:
        assert _resolve_service_credentials(service, "", "", "", "") is None

    def test_half_dedicated_pair_raises_naming_the_service(self, service: str) -> None:
        with pytest.raises(InterfaceError, match=f"{service}_api_key and {service}_api_secret"):
            _resolve_service_credentials(service, "", "", "k", "")

    def test_complete_global_short_circuits_half_dedicated_pair(self, service: str) -> None:
        # A usable global pair must not be tripped up by an incidental half-supplied dedicated
        # pair (e.g. one leaked in from the environment).
        assert _resolve_service_credentials(service, "gk", "gs", "k", "") == GLOBAL_PAIR

    @pytest.mark.parametrize(
        ("global_key", "global_secret"), [("gk", ""), ("", "gs"), ("gk", None), (None, "gs")]
    )
    def test_half_global_pair_does_not_short_circuit(
        self, service: str, global_key: str | None, global_secret: str | None
    ) -> None:
        # Falls through to the dedicated pair...
        assert _resolve_service_credentials(service, global_key, global_secret, "k", "s") == (
            "k",
            "s",
        )
        # ...and so still validates it, and yields None when there is none.
        with pytest.raises(InterfaceError, match=f"{service}_api_key"):
            _resolve_service_credentials(service, global_key, global_secret, "k", "")
        assert _resolve_service_credentials(service, global_key, global_secret, "", "") is None
