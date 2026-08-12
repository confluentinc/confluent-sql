import dataclasses
from datetime import datetime, timedelta, timezone

import pytest

from confluent_sql.oauth.token_set import EXPIRY_SKEW, TokenSet

pytestmark = pytest.mark.unit


def _now() -> datetime:
    return datetime(2026, 8, 10, 12, 0, 0, tzinfo=timezone.utc)


def _token_set(
    now: datetime, *, cp_ttl: timedelta, dp_ttl: timedelta, refresh_ttl: timedelta
) -> TokenSet:
    return TokenSet(
        refresh_token="refresh-tok",
        refresh_token_expires_at=now + refresh_ttl,
        cp_token="cp-tok",
        cp_token_expires_at=now + cp_ttl,
        dp_token="dp-tok",
        dp_token_expires_at=now + dp_ttl,
    )


def test_cp_token_valid_before_expiry():
    now = _now()
    tokens = _token_set(
        now,
        cp_ttl=timedelta(minutes=5),
        dp_ttl=timedelta(minutes=5),
        refresh_ttl=timedelta(hours=8),
    )
    assert tokens.cp_token_valid(now) is True


def test_cp_token_invalid_within_skew_window_of_expiry():
    now = _now()
    tokens = _token_set(
        now, cp_ttl=EXPIRY_SKEW / 2, dp_ttl=timedelta(minutes=5), refresh_ttl=timedelta(hours=8)
    )
    assert tokens.cp_token_valid(now) is False


def test_cp_token_invalid_after_expiry():
    now = _now()
    tokens = _token_set(
        now,
        cp_ttl=timedelta(minutes=5),
        dp_ttl=timedelta(minutes=5),
        refresh_ttl=timedelta(hours=8),
    )
    assert tokens.cp_token_valid(now + timedelta(minutes=6)) is False


def test_dp_token_valid_before_expiry():
    now = _now()
    tokens = _token_set(
        now,
        cp_ttl=timedelta(minutes=5),
        dp_ttl=timedelta(minutes=10),
        refresh_ttl=timedelta(hours=8),
    )
    assert tokens.dp_token_valid(now) is True


def test_dp_token_invalid_within_skew_window_of_expiry():
    now = _now()
    tokens = _token_set(
        now, cp_ttl=timedelta(minutes=5), dp_ttl=EXPIRY_SKEW / 2, refresh_ttl=timedelta(hours=8)
    )
    assert tokens.dp_token_valid(now) is False


def test_refresh_token_valid_before_absolute_wall():
    now = _now()
    tokens = _token_set(
        now,
        cp_ttl=timedelta(minutes=5),
        dp_ttl=timedelta(minutes=5),
        refresh_ttl=timedelta(hours=8),
    )
    assert tokens.refresh_token_valid(now + timedelta(hours=7)) is True


def test_refresh_token_invalid_past_absolute_wall():
    now = _now()
    tokens = _token_set(
        now,
        cp_ttl=timedelta(minutes=5),
        dp_ttl=timedelta(minutes=5),
        refresh_ttl=timedelta(hours=8),
    )
    assert tokens.refresh_token_valid(now + timedelta(hours=8, minutes=1)) is False


def test_token_set_is_frozen():
    now = _now()
    tokens = _token_set(
        now,
        cp_ttl=timedelta(minutes=5),
        dp_ttl=timedelta(minutes=5),
        refresh_ttl=timedelta(hours=8),
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        tokens.cp_token = "hacked"  # type: ignore[misc]
