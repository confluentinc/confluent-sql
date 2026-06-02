import pytest

from confluent_sql import polling
from confluent_sql.polling import sleep_with_backoff


class _FakeClock:
    """A monotonic clock that only advances when sleep() is called, so backoff pacing can be
    exercised deterministically without real time passing."""

    def __init__(self) -> None:
        self.now = 0.0
        self.slept: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, secs: float) -> None:
        self.slept.append(secs)
        self.now += secs


@pytest.fixture()
def fake_clock(mocker) -> _FakeClock:
    """Install a fake clock and disable jitter (uniform -> 1.0) inside the polling module."""
    clock = _FakeClock()
    mocker.patch.object(polling.time, "monotonic", clock.monotonic)
    mocker.patch.object(polling.time, "sleep", clock.sleep)
    mocker.patch.object(polling.random, "uniform", return_value=1.0)
    return clock


@pytest.mark.unit
class TestSleepWithBackoff:
    """Tests for the shared backoff pacing generator."""

    def test_sleeps_grow_geometrically_until_timeout(self, fake_clock: _FakeClock):
        """Each retry sleeps base * growth**n (jitter pinned to 1.0), and yielding stops once the
        accumulated wait reaches the timeout."""
        yields = list(sleep_with_backoff(10.0))

        # base=1.0, growth=1.5: 1.0, 1.5, 2.25, 3.375, 5.0625 -> cumulative 13.1875 > 10 after 5.
        assert fake_clock.slept == [1.0, 1.5, 2.25, 3.375, 5.0625]
        assert len(yields) == len(fake_clock.slept) == 5

    def test_delay_is_capped(self, fake_clock: _FakeClock):
        """The per-retry sleep never exceeds the 30s ceiling, even after many retries."""
        list(sleep_with_backoff(500.0))

        assert max(fake_clock.slept) == 30.0
        # Once the ceiling is hit it stays there.
        assert fake_clock.slept[-1] == 30.0

    def test_zero_timeout_yields_nothing(self, fake_clock: _FakeClock):
        """A non-positive timeout produces no retries and never sleeps."""
        assert list(sleep_with_backoff(0.0)) == []
        assert fake_clock.slept == []

    def test_jitter_band_applied(self, mocker):
        """Each sleep is scaled by a ±25% jitter draw."""
        clock = _FakeClock()
        mocker.patch.object(polling.time, "monotonic", clock.monotonic)
        mocker.patch.object(polling.time, "sleep", clock.sleep)
        uniform = mocker.patch.object(polling.random, "uniform", return_value=1.0)

        list(sleep_with_backoff(2.0))

        uniform.assert_called_with(0.75, 1.25)
